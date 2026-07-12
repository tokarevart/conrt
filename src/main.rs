mod cstring;
mod daemon;
mod interprocess;
mod pty;
mod sys;
mod uring;

use std::ffi::c_int;
use std::fs::File;
use std::io;
use std::io::Read;
use std::io::Write;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::io::AsRawFd;
use std::os::unix::io::FromRawFd;
use std::os::unix::io::RawFd;
use std::path::Path;
use std::path::PathBuf;
use std::process::ExitCode;
use std::str::FromStr;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use clap::Parser;
use cstring::CString;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::EnvFilter;

fn default_socket_path() -> PathBuf {
    let home = std::env::var("HOME").unwrap_or_else(|_| "/tmp".into());
    PathBuf::from(home).join(".conrt").join("conrt.sock")
}

#[derive(Parser)]
#[command(name = "conrt")]
enum Cli {
    /// Run a container
    Run {
        /// Path to the root filesystem (optional; uses host rootfs if omitted)
        #[arg(long)]
        rootfs: Option<PathBuf>,

        /// PID of a running container whose user+net namespace to join
        #[arg(long)]
        net_pid: Option<i32>,

        /// Preserve overlay upperdir after container exits (default: clean up)
        #[arg(long)]
        save: bool,

        /// Allocate a PTY for interactive use
        #[arg(short, long)]
        tty: bool,

        /// Keep stdin open
        #[arg(short, long)]
        interactive: bool,

        /// Detach and hand off to the daemon
        #[arg(short, long)]
        detach: bool,

        /// Daemon socket path
        #[arg(long)]
        socket_path: Option<PathBuf>,

        /// Command to run inside the container
        command: Vec<CString>,
    },
    /// Start the daemon
    Daemon {
        /// Daemon socket path
        #[arg(long)]
        socket_path: Option<PathBuf>,
    },
    /// Show container logs
    Logs {
        /// Container ID
        id: String,
        /// Follow (stream) log output
        #[arg(long, short = 'f')]
        follow: bool,
        /// Daemon socket path
        #[arg(long)]
        socket_path: Option<PathBuf>,
    },
    /// List running containers
    List {
        /// Daemon socket path
        #[arg(long)]
        socket_path: Option<PathBuf>,
    },
    /// Kill a container
    Kill {
        /// Container PID
        pid: i32,

        /// Daemon socket path
        #[arg(long)]
        socket_path: Option<PathBuf>,
    },
}

fn main() -> ExitCode {
    let ansi = unsafe { libc::isatty(libc::STDERR_FILENO) != 0 };
    tracing_subscriber::fmt()
        .with_ansi(ansi)
        .with_env_filter(
            EnvFilter::builder()
                .with_default_directive(LevelFilter::INFO.into())
                .from_env()
                .unwrap(),
        )
        .with_writer(|| std::io::LineWriter::new(std::io::stderr()))
        .init();

    let cli = Cli::parse();

    match cli {
        Cli::Daemon { socket_path } => run_daemon(socket_path.unwrap_or_else(default_socket_path)),
        Cli::Run {
            rootfs,
            net_pid,
            save,
            tty,
            interactive,
            detach,
            socket_path,
            command,
        } => {
            if detach {
                run_detach(
                    socket_path.unwrap_or_else(default_socket_path),
                    rootfs,
                    net_pid.map(|p| p as libc::pid_t),
                    save,
                    tty,
                    interactive,
                    command,
                )
            } else {
                let socket = socket_path.unwrap_or_else(default_socket_path);
                let mut sock = socket.into_os_string();
                sock.push(".stream");
                run_attach(PathBuf::from(sock), RunArgs {
                    rootfs,
                    net_pid: net_pid.map(|p| p as libc::pid_t),
                    save,
                    tty,
                    interactive,
                    command,
                })
            }
        }
        Cli::Logs {
            id,
            follow,
            socket_path,
        } => show_logs(id, follow, socket_path.unwrap_or_else(default_socket_path)),
        Cli::List { socket_path } => {
            list_containers(socket_path.unwrap_or_else(default_socket_path))
        }
        Cli::Kill { pid, socket_path } => {
            kill_container(pid, socket_path.unwrap_or_else(default_socket_path))
        }
    }
}

fn run_daemon(socket_path: PathBuf) -> ExitCode {
    tracing::info!(path = %socket_path.display(), "starting daemon");
    let mut daemon = daemon::Daemon::new(socket_path);
    match daemon.run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            tracing::error!(%e, "daemon exited with error");
            ExitCode::FAILURE
        }
    }
}

fn run_detach(
    socket_path: PathBuf,
    rootfs: Option<PathBuf>,
    net_pid: Option<libc::pid_t>,
    save: bool,
    tty: bool,
    interactive: bool,
    command: Vec<CString>,
) -> ExitCode {
    let rootfs_str = rootfs.as_ref().map(|p| p.to_string_lossy().into_owned());

    let request = daemon::Request::Run {
        rootfs: rootfs_str,
        net_pid,
        save,
        command: daemon::CStringSerde::from_inner_vec(command),
        interactive: Some(interactive),
        tty: Some(tty),
    };

    let resp = match daemon::send_request(&socket_path, &request) {
        Ok(r) => r,
        Err(e) => {
            tracing::error!(%e, "cannot connect to daemon");
            return ExitCode::FAILURE;
        }
    };

    #[derive(serde::Deserialize)]
    struct RunResp {
        ok: bool,
        pid: Option<i32>,
        error: Option<String>,
    }

    let resp: RunResp = match serde_json::from_slice(&resp) {
        Ok(r) => r,
        Err(e) => {
            tracing::error!(%e, "invalid daemon response");
            return ExitCode::FAILURE;
        }
    };

    if resp.ok {
        println!("{}", resp.pid.unwrap());
        ExitCode::SUCCESS
    } else {
        tracing::error!(error = %resp.error.as_deref().unwrap_or("unknown"), "daemon rejected run");
        ExitCode::FAILURE
    }
}

struct RunArgs {
    rootfs: Option<PathBuf>,
    net_pid: Option<libc::pid_t>,
    save: bool,
    tty: bool,
    interactive: bool,
    command: Vec<CString>,
}

// ── Attach protocol helpers (framed Unix stream) ─────────────────────────

fn build_frame(ty: u8, payload: &[u8]) -> Vec<u8> {
    let mut frame = Vec::with_capacity(3 + payload.len());
    frame.push(ty);
    frame.extend_from_slice(&(payload.len() as u16).to_le_bytes());
    frame.extend_from_slice(payload);
    frame
}

fn send_frame(fd: std::os::raw::c_int, ty: u8, payload: &[u8]) -> io::Result<()> {
    let frame = build_frame(ty, payload);
    let written = crate::sys::write(fd, &frame)? as usize;
    if written != frame.len() {
        return Err(io::Error::new(
            io::ErrorKind::WriteZero,
            "partial frame write",
        ));
    }
    Ok(())
}

fn read_frame(fd: std::os::raw::c_int) -> io::Result<(u8, Vec<u8>)> {
    let mut header = [0u8; 3];
    let mut off = 0usize;
    while off < header.len() {
        let n = crate::sys::read(fd, &mut header[off..])?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "stream closed",
            ));
        }
        off += n as usize;
    }
    let ty = header[0];
    let len = u16::from_le_bytes([header[1], header[2]]) as usize;
    if len == 0 {
        return Ok((ty, Vec::new()));
    }
    let mut buf = vec![0u8; len];
    let mut off = 0usize;
    while off < len {
        let n = crate::sys::read(fd, &mut buf[off..])?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "stream closed",
            ));
        }
        off += n as usize;
    }
    Ok((ty, buf))
}

fn get_window_size() -> (u16, u16) {
    let ws: libc::winsize = unsafe { std::mem::zeroed() };
    if unsafe { libc::ioctl(libc::STDIN_FILENO, libc::TIOCGWINSZ, &ws) } == 0 {
        (ws.ws_row, ws.ws_col)
    } else {
        (24, 80)
    }
}

static WINCH_PENDING: AtomicBool = AtomicBool::new(false);

extern "C" fn sigwinch_handler(_sig: c_int) {
    WINCH_PENDING.store(true, std::sync::atomic::Ordering::Release);
}

/// Run a container attached to the daemon via Unix stream.
fn run_attach(stream_path: PathBuf, args: RunArgs) -> ExitCode {
    let stream = match std::os::unix::net::UnixStream::connect(&stream_path) {
        Ok(s) => s,
        Err(e) => {
            tracing::error!(%e, path = %stream_path.display(), "cannot connect to daemon stream");
            return ExitCode::FAILURE;
        }
    };
    let raw_fd = stream.as_raw_fd();

    let request = daemon::Request::Run {
        rootfs: args
            .rootfs
            .as_ref()
            .map(|p| p.to_string_lossy().into_owned()),
        net_pid: args.net_pid,
        save: args.save,
        command: daemon::CStringSerde::from_inner_vec(args.command),
        interactive: Some(args.interactive),
        tty: Some(args.tty),
    };
    let payload = serde_json::to_vec(&request).unwrap();
    if let Err(e) = send_frame(raw_fd, 0x00, &payload) {
        tracing::error!(%e, "failed to send run request");
        return ExitCode::FAILURE;
    }

    let (ty, resp_data) = match read_frame(raw_fd) {
        Ok(r) => r,
        Err(e) => {
            tracing::error!(%e, "failed to read run response");
            return ExitCode::FAILURE;
        }
    };
    if ty != 0x01 {
        tracing::error!(%ty, "expected RunResponse frame");
        return ExitCode::FAILURE;
    }

    #[derive(serde::Deserialize)]
    struct RunResp {
        ok: bool,
        #[allow(dead_code)]
        pid: Option<i32>,
        error: Option<String>,
    }
    let resp: RunResp = match serde_json::from_slice(&resp_data) {
        Ok(r) => r,
        Err(e) => {
            tracing::error!(%e, "invalid RunResponse JSON");
            return ExitCode::FAILURE;
        }
    };
    if !resp.ok {
        tracing::error!(error = %resp.error.as_deref().unwrap_or("unknown"), "daemon rejected run");
        return ExitCode::FAILURE;
    }

    // Terminal setup.
    let original_termios = if args.tty && unsafe { libc::isatty(libc::STDIN_FILENO) } != 0 {
        match pty::set_raw_terminal() {
            Ok(termios) => {
                let (rows, cols) = get_window_size();
                let ws_payload = serde_json::json!({"rows": rows, "cols": cols});
                let _ = send_frame(raw_fd, 0x20, &serde_json::to_vec(&ws_payload).unwrap());
                WINCH_PENDING.store(false, std::sync::atomic::Ordering::Relaxed);
                let _ = unsafe {
                    libc::signal(
                        libc::SIGWINCH,
                        sigwinch_handler as *const () as usize as libc::sighandler_t,
                    )
                };
                Some(termios)
            }
            Err(e) => {
                tracing::warn!(%e, "raw terminal setup failed");
                None
            }
        }
    } else if args.interactive && unsafe { libc::isatty(libc::STDIN_FILENO) } != 0 {
        pty::disable_echo_output().ok()
    } else {
        None
    };

    // Reader thread: stream → stdout
    let reader_stream = stream.try_clone().unwrap();
    let reader_fd = reader_stream.as_raw_fd();
    // Prevent stream from being dropped while raw_fd is in use.
    std::mem::forget(reader_stream);

    let reader_handle = std::thread::spawn(move || {
        loop {
            match read_frame(reader_fd) {
                Ok((0x10, data)) => {
                    let mut written = 0usize;
                    while written < data.len() {
                        match crate::sys::write(libc::STDOUT_FILENO, &data[written..]) {
                            Ok(n) => written += n as usize,
                            Err(e) => {
                                tracing::error!(%e, "stdout write failed");
                                return 1i32;
                            }
                        }
                    }
                }
                Ok((0x02, payload)) => {
                    #[derive(serde::Deserialize)]
                    struct ExitPayload {
                        exit_code: i32,
                    }
                    if let Ok(ep) = serde_json::from_slice::<ExitPayload>(&payload) {
                        return ep.exit_code;
                    }
                    return 1;
                }
                Ok((ty, _)) => {
                    tracing::warn!(%ty, "unexpected frame from daemon");
                    return 1;
                }
                Err(e) => {
                    tracing::error!(%e, "reader: stream read error");
                    return 1;
                }
            }
        }
    });

    // Main thread: stdin → stream (0x10 frames)
    let mut stdin_buf = [0u8; 4096];
    let stdin_fd = libc::STDIN_FILENO;
    loop {
        // Check SIGWINCH before each read.
        if WINCH_PENDING.load(std::sync::atomic::Ordering::Acquire) {
            WINCH_PENDING.store(false, std::sync::atomic::Ordering::Relaxed);
            let (rows, cols) = get_window_size();
            let ws_payload = serde_json::json!({"rows": rows, "cols": cols});
            let _ = send_frame(raw_fd, 0x20, &serde_json::to_vec(&ws_payload).unwrap());
        }

        match crate::sys::read(stdin_fd, &mut stdin_buf) {
            Ok(0) => {
                // EOF
                let _ = send_frame(raw_fd, 0x11, &[]);
                break;
            }
            Ok(n) => {
                if let Err(e) = send_frame(raw_fd, 0x10, &stdin_buf[..n as usize]) {
                    tracing::error!(%e, "stdin → stream write failed");
                    break;
                }
            }
            Err(e) if e.raw_os_error() == Some(libc::EINTR) => continue,
            Err(e) => {
                // stdin error (e.g. EOF on a non-TTY)
                if e.kind() == io::ErrorKind::UnexpectedEof || e.raw_os_error() == Some(libc::EIO) {
                    let _ = send_frame(raw_fd, 0x11, &[]);
                }
                break;
            }
        }
    }

    // Restore terminal before joining thread.
    if let Some(termios) = original_termios {
        pty::restore_terminal(&termios).ok();
        unsafe { libc::signal(libc::SIGWINCH, libc::SIG_DFL) };
    }

    let exit_code = match reader_handle.join() {
        Ok(code) => code as u8,
        Err(_) => 1,
    };

    ExitCode::from(exit_code)
}

fn clone3_container(flags: c_int) -> io::Result<Option<libc::pid_t>> {
    let args = libc::clone_args {
        flags: flags as u64,
        pidfd: 0,
        child_tid: 0,
        parent_tid: 0,
        exit_signal: libc::SIGCHLD as u64,
        stack: 0,
        stack_size: 0,
        tls: 0,
        set_tid: 0,
        set_tid_size: 0,
        cgroup: 0,
    };

    Ok(match unsafe { sys::clone3(&args) }? {
        0 => None,
        x => Some(x as libc::pid_t),
    })
}

/// Set up the container root filesystem.
///
/// Uses `chroot` instead of `pivot_root` because an unprivileged user namespace
/// cannot unmount the old root (created by the init namespace), which
/// `pivot_root` + `umount2` requires.
///
/// 1. Remount the mount tree as private (prevent mount leaks to host).
/// 2. Bind-mount the rootfs onto itself.
/// 3. Bind-mount essential device nodes from the host into `/dev`.
/// 4. `chdir` into rootfs.
/// 5. `chroot(".")` — change root to the bound rootfs.
/// 6. `chdir("/")`.
/// 7. Mount `/proc`.
fn setup_container_root(rootfs: &Path) -> io::Result<()> {
    let rootfs_c = CString::try_from_bytes(rootfs.as_os_str().as_bytes()).unwrap();
    let root_c = CString::from_str("/").unwrap();
    let proc_c = CString::from_str("proc").unwrap();
    let proc_dir_c = CString::from_str("/proc").unwrap();

    // 1. Remount entire tree as private
    sys::mount(
        None,
        root_c.borrow(),
        None,
        libc::MS_REC | libc::MS_PRIVATE,
        None,
    )?;

    // 2. Bind-mount rootfs onto itself (so it's a mount point)
    sys::mount(
        rootfs_c.borrow().into(),
        rootfs_c.borrow(),
        None,
        libc::MS_BIND | libc::MS_REC,
        None,
    )?;

    // 3. Bind-mount essential device nodes from the host. `mknod` is not permitted
    //    inside user namespaces, so we bind-mount the host's device nodes before
    //    chroot hides them.
    let dev = rootfs.join("dev");
    std::fs::create_dir_all(&dev)?;
    for name in ["null", "zero", "random", "urandom", "full", "tty"] {
        let dst = dev.join(name);
        std::fs::write(&dst, [])?; // create mount target
        let src = CString::from(format!("/dev/{}", name));
        let dst_c = CString::try_from_bytes(dst.as_os_str().as_encoded_bytes()).unwrap();
        sys::mount(
            Some(src.borrow()),
            dst_c.borrow(),
            None,
            libc::MS_BIND,
            None,
        )?;
    }

    // 4. chdir into rootfs
    sys::chdir(rootfs_c.borrow())?;

    // 5. chroot to current directory (".")
    sys::chroot(rootfs_c.borrow())?;

    // 6. chdir to new root
    sys::chdir(root_c.borrow())?;

    // 7. Mount proc
    sys::mount(
        proc_c.borrow().into(),
        proc_dir_c.borrow(),
        proc_c.borrow().into(),
        0,
        None,
    )?;

    Ok(())
}

fn create_overlay_tempdir() -> io::Result<PathBuf> {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
    let path = std::env::temp_dir().join(format!("conrt.{}.{}", std::process::id(), seq));
    std::fs::create_dir(&path)?;
    Ok(path)
}

fn setup_overlay_rootfs(rootfs: &Path, overlay_dir: &Path) -> io::Result<PathBuf> {
    let upper = overlay_dir.join("upper");
    let work = overlay_dir.join("work");
    let merged = overlay_dir.join("merged");

    std::fs::create_dir(&upper)?;
    std::fs::create_dir(&work)?;
    std::fs::create_dir(&merged)?;

    let opts_str = format!(
        "lowerdir={},upperdir={},workdir={}",
        rootfs.display(),
        upper.display(),
        work.display(),
    );
    let opts = CString::from(opts_str.as_str());
    let overlay_c = CString::from("overlay");

    sys::mount(
        overlay_c.borrow().into(),
        CString::try_from_bytes(merged.as_os_str().as_bytes())
            .unwrap()
            .borrow(),
        overlay_c.borrow().into(),
        0,
        opts.borrow().into(),
    )?;

    Ok(merged)
}

fn cleanup_overlay(dir: &Path) {
    use std::os::unix::fs::PermissionsExt;

    fn chmod_r(path: &Path) {
        let Ok(meta) = std::fs::symlink_metadata(path) else {
            return;
        };

        if meta.is_dir() {
            match std::fs::read_dir(path) {
                Ok(entries) => {
                    for entry in entries.flatten() {
                        chmod_r(&entry.path());
                    }
                }
                Err(_) => {
                    let _ = std::fs::set_permissions(path, PermissionsExt::from_mode(0o700));
                    if let Ok(entries) = std::fs::read_dir(path) {
                        for entry in entries.flatten() {
                            chmod_r(&entry.path());
                        }
                    }
                }
            }
        }

        let mode = if meta.is_dir() { 0o700 } else { 0o600 };
        let _ = std::fs::set_permissions(path, PermissionsExt::from_mode(mode));
    }

    chmod_r(dir);

    if let Err(e) = std::fs::remove_dir_all(dir) {
        tracing::warn!(%e, path = %dir.display(), "overlay cleanup failed");
    }
}

/// Replace the current process with the given command.
fn execvp(argv: &sys::ArgvSlice) -> io::Error {
    sys::execvp(argv)
}

fn setup_userns_maps(pid: libc::pid_t) -> io::Result<()> {
    let uid = unsafe { libc::getuid() };
    let gid = unsafe { libc::getgid() };

    let setgroups_path = format!("/proc/{}/setgroups", pid);
    if std::fs::write(&setgroups_path, "deny\n").is_err() {
        // setgroups file may not exist on older kernels; ignore
    }

    let uid_map_path = format!("/proc/{}/uid_map", pid);
    std::fs::write(&uid_map_path, format!("0 {} 1\n", uid))?;

    let gid_map_path = format!("/proc/{}/gid_map", pid);
    std::fs::write(&gid_map_path, format!("0 {} 1\n", gid))?;

    Ok(())
}

fn show_logs(id: String, follow: bool, socket_path: PathBuf) -> ExitCode {
    let pid: i32 = match id.parse() {
        Ok(p) => p,
        Err(e) => {
            tracing::error!(%e, "invalid container id (expected numeric PID)");
            return ExitCode::FAILURE;
        }
    };

    if follow {
        follow_logs(pid, &socket_path)
    } else {
        let request = daemon::Request::Logs { pid, follow: false };
        let resp = match daemon::send_request(&socket_path, &request) {
            Ok(r) => r,
            Err(e) => {
                tracing::error!(%e, "cannot connect to daemon");
                return ExitCode::FAILURE;
            }
        };

        if let Ok(success) = serde_json::from_slice::<daemon::LogsResponse>(&resp) {
            for line in &success.lines {
                println!("{line}");
            }
            ExitCode::SUCCESS
        } else if let Ok(err) = serde_json::from_slice::<daemon::ErrorResponse>(&resp) {
            tracing::error!(error = %err.error, "logs failed");
            ExitCode::FAILURE
        } else {
            tracing::error!("invalid daemon response");
            ExitCode::FAILURE
        }
    }
}

fn follow_logs(pid: i32, socket_path: &Path) -> ExitCode {
    let fd = match create_datagram_socket(socket_path) {
        Ok(fd) => fd,
        Err(e) => {
            tracing::error!(%e, "cannot connect to daemon");
            return ExitCode::FAILURE;
        }
    };

    // Send follow request.
    let req = serde_json::to_vec(&daemon::Request::Logs { pid, follow: true }).unwrap();
    let ret = unsafe { libc::send(fd, req.as_ptr() as *const _, req.len(), 0) };
    if ret < 0 {
        let e = std::io::Error::last_os_error();
        tracing::error!(%e, "send failed");
        let _ = unsafe { libc::close(fd) };
        return ExitCode::FAILURE;
    }

    // Receive pipe fd via SCM_RIGHTS.
    #[repr(align(8))]
    #[derive(Default)]
    struct CmsgBuffer {
        bytes: [u8; unsafe { libc::CMSG_SPACE(std::mem::size_of::<RawFd>() as _) as usize }],
    }

    let mut cmsg_buf = CmsgBuffer::default();
    let mut data_buf = [0u8; 16];
    let mut iov = libc::iovec {
        iov_base: data_buf.as_mut_ptr() as *mut _,
        iov_len: data_buf.len(),
    };
    let mut msghdr: libc::msghdr = unsafe { std::mem::zeroed() };
    msghdr.msg_iov = &mut iov;
    msghdr.msg_iovlen = 1;
    msghdr.msg_control = cmsg_buf.bytes.as_mut_ptr() as *mut _;
    msghdr.msg_controllen = cmsg_buf.bytes.len() as _;

    let ret = unsafe { libc::recvmsg(fd, &mut msghdr, 0) };
    if ret < 0 {
        let e = std::io::Error::last_os_error();
        tracing::error!(%e, "recvmsg failed");
        let _ = unsafe { libc::close(fd) };
        return ExitCode::FAILURE;
    }

    let pipe_fd = unsafe {
        let cmsg_hdr = cmsg_buf.bytes.as_ptr() as *const libc::cmsghdr;
        if (*cmsg_hdr).cmsg_len == 0
            || (*cmsg_hdr).cmsg_level != libc::SOL_SOCKET
            || (*cmsg_hdr).cmsg_type != libc::SCM_RIGHTS
        {
            tracing::error!("daemon did not send a pipe fd");
            let _ = libc::close(fd);
            return ExitCode::FAILURE;
        }
        let data_ptr = libc::CMSG_DATA(cmsg_hdr);
        data_ptr.cast::<RawFd>().read()
    };
    let _ = unsafe { libc::close(fd) };

    // Read from pipe until EOF, writing to stdout.
    let mut pipe_reader = unsafe { File::from_raw_fd(pipe_fd) };
    let mut buf = [0u8; 4096];
    loop {
        match pipe_reader.read(&mut buf) {
            Ok(0) => break ExitCode::SUCCESS,
            Ok(n) => {
                let _ = std::io::stdout().write_all(&buf[..n]);
            }
            Err(e) => {
                tracing::error!(%e, "pipe read error");
                break ExitCode::FAILURE;
            }
        }
    }
}

fn create_datagram_socket(socket_path: &Path) -> io::Result<RawFd> {
    unsafe {
        let fd = libc::socket(libc::AF_UNIX, libc::SOCK_DGRAM | libc::SOCK_CLOEXEC, 0);
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        let mut addr: libc::sockaddr_un = std::mem::zeroed();
        addr.sun_family = libc::AF_UNIX as _;
        let ret = libc::bind(
            fd,
            &addr as *const _ as *const libc::sockaddr,
            std::mem::size_of::<libc::sa_family_t>() as _,
        );
        if ret < 0 {
            let e = io::Error::last_os_error();
            let _ = libc::close(fd);
            return Err(e);
        }
        let socket_c = std::ffi::CString::new(socket_path.to_str().unwrap()).unwrap();
        let mut dest: libc::sockaddr_un = std::mem::zeroed();
        dest.sun_family = libc::AF_UNIX as _;
        std::ptr::copy_nonoverlapping(
            socket_c.as_ptr(),
            dest.sun_path.as_mut_ptr(),
            socket_c.as_bytes().len(),
        );
        let addr_len =
            std::mem::size_of::<libc::sa_family_t>() + socket_c.as_bytes_with_nul().len();
        let ret = libc::connect(
            fd,
            &dest as *const _ as *const libc::sockaddr,
            addr_len as _,
        );
        if ret < 0 {
            let e = io::Error::last_os_error();
            let _ = libc::close(fd);
            return Err(e);
        }
        Ok(fd)
    }
}

fn list_containers(socket_path: PathBuf) -> ExitCode {
    let request = daemon::Request::List;
    let resp = match daemon::send_request(&socket_path, &request) {
        Ok(r) => r,
        Err(e) => {
            tracing::error!(%e, "cannot connect to daemon");
            return ExitCode::FAILURE;
        }
    };

    let resp: daemon::ListResponse = match serde_json::from_slice(&resp) {
        Ok(r) => r,
        Err(e) => {
            tracing::error!(%e, "invalid daemon response");
            return ExitCode::FAILURE;
        }
    };

    if resp.containers.is_empty() {
        println!("No running containers");
    } else {
        for c in &resp.containers {
            println!("{:>6}  {:50} {}", c.pid, c.command, c.start_time);
        }
    }
    ExitCode::SUCCESS
}

fn kill_container(pid: i32, socket_path: PathBuf) -> ExitCode {
    let request = daemon::Request::Kill { pid };
    let resp = match daemon::send_request(&socket_path, &request) {
        Ok(r) => r,
        Err(e) => {
            tracing::error!(%e, "cannot connect to daemon");
            return ExitCode::FAILURE;
        }
    };

    let resp: daemon::KillResponse = match serde_json::from_slice(&resp) {
        Ok(r) => r,
        Err(e) => {
            tracing::error!(%e, "invalid daemon response");
            return ExitCode::FAILURE;
        }
    };

    if resp.ok {
        println!("killed {pid}");
        ExitCode::SUCCESS
    } else {
        tracing::error!(
            error = %resp.error.as_deref().unwrap_or("unknown"),
            "kill failed"
        );
        ExitCode::FAILURE
    }
}
