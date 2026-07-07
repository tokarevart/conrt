mod cstring;
mod daemon;
mod interprocess;
mod pty;
mod sys;
mod uring;

use std::ffi::c_int;
use std::io;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::io::AsRawFd;
use std::path::Path;
use std::path::PathBuf;
use std::process::ExitCode;
use std::str::FromStr;

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
        #[arg(short)]
        t: bool,

        /// Detach and hand off to the daemon
        #[arg(long)]
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
            t,
            detach,
            socket_path,
            command,
        } => {
            if detach {
                if t {
                    tracing::error!("--detach is incompatible with -t");
                    return ExitCode::FAILURE;
                }
                run_detach(
                    socket_path.unwrap_or_else(default_socket_path),
                    rootfs,
                    net_pid.map(|p| p as libc::pid_t),
                    save,
                    command,
                )
            } else {
                run_container(RunArgs {
                    rootfs,
                    net_pid: net_pid.map(|p| p as libc::pid_t),
                    save,
                    tty: t,
                    command,
                })
            }
        }
        Cli::Logs { id } => show_logs(id),
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
    command: Vec<CString>,
) -> ExitCode {
    let cmd_strs: Vec<String> = command
        .iter()
        .map(|c| String::from_utf8_lossy(c.to_bytes()).into_owned())
        .collect();
    let rootfs_str = rootfs.as_ref().map(|p| p.to_string_lossy().into_owned());

    let request = daemon::Request::Run {
        rootfs: rootfs_str,
        net_pid,
        save,
        command: cmd_strs,
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
    command: Vec<CString>,
}

fn run_container(args: RunArgs) -> ExitCode {
    let rootfs = match args.rootfs {
        Some(p) => match p.canonicalize() {
            Ok(path) => Some(path),
            Err(e) => {
                tracing::error!(%e, rootfs = %p.display(), "invalid rootfs path");
                return ExitCode::FAILURE;
            }
        },
        None => None,
    };

    let overlay_dir = match rootfs {
        Some(_) => match create_overlay_tempdir() {
            Ok(dir) => Some(dir),
            Err(e) => {
                tracing::error!(%e, "cannot create overlay tempdir");
                return ExitCode::FAILURE;
            }
        },
        None => None,
    };

    let (master, slave) = if args.tty {
        match pty::open_pty() {
            Ok((m, s)) => (Some(m), Some(s)),
            Err(e) => {
                tracing::error!(%e, "pty allocation failed");
                return ExitCode::FAILURE;
            }
        }
    } else {
        (None, None)
    };

    let signal = match interprocess::OneshotSignal::new() {
        Ok(s) => s,
        Err(e) => {
            tracing::error!(%e, "sync pipe creation failed");
            return ExitCode::FAILURE;
        }
    };

    let (clone_flags, needs_userns_maps) = match args.net_pid {
        Some(pid) => {
            let user_f = match std::fs::File::open(format!("/proc/{pid}/ns/user")) {
                Ok(f) => f,
                Err(e) => {
                    tracing::error!(%e, "cannot open pid {pid} user ns");
                    return ExitCode::FAILURE;
                }
            };
            let net_f = match std::fs::File::open(format!("/proc/{pid}/ns/net")) {
                Ok(f) => f,
                Err(e) => {
                    tracing::error!(%e, "cannot open pid {pid} net ns");
                    return ExitCode::FAILURE;
                }
            };
            if let Err(e) = sys::setns(user_f.as_raw_fd(), libc::CLONE_NEWUSER) {
                tracing::error!(%e, "setns(CLONE_NEWUSER) into pid {pid} failed");
                return ExitCode::FAILURE;
            }
            if let Err(e) = sys::setns(net_f.as_raw_fd(), libc::CLONE_NEWNET) {
                tracing::error!(%e, "setns(CLONE_NEWNET) into pid {pid} failed");
                return ExitCode::FAILURE;
            }
            (
                libc::CLONE_NEWPID | libc::CLONE_NEWNS | libc::CLONE_NEWUTS | libc::CLONE_NEWIPC,
                false,
            )
        }
        None => (
            libc::CLONE_NEWPID
                | libc::CLONE_NEWNS
                | libc::CLONE_NEWUTS
                | libc::CLONE_NEWIPC
                | libc::CLONE_NEWUSER
                | libc::CLONE_NEWNET,
            true,
        ),
    };

    match clone3_container(clone_flags) {
        Err(e) => {
            tracing::error!(%e, "clone3 failed");
            ExitCode::FAILURE
        }
        Ok(None) => {
            // ---- CHILD ----

            // Close master — only the parent needs it
            drop(master);

            // Set up PTY slave as the controlling terminal
            if let Some(slave) = slave
                && let Err(e) = slave.make_controlling()
            {
                tracing::error!(%e, "pty setup failed");
                std::process::exit(1);
            }

            if let Err(e) = signal.wait() {
                tracing::error!(%e, "sync wait failed");
                std::process::exit(1);
            }

            if let Err(e) = sys::bring_up_lo() {
                tracing::warn!(%e, "bring_up_lo failed");
            }

            if let Err(e) = sys::sethostname("conrt") {
                tracing::error!(%e, "sethostname failed");
            }

            if let Some(ref rootfs) = rootfs {
                let overlay_dir =
                    overlay_dir.expect("overlay_dir is always created when rootfs is provided");

                let container_root = match setup_overlay_rootfs(rootfs, &overlay_dir) {
                    Ok(merged) => merged,
                    Err(e) => {
                        tracing::error!(%e, "overlay setup failed");
                        std::process::exit(1);
                    }
                };

                if let Err(e) = setup_container_root(&container_root) {
                    tracing::error!(%e, "container root setup failed");
                    std::process::exit(1);
                }
            }

            let argv = sys::Argv::new(args.command);
            let errno = execvp(argv.as_slice());
            tracing::error!(%errno, "execvp failed");
            std::process::exit(1)
        }
        Ok(Some(pid)) => {
            // ---- PARENT ----

            // Close slave — only the child needs it
            drop(slave);

            let maps_result = if needs_userns_maps {
                setup_userns_maps(pid)
            } else {
                Ok(())
            };
            signal.signal();

            if let Err(e) = maps_result {
                tracing::error!(%e, "container aborted");
                return ExitCode::FAILURE;
            }

            tracing::info!(child = pid, "container started");

            // PTY I/O relay — blocks until the child closes the slave
            if let Some(ref master) = master {
                let raw = if unsafe { libc::isatty(libc::STDIN_FILENO) } != 0 {
                    pty::set_raw_terminal().ok()
                } else {
                    None
                };
                if let Err(e) = pty::relay_pty(master.raw_fd()) {
                    tracing::error!(%e, "pty relay failed");
                }
                if let Some(termios) = raw {
                    pty::restore_terminal(&termios).ok();
                }
            }
            // master dropped here if Some — closes the master fd

            let exit_code = match wait_for_child(pid) {
                Ok(code) => code,
                Err(e) => {
                    tracing::error!(%e, "wait failed");
                    ExitCode::FAILURE
                }
            };

            if let Some(ref overlay) = overlay_dir
                && !args.save
            {
                cleanup_overlay(overlay);
            }

            exit_code
        }
    }
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
/// 3. `chdir` into rootfs.
/// 4. `chroot(".")` — change root to the bound rootfs.
/// 5. `chdir("/")`.
/// 6. Mount `/proc`, `/sys`, `/dev`.
fn setup_container_root(rootfs: &Path) -> io::Result<()> {
    let rootfs_c = CString::try_from_bytes(rootfs.as_os_str().as_bytes()).unwrap();
    let root_c = CString::from_str("/").unwrap();
    let proc_c = CString::from_str("proc").unwrap();
    let proc_dir_c = CString::from_str("/proc").unwrap();
    let tmpfs_c = CString::from_str("tmpfs").unwrap();
    let dev_c = CString::from_str("/dev").unwrap();

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

    // 3. chdir into rootfs
    sys::chdir(rootfs_c.borrow())?;

    // 4. chroot to current directory (".")
    sys::chroot(rootfs_c.borrow())?;

    // 5. chdir to new root
    sys::chdir(root_c.borrow())?;

    // 6. Mount proc
    sys::mount(
        proc_c.borrow().into(),
        proc_dir_c.borrow(),
        proc_c.borrow().into(),
        0,
        None,
    )?;

    // 7. Mount dev (tmpfs)
    sys::mount(None, dev_c.borrow(), tmpfs_c.borrow().into(), 0, None)?;

    Ok(())
}

fn create_overlay_tempdir() -> io::Result<PathBuf> {
    use std::time::SystemTime;
    use std::time::UNIX_EPOCH;
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let path = std::env::temp_dir().join(format!("conrt.{:x}", ts));
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
    if let Err(e) = std::fs::remove_dir_all(dir) {
        tracing::warn!(%e, path = %dir.display(), "overlay cleanup failed");
    }
}

/// Replace the current process with the given command.
fn execvp(argv: &sys::ArgvSlice) -> io::Error {
    sys::execvp(argv)
}

/// Wait for a child process and return its exit code.
fn wait_for_child(pid: libc::pid_t) -> io::Result<ExitCode> {
    let mut status: i32 = 0;
    let _ = sys::wait4(pid, &mut status, 0, None)?;
    tracing::info!(%status, "container exited");
    if libc::WIFEXITED(status) {
        Ok(ExitCode::from(libc::WEXITSTATUS(status) as u8))
    } else if libc::WIFSIGNALED(status) {
        Ok(ExitCode::from(128 + libc::WTERMSIG(status) as u8))
    } else {
        Ok(ExitCode::FAILURE)
    }
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

fn show_logs(_id: String) -> ExitCode {
    tracing::error!("container logs not implemented yet");
    ExitCode::FAILURE
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

    #[derive(serde::Deserialize)]
    struct ListResp {
        containers: Vec<ListContainer>,
    }
    #[derive(serde::Deserialize)]
    struct ListContainer {
        pid: i32,
        command: String,
        start_time: String,
    }

    let resp: ListResp = match serde_json::from_slice(&resp) {
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

    #[derive(serde::Deserialize)]
    struct KillResp {
        ok: bool,
        error: Option<String>,
    }

    let resp: KillResp = match serde_json::from_slice(&resp) {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rootfs_nonexistent_returns_failure() {
        let code = run_container(RunArgs {
            rootfs: Some("/definitely/not/a/real/path".into()),
            net_pid: None,
            save: false,
            tty: false,
            command: vec![CString::from_str("/bin/true").unwrap()],
        });
        assert_eq!(code, ExitCode::FAILURE);
    }
}
