pub mod daemon;
pub mod interprocess;
pub mod pty;
pub mod uring;

use std::ffi::c_int;
use std::io;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::io::RawFd;
use std::path::Path;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use conrt_cstring::CString;
use conrt_sys as sys;

// ── Attach protocol helpers (framed Unix stream) ─────────────────────────

pub fn build_frame(ty: u8, payload: &[u8]) -> Vec<u8> {
    let mut frame = Vec::with_capacity(3 + payload.len());
    frame.push(ty);
    frame.extend_from_slice(&(payload.len() as u16).to_le_bytes());
    frame.extend_from_slice(payload);
    frame
}

pub fn send_frame(fd: std::os::raw::c_int, ty: u8, payload: &[u8]) -> io::Result<()> {
    let frame = build_frame(ty, payload);
    let written = conrt_sys::write(fd, &frame)? as usize;
    if written != frame.len() {
        return Err(io::Error::new(
            io::ErrorKind::WriteZero,
            "partial frame write",
        ));
    }
    Ok(())
}

pub fn read_frame(fd: std::os::raw::c_int) -> io::Result<(u8, Vec<u8>)> {
    let mut header = [0u8; 3];
    let mut off = 0usize;
    while off < header.len() {
        let n = conrt_sys::read(fd, &mut header[off..])?;
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
        let n = conrt_sys::read(fd, &mut buf[off..])?;
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

// ── Terminal helpers ─────────────────────────────────────────────────────

pub fn get_window_size() -> (u16, u16) {
    let ws: libc::winsize = unsafe { std::mem::zeroed() };
    if unsafe { libc::ioctl(libc::STDIN_FILENO, libc::TIOCGWINSZ, &ws) } == 0 {
        (ws.ws_row, ws.ws_col)
    } else {
        (24, 80)
    }
}

pub static WINCH_PENDING: AtomicBool = AtomicBool::new(false);

pub extern "C" fn sigwinch_handler(_sig: c_int) {
    WINCH_PENDING.store(true, std::sync::atomic::Ordering::Release);
}

// ── Container lifecycle ──────────────────────────────────────────────────

pub fn clone3_container(flags: c_int) -> io::Result<Option<libc::pid_t>> {
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
pub fn setup_container_root(rootfs: &Path) -> io::Result<()> {
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

pub fn create_overlay_tempdir() -> io::Result<PathBuf> {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
    let path = std::env::temp_dir().join(format!("conrt.{}.{}", std::process::id(), seq));
    std::fs::create_dir(&path)?;
    Ok(path)
}

pub fn setup_overlay_rootfs(rootfs: &Path, overlay_dir: &Path) -> io::Result<PathBuf> {
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

pub fn cleanup_overlay(dir: &Path) {
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
pub fn execvp(argv: &sys::ArgvSlice) -> io::Error {
    sys::execvp(argv)
}

pub fn setup_userns_maps(pid: libc::pid_t) -> io::Result<()> {
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

// ── Socket helpers ───────────────────────────────────────────────────────

pub fn create_datagram_socket(socket_path: &Path) -> io::Result<RawFd> {
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
