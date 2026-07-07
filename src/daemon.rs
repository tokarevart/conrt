use std::collections::HashMap;
use std::io;
use std::os::fd::AsRawFd;
use std::os::fd::RawFd;
use std::os::unix::net::UnixListener;
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::path::PathBuf;
use std::time::SystemTime;

use serde::Deserialize;
use serde::Serialize;

use crate::cstring::CString;
use crate::interprocess;
use crate::sys;
use crate::uring::Ring;

// ── Protocol ──────────────────────────────────────────────────────────────

#[derive(Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Request {
    Run {
        rootfs: Option<String>,
        net_pid: Option<i32>,
        save: bool,
        command: Vec<String>,
    },
    List,
    Kill {
        pid: i32,
    },
}

#[derive(Serialize)]
struct RunResponse {
    ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pid: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

#[derive(Serialize)]
struct ListResponse {
    containers: Vec<ContainerSummary>,
}

#[derive(Serialize)]
struct ContainerSummary {
    pid: i32,
    command: String,
    start_time: String,
}

#[derive(Serialize)]
struct KillResponse {
    ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

#[derive(Serialize)]
struct ErrorResponse {
    ok: bool,
    error: String,
}

// ── Container State ───────────────────────────────────────────────────────

struct ContainerInfo {
    pid: libc::pid_t,
    command: String,
    overlay_dir: Option<PathBuf>,
    save: bool,
    start_time: SystemTime,
}

pub struct Daemon {
    socket_path: PathBuf,
    containers: HashMap<libc::pid_t, ContainerInfo>,
}

impl Daemon {
    pub fn new(socket_path: PathBuf) -> Self {
        Self {
            socket_path,
            containers: HashMap::new(),
        }
    }

    pub fn run(&mut self) -> io::Result<()> {
        let dir = self.socket_path.parent().unwrap();
        std::fs::create_dir_all(dir)?;

        if self.socket_path.exists() {
            std::fs::remove_file(&self.socket_path)?;
        }

        let listener = UnixListener::bind(&self.socket_path)?;
        listener.set_nonblocking(true)?;
        tracing::info!(path = %self.socket_path.display(), "daemon listening");

        let sigchld = setup_sigchld_fd()?;

        let listener_fd = listener.as_raw_fd();

        let mut ring = Ring::new(8)?;
        ring.poll_add(listener_fd, libc::POLLIN as u32, 0);
        ring.poll_add(sigchld, libc::POLLIN as u32, 1);

        loop {
            ring.submit_and_wait(1)?;

            let cq = ring.completion();
            for cqe in cq {
                let ret = cqe.result();
                if ret < 0 {
                    continue;
                }

                match cqe.user_data() {
                    0 => loop {
                        match listener.accept() {
                            Ok((stream, _)) => {
                                if let Err(e) = self.handle_client(stream) {
                                    tracing::warn!(%e, "client handler error");
                                }
                            }
                            Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                            Err(e) => {
                                tracing::warn!(%e, "accept error");
                                break;
                            }
                        }
                    },
                    1 => {
                        self.reap_children();
                    }
                    _ => {}
                }
            }
        }
    }

    fn handle_client(&mut self, stream: UnixStream) -> io::Result<()> {
        let raw_fd = stream.as_raw_fd();

        let mut len_buf = [0u8; 4];
        read_exact(raw_fd, &mut len_buf)?;
        let payload_len = u32::from_le_bytes(len_buf) as usize;

        let mut payload = vec![0u8; payload_len];
        read_exact(raw_fd, &mut payload)?;

        let request: Request = match serde_json::from_slice(&payload) {
            Ok(r) => r,
            Err(e) => {
                let resp = ErrorResponse {
                    ok: false,
                    error: format!("invalid request: {e}"),
                };
                return write_response(raw_fd, &resp);
            }
        };

        match request {
            Request::Run {
                rootfs,
                net_pid,
                save,
                command,
            } => self.handle_run(raw_fd, rootfs, net_pid, save, command),
            Request::List => self.handle_list(raw_fd),
            Request::Kill { pid } => self.handle_kill(raw_fd, pid),
        }
    }

    fn handle_run(
        &mut self,
        fd: RawFd,
        rootfs: Option<String>,
        net_pid: Option<i32>,
        save: bool,
        command: Vec<String>,
    ) -> io::Result<()> {
        let rootfs = match rootfs {
            Some(p) => match Path::new(&p).canonicalize() {
                Ok(path) => Some(path),
                Err(e) => {
                    let resp = ErrorResponse {
                        ok: false,
                        error: format!("invalid rootfs path: {e}"),
                    };
                    return write_response(fd, &resp);
                }
            },
            None => None,
        };

        let overlay_dir = match rootfs {
            Some(_) => match crate::create_overlay_tempdir() {
                Ok(dir) => Some(dir),
                Err(e) => {
                    let resp = ErrorResponse {
                        ok: false,
                        error: format!("cannot create overlay tempdir: {e}"),
                    };
                    return write_response(fd, &resp);
                }
            },
            None => None,
        };

        let signal = match interprocess::OneshotSignal::new() {
            Ok(s) => s,
            Err(e) => {
                let resp = ErrorResponse {
                    ok: false,
                    error: format!("sync pipe creation failed: {e}"),
                };
                return write_response(fd, &resp);
            }
        };

        let command_c: Vec<CString> = match command
            .iter()
            .map(|s| CString::try_from_bytes(s.as_bytes()))
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(v) => v,
            Err(e) => {
                let resp = ErrorResponse {
                    ok: false,
                    error: format!("invalid command (null byte): {e}"),
                };
                return write_response(fd, &resp);
            }
        };

        let (clone_flags, needs_userns_maps) = match net_pid {
            Some(pid) => {
                let user_f = match std::fs::File::open(format!("/proc/{pid}/ns/user")) {
                    Ok(f) => f,
                    Err(e) => {
                        let resp = ErrorResponse {
                            ok: false,
                            error: format!("cannot open pid {pid} user ns: {e}"),
                        };
                        return write_response(fd, &resp);
                    }
                };
                let net_f = match std::fs::File::open(format!("/proc/{pid}/ns/net")) {
                    Ok(f) => f,
                    Err(e) => {
                        let resp = ErrorResponse {
                            ok: false,
                            error: format!("cannot open pid {pid} net ns: {e}"),
                        };
                        return write_response(fd, &resp);
                    }
                };
                if let Err(e) = sys::setns(user_f.as_raw_fd(), libc::CLONE_NEWUSER) {
                    let resp = ErrorResponse {
                        ok: false,
                        error: format!("setns(CLONE_NEWUSER) into pid {pid} failed: {e}"),
                    };
                    return write_response(fd, &resp);
                }
                if let Err(e) = sys::setns(net_f.as_raw_fd(), libc::CLONE_NEWNET) {
                    let resp = ErrorResponse {
                        ok: false,
                        error: format!("setns(CLONE_NEWNET) into pid {pid} failed: {e}"),
                    };
                    return write_response(fd, &resp);
                }
                (
                    libc::CLONE_NEWPID
                        | libc::CLONE_NEWNS
                        | libc::CLONE_NEWUTS
                        | libc::CLONE_NEWIPC,
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

        let clone_result = crate::clone3_container(clone_flags);
        match clone_result {
            Err(e) => {
                let resp = ErrorResponse {
                    ok: false,
                    error: format!("clone3 failed: {e}"),
                };
                write_response(fd, &resp)
            }
            Ok(None) => {
                // ── CHILD ──
                if let Err(e) = signal.wait() {
                    tracing::error!(%e, "sync wait failed");
                    std::process::exit(1);
                }

                if let Err(e) = sys::prctl(libc::PR_SET_PDEATHSIG as _, libc::SIGKILL as _, 0, 0, 0)
                {
                    tracing::error!(%e, "PR_SET_PDEATHSIG failed");
                    std::process::exit(1);
                }

                if let Err(e) = sys::bring_up_lo() {
                    tracing::warn!(%e, "bring_up_lo failed");
                }

                if let Err(e) = sys::sethostname("conrt") {
                    tracing::error!(%e, "sethostname failed");
                }

                if let Some(ref rootfs_path) = rootfs {
                    let overlay =
                        overlay_dir.expect("overlay_dir is always created when rootfs is provided");

                    let container_root = match crate::setup_overlay_rootfs(rootfs_path, &overlay) {
                        Ok(merged) => merged,
                        Err(e) => {
                            tracing::error!(%e, "overlay setup failed");
                            std::process::exit(1);
                        }
                    };

                    if let Err(e) = crate::setup_container_root(&container_root) {
                        tracing::error!(%e, "container root setup failed");
                        std::process::exit(1);
                    }
                }

                let argv = sys::Argv::new(command_c);
                let errno = crate::execvp(argv.as_slice());
                tracing::error!(%errno, "execvp failed");
                std::process::exit(1)
            }
            Ok(Some(pid)) => {
                // ── PARENT ──
                let maps_result = if needs_userns_maps {
                    crate::setup_userns_maps(pid)
                } else {
                    Ok(())
                };
                signal.signal();

                if let Err(e) = maps_result {
                    let resp = ErrorResponse {
                        ok: false,
                        error: format!("container aborted: {e}"),
                    };
                    return write_response(fd, &resp);
                }

                let cmd_str = command.join(" ");
                self.containers.insert(pid, ContainerInfo {
                    pid,
                    command: cmd_str,
                    overlay_dir,
                    save,
                    start_time: SystemTime::now(),
                });

                tracing::info!(%pid, "detached container started");

                let resp = RunResponse {
                    ok: true,
                    pid: Some(pid),
                    error: None,
                };
                write_response(fd, &resp)
            }
        }
    }

    fn handle_list(&self, fd: RawFd) -> io::Result<()> {
        let now = SystemTime::now();
        let containers: Vec<ContainerSummary> = self
            .containers
            .values()
            .map(|info| {
                let age = now.duration_since(info.start_time).unwrap_or_default();
                ContainerSummary {
                    pid: info.pid,
                    command: info.command.clone(),
                    start_time: format!("{:.1}s", age.as_secs_f64()),
                }
            })
            .collect();

        let resp = ListResponse { containers };
        write_response(fd, &resp)
    }

    fn handle_kill(&self, fd: RawFd, pid: i32) -> io::Result<()> {
        if !self.containers.contains_key(&(pid as libc::pid_t)) {
            let resp = KillResponse {
                ok: false,
                error: Some(format!("container {pid} not found")),
            };
            return write_response(fd, &resp);
        }

        let ret = unsafe { libc::kill(pid as libc::pid_t, libc::SIGKILL) };
        if ret < 0 {
            let err = io::Error::last_os_error();
            let resp = KillResponse {
                ok: false,
                error: Some(format!("kill failed: {err}")),
            };
            return write_response(fd, &resp);
        }

        tracing::info!(%pid, "sent SIGKILL");
        let resp = KillResponse {
            ok: true,
            error: None,
        };
        write_response(fd, &resp)
    }

    fn reap_children(&mut self) {
        loop {
            let mut status: i32 = 0;
            match sys::wait4(-1, &mut status, libc::WNOHANG, None) {
                Ok(pid) if pid > 0 => {
                    tracing::info!(%pid, %status, "container exited");
                    if let Some(info) = self.containers.remove(&pid)
                        && let Some(ref overlay) = info.overlay_dir
                        && !info.save
                    {
                        crate::cleanup_overlay(overlay);
                    }
                }
                Ok(_) => break,
                Err(e) => {
                    if e.raw_os_error() == Some(libc::ECHILD) {
                        break;
                    }
                    tracing::warn!(%e, "waitpid error during reap");
                    break;
                }
            }
        }
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────

fn setup_sigchld_fd() -> io::Result<RawFd> {
    let mut mask: libc::sigset_t = unsafe { std::mem::zeroed() };
    unsafe { libc::sigemptyset(&mut mask) };
    unsafe { libc::sigaddset(&mut mask, libc::SIGCHLD) };
    unsafe { libc::pthread_sigmask(libc::SIG_BLOCK, &mask, std::ptr::null_mut()) };
    let fd = sys::signalfd(-1, &mask, libc::SFD_CLOEXEC | libc::SFD_NONBLOCK)?;
    Ok(fd)
}

fn read_exact(fd: RawFd, buf: &mut [u8]) -> io::Result<()> {
    let mut offset = 0;
    while offset < buf.len() {
        let n = sys::read(fd, &mut buf[offset..])?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "connection closed",
            ));
        }
        offset += n as usize;
    }
    Ok(())
}

fn write_response<T: Serialize>(fd: RawFd, resp: &T) -> io::Result<()> {
    let payload = serde_json::to_vec(resp).unwrap();
    let len = payload.len() as u32;
    let header = len.to_le_bytes();
    sys::write(fd, &header)?;
    sys::write(fd, &payload)?;
    Ok(())
}

// ── Client helpers ────────────────────────────────────────────────────────

pub fn send_request(socket_path: &Path, request: &Request) -> io::Result<Vec<u8>> {
    use std::os::unix::io::AsRawFd;
    let stream = UnixStream::connect(socket_path)?;
    let fd = stream.as_raw_fd();

    let payload = serde_json::to_vec(request).unwrap();
    let len = payload.len() as u32;
    let header = len.to_le_bytes();

    sys::write(fd, &header)?;
    sys::write(fd, &payload)?;

    let mut len_buf = [0u8; 4];
    read_exact(fd, &mut len_buf)?;
    let resp_len = u32::from_le_bytes(len_buf) as usize;
    let mut resp_buf = vec![0u8; resp_len];
    read_exact(fd, &mut resp_buf)?;

    Ok(resp_buf)
}
