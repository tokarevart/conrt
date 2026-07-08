use std::collections::HashMap;
use std::io;
use std::os::fd::AsRawFd;
use std::os::fd::RawFd;
use std::os::unix::net::UnixListener;
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::path::PathBuf;
use std::time::SystemTime;

use bbqueue::nicknames::Churrasco;
use libc::pid_t;
use serde::Deserialize;
use serde::Serialize;

use crate::cstring::CString;
use crate::interprocess;
use crate::sys;
use crate::uring::Ring;

const LOG_CAPACITY: usize = 65536;

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
    Logs {
        pid: i32,
    },
}

#[derive(Serialize, Deserialize)]
pub struct RunResponse {
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pid: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Serialize, Deserialize)]
pub struct ListResponse {
    pub containers: Vec<ContainerSummary>,
}

#[derive(Serialize, Deserialize)]
pub struct ContainerSummary {
    pub pid: i32,
    pub command: String,
    pub start_time: String,
}

#[derive(Serialize, Deserialize)]
pub struct KillResponse {
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Serialize, Deserialize)]
pub struct LogsResponse {
    pub lines: Vec<String>,
}

#[derive(Serialize, Deserialize)]
pub struct ErrorResponse {
    pub ok: bool,
    pub error: String,
}

// ── Container State ───────────────────────────────────────────────────────

struct ContainerInfo {
    pid: pid_t,
    command: String,
    overlay_dir: Option<PathBuf>,
    save: bool,
    start_time: SystemTime,
    bbq: Churrasco<LOG_CAPACITY>,
}

pub struct Daemon {
    socket_path: PathBuf,
    containers: HashMap<pid_t, ContainerInfo>,
    output_fds: HashMap<u64, (RawFd, pid_t)>,
    line_bufs: HashMap<u64, Vec<u8>>,
    next_output_id: u64,
    log_graveyard: HashMap<pid_t, Churrasco<LOG_CAPACITY>>,
}

impl Daemon {
    pub fn new(socket_path: PathBuf) -> Self {
        Self {
            socket_path,
            containers: HashMap::new(),
            output_fds: HashMap::new(),
            line_bufs: HashMap::new(),
            next_output_id: 2,
            log_graveyard: HashMap::new(),
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

        let listener_fd = listener.as_raw_fd();
        let sigchld = setup_sigchld_fd()?;

        let mut ring = Ring::new(8)?;
        ring.poll_add(listener_fd, libc::POLLIN as u32, 0);
        ring.poll_add(sigchld, libc::POLLIN as u32, 1);

        loop {
            ring.submit_and_wait(1)?;

            let entries: Vec<_> = ring
                .completion()
                .map(|cqe| (cqe.user_data(), cqe.result()))
                .collect();
            for (user_data, ret) in entries {
                match user_data {
                    0 => {
                        if ret < 0 {
                            continue;
                        }
                        loop {
                            match listener.accept() {
                                Ok((stream, _)) => {
                                    if let Err(e) = self.handle_client(stream, &mut ring) {
                                        tracing::warn!(%e, "client handler error");
                                    }
                                }
                                Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                                Err(e) => {
                                    tracing::warn!(%e, "accept error");
                                    break;
                                }
                            }
                        }
                    }
                    1 => {
                        if ret < 0 {
                            continue;
                        }
                        self.reap_children();
                    }
                    id => {
                        if ret < 0 {
                            self.cleanup_output(id);
                            continue;
                        }
                        if let Err(e) = self.handle_container_output(id, ret as u32) {
                            tracing::warn!(%e, "container output error");
                            self.cleanup_output(id);
                        }
                    }
                }
            }
        }
    }

    fn handle_client(&mut self, stream: UnixStream, ring: &mut Ring) -> io::Result<()> {
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
            } => self.handle_run(raw_fd, ring, rootfs, net_pid, save, command),
            Request::List => self.handle_list(raw_fd),
            Request::Kill { pid } => self.handle_kill(raw_fd, pid),
            Request::Logs { pid } => self.handle_logs(raw_fd, pid),
        }
    }

    fn handle_run(
        &mut self,
        fd: RawFd,
        ring: &mut Ring,
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

        // Create pipe for capturing container stdout/stderr
        let mut pipe_fds = sys::FdPair {
            read: -1,
            write: -1,
        };
        if let Err(e) = sys::pipe2(&mut pipe_fds, libc::O_CLOEXEC | libc::O_NONBLOCK) {
            let resp = ErrorResponse {
                ok: false,
                error: format!("log pipe creation failed: {e}"),
            };
            return write_response(fd, &resp);
        }

        let command_c: Vec<CString> = match command
            .iter()
            .map(|s| CString::try_from_bytes(s.as_bytes()))
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(v) => v,
            Err(e) => {
                sys::close(pipe_fds.read);
                sys::close(pipe_fds.write);
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
                        sys::close(pipe_fds.read);
                        sys::close(pipe_fds.write);
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
                        sys::close(pipe_fds.read);
                        sys::close(pipe_fds.write);
                        let resp = ErrorResponse {
                            ok: false,
                            error: format!("cannot open pid {pid} net ns: {e}"),
                        };
                        return write_response(fd, &resp);
                    }
                };
                if let Err(e) = sys::setns(user_f.as_raw_fd(), libc::CLONE_NEWUSER) {
                    sys::close(pipe_fds.read);
                    sys::close(pipe_fds.write);
                    let resp = ErrorResponse {
                        ok: false,
                        error: format!("setns(CLONE_NEWUSER) into pid {pid} failed: {e}"),
                    };
                    return write_response(fd, &resp);
                }
                if let Err(e) = sys::setns(net_f.as_raw_fd(), libc::CLONE_NEWNET) {
                    sys::close(pipe_fds.read);
                    sys::close(pipe_fds.write);
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
                sys::close(pipe_fds.read);
                sys::close(pipe_fds.write);
                let resp = ErrorResponse {
                    ok: false,
                    error: format!("clone3 failed: {e}"),
                };
                write_response(fd, &resp)
            }
            Ok(None) => {
                // ── CHILD ──
                sys::close(pipe_fds.read);
                let _ = sys::dup2(pipe_fds.write, libc::STDOUT_FILENO);
                let _ = sys::dup2(pipe_fds.write, libc::STDERR_FILENO);
                if pipe_fds.write != libc::STDOUT_FILENO && pipe_fds.write != libc::STDERR_FILENO {
                    sys::close(pipe_fds.write);
                }

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
                sys::close(pipe_fds.write);

                let output_id = self.next_output_id;
                self.next_output_id += 1;
                ring.poll_add(pipe_fds.read, libc::POLLIN as u32, output_id);
                self.output_fds.insert(output_id, (pipe_fds.read, pid));
                self.line_bufs.insert(output_id, Vec::new());

                let maps_result = if needs_userns_maps {
                    crate::setup_userns_maps(pid)
                } else {
                    Ok(())
                };
                signal.signal();

                if let Err(e) = maps_result {
                    self.cleanup_output(output_id);
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
                    bbq: Churrasco::new(),
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
        if !self.containers.contains_key(&(pid as pid_t)) {
            let resp = KillResponse {
                ok: false,
                error: Some(format!("container {pid} not found")),
            };
            return write_response(fd, &resp);
        }

        let ret = unsafe { libc::kill(pid as pid_t, libc::SIGKILL) };
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
                    if let Some(mut info) = self.containers.remove(&pid) {
                        self.log_graveyard
                            .insert(pid, std::mem::replace(&mut info.bbq, Churrasco::new()));
                        if let Some(ref overlay) = info.overlay_dir
                            && !info.save
                        {
                            crate::cleanup_overlay(overlay);
                        }
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

    fn handle_container_output(&mut self, id: u64, revents: u32) -> io::Result<()> {
        let (fd, pid) = match self.output_fds.get(&id) {
            Some(v) => *v,
            None => return Ok(()),
        };

        if revents & libc::POLLIN as u32 != 0 {
            let buf = self.line_bufs.get_mut(&id).unwrap();
            let read_offset = buf.len();
            buf.resize(read_offset + 4096, 0);
            match sys::read(fd, &mut buf[read_offset..]) {
                Ok(n) if n > 0 => {
                    let total = read_offset + n as usize;
                    buf.truncate(total);
                    let mut start = 0usize;
                    let mut i = 0;
                    while i < total {
                        if buf[i] == b'\n' {
                            let line = std::str::from_utf8(&buf[start..i])
                                .unwrap_or_default()
                                .to_string();
                            if let Some(info) = self.containers.get_mut(&pid) {
                                let prod = info.bbq.stream_producer();
                                if let Ok(mut wgr) = prod.grant_exact(line.len()) {
                                    wgr.copy_from_slice(line.as_bytes());
                                    wgr.commit(line.len());
                                }
                            }
                            start = i + 1;
                        }
                        i += 1;
                    }
                    if start < total {
                        let remaining = buf[start..total].to_vec();
                        *buf = remaining;
                    } else {
                        buf.clear();
                    }
                }
                Ok(_) => {
                    buf.clear();
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
                Err(e) => return Err(e),
            }
        }

        if revents & (libc::POLLHUP | libc::POLLERR) as u32 != 0 {
            self.cleanup_output(id);
        }

        Ok(())
    }

    fn cleanup_output(&mut self, id: u64) {
        if let Some((fd, ..)) = self.output_fds.remove(&id) {
            sys::close(fd);
        }
        self.line_bufs.remove(&id);
    }

    fn handle_logs(&mut self, fd: RawFd, pid: i32) -> io::Result<()> {
        let lines = if let Some(info) = self.containers.get_mut(&pid) {
            drain_bbq(&info.bbq)
        } else if let Some(bbq) = self.log_graveyard.get_mut(&pid) {
            drain_bbq(bbq)
        } else {
            let resp = ErrorResponse {
                ok: false,
                error: format!("container {pid} not found"),
            };
            return write_response(fd, &resp);
        };

        let resp = LogsResponse { lines };
        write_response(fd, &resp)
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────

fn setup_sigchld_fd() -> io::Result<RawFd> {
    let mut mask: libc::sigset_t = unsafe { std::mem::zeroed() };
    unsafe { libc::sigemptyset(&mut mask) };
    unsafe { libc::sigaddset(&mut mask, libc::SIGCHLD) };
    sys::sigprocmask(libc::SIG_BLOCK, Some(&mask), None)?;
    let fd = sys::signalfd(-1, &mask, libc::SFD_CLOEXEC)?;
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

fn drain_bbq(bbq: &Churrasco<LOG_CAPACITY>) -> Vec<String> {
    let cons = bbq.stream_consumer();
    let mut lines = Vec::new();
    while let Ok(grant) = cons.read() {
        let s = std::str::from_utf8(&grant).unwrap_or_default().to_string();
        let len = grant.len();
        lines.push(s);
        grant.release(len);
    }
    lines
}

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
