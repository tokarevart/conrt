use std::collections::HashMap;
use std::io;
use std::os::fd::AsRawFd;
use std::os::fd::RawFd;
use std::os::unix::net::UnixDatagram;
use std::path::Path;
use std::path::PathBuf;
use std::time::SystemTime;

use bbqueue::nicknames::Churrasco;
use io_uring::IoUring;
use libc::pid_t;
use serde::Deserialize;
use serde::Serialize;

use crate::cstring::CString;
use crate::interprocess;
use crate::sys;
use crate::uring;

const LOG_CAPACITY: usize = 65536;
const RING_SIZE: u32 = 1024;

// ── Protocol ──────────────────────────────────────────────────────────────

#[derive(Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Request {
    Run {
        rootfs: Option<String>,
        net_pid: Option<i32>,
        save: bool,
        command: Vec<String>,
        interactive: Option<bool>,
        tty: Option<bool>,
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

struct RunArgs {
    rootfs: Option<String>,
    net_pid: Option<i32>,
    save: bool,
    command: Vec<String>,
}

// ── Datagram receive phases ─────────────────────────────────────────────

#[derive(PartialEq)]
enum RecvPhase {
    Peek,
    Consume,
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
    ring: IoUring,
    sigchld_fd: RawFd,
    sigchld_buf: Vec<u8>,
    datagram_fd: RawFd,
    socket_path: PathBuf,
    containers: HashMap<pid_t, ContainerInfo>,
    outputs: HashMap<u64, Output>,
    next_output_id: u64,
    log_graveyard: HashMap<pid_t, Churrasco<LOG_CAPACITY>>,

    // Datagram receive state
    recv_buf: Vec<u8>,
    recv_addr: libc::sockaddr_un,
    recv_addr_len: libc::socklen_t,
    recv_iov: libc::iovec,
    recv_msghdr: libc::msghdr,
    recv_phase: RecvPhase,
}

/// One container stdout/stderr pipe being drained asynchronously.
struct Output {
    fd: RawFd,
    pid: pid_t,
    read_buf: Vec<u8>,
    line_buf: Vec<u8>,
}

impl Daemon {
    pub fn new(socket_path: PathBuf) -> Self {
        let ring = IoUring::new(RING_SIZE).expect("failed to create io_uring");
        Self {
            ring,
            sigchld_fd: -1,
            sigchld_buf: Vec::new(),
            datagram_fd: -1,
            socket_path,
            containers: HashMap::new(),
            outputs: HashMap::new(),
            next_output_id: 2,
            log_graveyard: HashMap::new(),
            recv_buf: vec![0u8; 65536],
            recv_addr: unsafe { std::mem::zeroed() },
            recv_addr_len: 0,
            recv_iov: unsafe { std::mem::zeroed() },
            recv_msghdr: libc::msghdr {
                msg_name: std::ptr::null_mut(),
                msg_namelen: 0,
                msg_iov: std::ptr::null_mut(),
                msg_iovlen: 0,
                msg_control: std::ptr::null_mut(),
                msg_controllen: 0,
                msg_flags: 0,
            },
            recv_phase: RecvPhase::Peek,
        }
    }

    pub fn run(&mut self) -> io::Result<()> {
        // Self-referential pointers: init AFTER self is at its final location.
        self.recv_addr = unsafe { std::mem::zeroed() };
        self.recv_iov = libc::iovec {
            iov_base: self.recv_buf.as_mut_ptr() as *mut libc::c_void,
            iov_len: self.recv_buf.len(),
        };
        self.recv_msghdr = libc::msghdr {
            msg_name: &mut self.recv_addr as *mut _ as *mut _,
            msg_namelen: size_of::<libc::sockaddr_un>() as _,
            msg_iov: &self.recv_iov as *const _ as *mut _,
            msg_iovlen: 1,
            msg_control: std::ptr::null_mut(),
            msg_controllen: 0,
            msg_flags: 0,
        };

        let dir = self.socket_path.parent().unwrap();
        std::fs::create_dir_all(dir)?;

        if self.socket_path.exists() {
            std::fs::remove_file(&self.socket_path)?;
        }

        let datagram = UnixDatagram::bind(&self.socket_path)?;
        self.datagram_fd = datagram.as_raw_fd();
        tracing::info!(path = %self.socket_path.display(), "daemon listening");

        let sigchld = setup_sigchld_fd()?;
        self.sigchld_fd = sigchld;
        self.sigchld_buf = vec![0u8; std::mem::size_of::<libc::signalfd_siginfo>()];

        {
            let mut sq = self.ring.submission();
            uring::push_recvmsg(
                &mut sq,
                self.datagram_fd,
                &mut self.recv_msghdr as *mut _,
                (libc::MSG_PEEK | libc::MSG_TRUNC) as u32,
                0,
            );
            uring::push_read(&mut sq, sigchld, &mut self.sigchld_buf, 1);
        }

        loop {
            self.ring.submit_and_wait(1)?;

            let entries: Vec<_> = self
                .ring
                .completion()
                .map(|cqe| (cqe.user_data(), cqe.result()))
                .collect();

            for (user_data, ret) in entries {
                tracing::trace!(%ret, ?user_data, "cqe completion");
                match user_data {
                    0 => self.handle_datagram_cqe(ret),
                    1 => self.handle_signal(ret),
                    id => self.handle_output(id, ret),
                }
            }
        }
    }

    // ── Datagram handling (two-phase: peek → consume) ────────────────────

    fn submit_datagram_peek(&mut self) {
        self.recv_msghdr.msg_namelen = size_of::<libc::sockaddr_un>() as _;
        let mut sq = self.ring.submission();
        uring::push_recvmsg(
            &mut sq,
            self.datagram_fd,
            &mut self.recv_msghdr as *mut _,
            (libc::MSG_PEEK | libc::MSG_TRUNC) as u32,
            0,
        );
    }

    fn submit_datagram_consume(&mut self) {
        self.recv_msghdr.msg_namelen = size_of::<libc::sockaddr_un>() as _;
        let mut sq = self.ring.submission();
        uring::push_recvmsg(
            &mut sq,
            self.datagram_fd,
            &mut self.recv_msghdr as *mut _,
            0,
            0,
        );
    }

    fn handle_datagram_cqe(&mut self, ret: i32) {
        match self.recv_phase {
            RecvPhase::Peek => self.handle_datagram_peek(ret),
            RecvPhase::Consume => self.handle_datagram_consume(ret),
        }
    }

    fn handle_datagram_peek(&mut self, ret: i32) {
        tracing::trace!(%ret, "datagram peek");
        let size = ret as usize;
        if size > self.recv_buf.len() {
            // Resize the receive buffer and update the iovec to match.
            self.recv_buf.resize(size, 0);
            self.recv_iov.iov_base = self.recv_buf.as_mut_ptr() as *mut libc::c_void;
            self.recv_iov.iov_len = size;
        }
        self.recv_phase = RecvPhase::Consume;
        self.submit_datagram_consume();
    }

    fn handle_datagram_consume(&mut self, ret: i32) {
        tracing::trace!(%ret, "datagram consume");
        self.recv_phase = RecvPhase::Peek;
        if ret >= 0 {
            let sender = self.recv_addr;
            self.recv_addr_len = self.recv_msghdr.msg_namelen;
            let data = &self.recv_buf[..ret as usize];
            let request: Request = match serde_json::from_slice(data) {
                Ok(r) => r,
                Err(e) => {
                    let resp = serde_json::to_vec(&ErrorResponse {
                        ok: false,
                        error: format!("invalid request: {e}"),
                    })
                    .unwrap();
                    let _ = self.send_datagram(&sender, &resp);
                    self.submit_datagram_peek();
                    return;
                }
            };

            match request {
                Request::Run {
                    rootfs,
                    net_pid,
                    save,
                    command,
                    interactive: _,
                    tty: _,
                } => self.handle_run(sender, RunArgs {
                    rootfs,
                    net_pid,
                    save,
                    command,
                }),
                Request::List => self.handle_list(sender),
                Request::Kill { pid } => self.handle_kill(sender, pid),
                Request::Logs { pid } => self.handle_logs(sender, pid),
            }
        }
        self.submit_datagram_peek();
    }

    fn send_datagram(&self, addr: &libc::sockaddr_un, data: &[u8]) -> io::Result<usize> {
        send_datagram_raw(self.datagram_fd, addr, self.recv_addr_len, data)
    }

    fn handle_signal(&mut self, ret: i32) {
        if ret > 0 {
            self.reap_children();
        }
        {
            let mut sq = self.ring.submission();
            uring::push_read(&mut sq, self.sigchld_fd, &mut self.sigchld_buf, 1);
        }
    }

    // ── Container output (async read that stays in flight) ─────────────────

    fn handle_output(&mut self, id: u64, result: i32) {
        if result <= 0 {
            self.cleanup_output(id);
            return;
        }
        let n = result as usize;
        let _pid = match self.outputs.get(&id) {
            Some(o) => o.pid,
            None => return,
        };

        {
            let output = match self.outputs.get_mut(&id) {
                Some(o) => o,
                None => return,
            };

            output.line_buf.extend_from_slice(&output.read_buf[..n]);

            let mut start = 0usize;
            let total = output.line_buf.len();
            for i in 0..total {
                if output.line_buf[i] == b'\n' {
                    let line = std::str::from_utf8(&output.line_buf[start..i])
                        .unwrap_or_default()
                        .to_string();
                    let line = line + "\n";
                    if let Some(info) = self.containers.get_mut(&output.pid) {
                        let prod = info.bbq.stream_producer();
                        if let Ok(mut wgr) = prod.grant_exact(line.len()) {
                            wgr.copy_from_slice(line.as_bytes());
                            wgr.commit(line.len());
                        }
                    } else if let Some(bbq) = self.log_graveyard.get_mut(&output.pid) {
                        let prod = bbq.stream_producer();
                        if let Ok(mut wgr) = prod.grant_exact(line.len()) {
                            wgr.copy_from_slice(line.as_bytes());
                            wgr.commit(line.len());
                        }
                    }
                    start = i + 1;
                }
            }

            if start < total {
                let remaining = output.line_buf[start..].to_vec();
                output.line_buf = remaining;
            } else {
                output.line_buf.clear();
            }
        }

        let fd = match self.outputs.get(&id) {
            Some(o) => o.fd,
            None => return,
        };
        {
            let mut sq = self.ring.submission();
            let output = self.outputs.get_mut(&id).unwrap();
            uring::push_read(&mut sq, fd, &mut output.read_buf, id);
        }
    }

    fn cleanup_output(&mut self, id: u64) {
        if let Some(output) = self.outputs.remove(&id) {
            sys::close(output.fd);
        }
    }

    // ── Request handlers ──────────────────────────────────────────────────

    fn reply(&self, addr: &libc::sockaddr_un, data: impl serde::Serialize) {
        let resp = serde_json::to_vec(&data).unwrap();
        if let Err(e) = self.send_datagram(addr, &resp) {
            tracing::error!(%e, "reply sendto failed");
        }
    }

    fn handle_run(&mut self, sender: libc::sockaddr_un, args: RunArgs) {
        let output_id = self.next_output_id;
        self.next_output_id += 1;
        let datagram_fd = self.datagram_fd;
        let sender_len = self.recv_addr_len;

        let err = |msg: &str| {
            let resp = serde_json::to_vec(&ErrorResponse {
                ok: false,
                error: msg.to_string(),
            })
            .unwrap();
            let _ = send_datagram_raw(datagram_fd, &sender, sender_len, &resp);
        };

        let rootfs = match args.rootfs {
            Some(p) => match Path::new(&p).canonicalize() {
                Ok(path) => Some(path),
                Err(e) => {
                    err(&format!("invalid rootfs path: {e}"));
                    return;
                }
            },
            None => None,
        };

        let overlay_dir = match rootfs {
            Some(_) => match crate::create_overlay_tempdir() {
                Ok(dir) => Some(dir),
                Err(e) => {
                    err(&format!("cannot create overlay tempdir: {e}"));
                    return;
                }
            },
            None => None,
        };

        let signal = match interprocess::OneshotSignal::new() {
            Ok(s) => s,
            Err(e) => {
                err(&format!("sync pipe creation failed: {e}"));
                return;
            }
        };

        let mut pipe_fds = sys::FdPair {
            read: -1,
            write: -1,
        };
        if let Err(e) = sys::pipe2(&mut pipe_fds, libc::O_CLOEXEC) {
            err(&format!("log pipe creation failed: {e}"));
            return;
        }

        let command_c: Vec<CString> = match args
            .command
            .iter()
            .map(|s| CString::try_from_bytes(s.as_bytes()))
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(v) => v,
            Err(e) => {
                sys::close(pipe_fds.read);
                sys::close(pipe_fds.write);
                err(&format!("invalid command (null byte): {e}"));
                return;
            }
        };

        let (clone_flags, needs_userns_maps) = match args.net_pid {
            Some(pid) => {
                let user_f = match std::fs::File::open(format!("/proc/{pid}/ns/user")) {
                    Ok(f) => f,
                    Err(e) => {
                        sys::close(pipe_fds.read);
                        sys::close(pipe_fds.write);
                        err(&format!("cannot open pid {pid} user ns: {e}"));
                        return;
                    }
                };
                let net_f = match std::fs::File::open(format!("/proc/{pid}/ns/net")) {
                    Ok(f) => f,
                    Err(e) => {
                        sys::close(pipe_fds.read);
                        sys::close(pipe_fds.write);
                        err(&format!("cannot open pid {pid} net ns: {e}"));
                        return;
                    }
                };
                if let Err(e) = sys::setns(user_f.as_raw_fd(), libc::CLONE_NEWUSER) {
                    sys::close(pipe_fds.read);
                    sys::close(pipe_fds.write);
                    err(&format!("setns(CLONE_NEWUSER) into pid {pid} failed: {e}"));
                    return;
                }
                if let Err(e) = sys::setns(net_f.as_raw_fd(), libc::CLONE_NEWNET) {
                    sys::close(pipe_fds.read);
                    sys::close(pipe_fds.write);
                    err(&format!("setns(CLONE_NEWNET) into pid {pid} failed: {e}"));
                    return;
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
                err(&format!("clone3 failed: {e}"));
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

                let output = Output {
                    fd: pipe_fds.read,
                    pid,
                    read_buf: vec![0u8; 4096],
                    line_buf: Vec::new(),
                };
                self.outputs.insert(output_id, output);

                {
                    let mut sq = self.ring.submission();
                    let output = self.outputs.get_mut(&output_id).unwrap();
                    uring::push_read(&mut sq, pipe_fds.read, &mut output.read_buf, output_id);
                }

                let maps_result = if needs_userns_maps {
                    crate::setup_userns_maps(pid)
                } else {
                    Ok(())
                };
                signal.signal();

                if let Err(e) = maps_result {
                    self.cleanup_output(output_id);
                    err(&format!("container aborted: {e}"));
                    return;
                }

                let cmd_str = args.command.join(" ");
                self.containers.insert(pid, ContainerInfo {
                    pid,
                    command: cmd_str,
                    overlay_dir,
                    save: args.save,
                    start_time: SystemTime::now(),
                    bbq: Churrasco::new(),
                });

                tracing::info!(%pid, "container started");

                self.reply(&sender, &RunResponse {
                    ok: true,
                    pid: Some(pid),
                    error: None,
                });
            }
        }
    }

    fn handle_list(&mut self, sender: libc::sockaddr_un) {
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

        self.reply(&sender, &ListResponse { containers });
    }

    fn handle_kill(&mut self, sender: libc::sockaddr_un, pid: i32) {
        if !self.containers.contains_key(&(pid as pid_t)) {
            self.reply(&sender, &KillResponse {
                ok: false,
                error: Some(format!("container {pid} not found")),
            });
            return;
        }

        let ret = unsafe { libc::kill(pid as pid_t, libc::SIGKILL) };
        if ret < 0 {
            let err = io::Error::last_os_error();
            self.reply(&sender, &KillResponse {
                ok: false,
                error: Some(format!("kill failed: {err}")),
            });
            return;
        }

        tracing::info!(%pid, "sent SIGKILL");
        self.reply(&sender, &KillResponse {
            ok: true,
            error: None,
        });
    }

    fn handle_logs(&mut self, sender: libc::sockaddr_un, pid: i32) {
        let pid = pid as pid_t;
        let _in_containers = self.containers.contains_key(&pid);
        let initial = if let Some(info) = self.containers.get_mut(&pid) {
            drain_bbq(&info.bbq)
        } else if let Some(bbq) = self.log_graveyard.get_mut(&pid) {
            drain_bbq(bbq)
        } else {
            self.reply(&sender, &ErrorResponse {
                ok: false,
                error: format!("container {pid} not found"),
            });
            return;
        };

        self.reply(&sender, &LogsResponse { lines: initial });
    }

    fn reap_children(&mut self) {
        loop {
            let mut status: i32 = 0;
            match sys::wait4(-1, &mut status, libc::WNOHANG, None) {
                Ok(pid) if pid > 0 => {
                    tracing::info!(%pid, %status, "container exited");
                    if let Some(mut info) = self.containers.remove(&pid) {
                        let bbq = std::mem::replace(&mut info.bbq, Churrasco::new());
                        self.log_graveyard.insert(pid, bbq);
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
}

// ── Helpers ───────────────────────────────────────────────────────────────

fn send_datagram_raw(
    fd: RawFd,
    addr: &libc::sockaddr_un,
    addrlen: libc::socklen_t,
    data: &[u8],
) -> io::Result<usize> {
    let ret = unsafe {
        libc::sendto(
            fd,
            data.as_ptr() as *const _,
            data.len(),
            0,
            addr as *const _ as *const libc::sockaddr,
            addrlen as _,
        )
    };
    if ret < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(ret as usize)
    }
}

fn bind_abstract(sock: &UnixDatagram, name: &[u8]) -> io::Result<()> {
    let len = size_of::<libc::sa_family_t>() + 1 + name.len().min(107);
    let mut addr: libc::sockaddr_un = unsafe { std::mem::zeroed() };
    addr.sun_family = libc::AF_UNIX as _;
    addr.sun_path[0] = 0;
    unsafe {
        std::ptr::copy_nonoverlapping(
            name.as_ptr(),
            addr.sun_path.as_mut_ptr().add(1) as *mut u8,
            name.len().min(107),
        );
    }
    unsafe {
        let r = libc::bind(
            sock.as_raw_fd(),
            &addr as *const _ as *const libc::sockaddr,
            len as _,
        );
        if r < 0 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(())
}

fn setup_sigchld_fd() -> io::Result<RawFd> {
    let mut mask: libc::sigset_t = unsafe { std::mem::zeroed() };
    unsafe { libc::sigemptyset(&mut mask) };
    unsafe { libc::sigaddset(&mut mask, libc::SIGCHLD) };
    sys::sigprocmask(libc::SIG_BLOCK, Some(&mask), None)?;
    let fd = sys::signalfd(-1, &mask, libc::SFD_CLOEXEC)?;
    Ok(fd)
}

fn drain_bbq(bbq: &Churrasco<LOG_CAPACITY>) -> Vec<String> {
    let cons = bbq.stream_consumer();
    let mut lines = Vec::new();
    while let Ok(grant) = cons.read() {
        let s = std::str::from_utf8(&grant).unwrap_or_default();
        let len = grant.len();
        for part in s.split('\n') {
            if !part.is_empty() {
                lines.push(part.to_string());
            }
        }
        grant.release(len);
    }
    lines
}

pub fn send_request(socket_path: &Path, request: &Request) -> io::Result<Vec<u8>> {
    let datagram = UnixDatagram::unbound()?;
    let abstract_name = format!("conrt-client.{}", std::process::id());
    bind_abstract(&datagram, abstract_name.as_bytes())?;
    // Retry connect with backoff for transient ECONNREFUSED.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
    loop {
        match datagram.connect(socket_path) {
            Ok(()) => break,
            Err(ref e)
                if e.raw_os_error() == Some(libc::ECONNREFUSED)
                    && std::time::Instant::now() < deadline =>
            {
                std::thread::sleep(std::time::Duration::from_millis(20));
            }
            Err(e) => return Err(e),
        }
    }

    let payload = serde_json::to_vec(request).unwrap();
    datagram.send(&payload)?;

    // Peek to learn the exact response size.
    let fd = datagram.as_raw_fd();
    let n = unsafe {
        libc::recv(
            fd,
            std::ptr::null_mut(),
            0,
            libc::MSG_PEEK | libc::MSG_TRUNC,
        )
    };
    if n < 0 {
        return Err(io::Error::last_os_error());
    }
    let mut buf = vec![0u8; n as usize];
    datagram.recv(&mut buf)?;
    Ok(buf)
}
