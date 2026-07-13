#![allow(dead_code)]

use std::collections::HashMap;
use std::io;
use std::os::fd::AsRawFd;
use std::os::fd::RawFd;
use std::os::unix::net::UnixDatagram;
use std::path::Path;
use std::path::PathBuf;
use std::time::SystemTime;

use io_uring::IoUring;
use libc::pid_t;
use serde::Deserialize;
use serde::Serialize;

use crate::cstring::CString;
use crate::interprocess;
use crate::sys;
use crate::uring;

const CACHE_CAPACITY: usize = 65536;
const LOG_CAPACITY: usize = CACHE_CAPACITY;
const RING_SIZE: u32 = 1024;

// user_data flags
const BACKLOG_WRITE: u64 = 1 << 63;
const PIPE_WRITE: u64 = 1 << 62;
const FOLLOW_FD: u64 = 1 << 61;
const ACCEPT: u64 = 1 << 60;
const STREAM_READ: u64 = 1 << 59;
const STREAM_WRITE: u64 = 1 << 58;
const PTY_READ: u64 = 1 << 57;
const PTY_WRITE: u64 = 1 << 56;
const SESSION_MASK: u64 = !(BACKLOG_WRITE
    | PIPE_WRITE
    | FOLLOW_FD
    | ACCEPT
    | STREAM_READ
    | STREAM_WRITE
    | PTY_READ
    | PTY_WRITE);
const PIPE_ID_MASK: u64 = !(BACKLOG_WRITE | PIPE_WRITE | FOLLOW_FD);

// ── Protocol ──────────────────────────────────────────────────────────────

#[derive(Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Request {
    Run {
        rootfs: Option<String>,
        net_pid: Option<i32>,
        save: bool,
        command: Vec<CStringSerde>,
        interactive: Option<bool>,
        tty: Option<bool>,
    },
    List,
    Kill {
        pid: i32,
    },
    Logs {
        pid: i32,
        #[serde(default)]
        follow: bool,
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

#[repr(transparent)]
pub struct CStringSerde(pub CString);

impl CStringSerde {
    pub fn into_inner_vec(v: Vec<CStringSerde>) -> Vec<CString> {
        unsafe { std::mem::transmute(v) }
    }

    pub fn from_inner_vec(v: Vec<CString>) -> Vec<CStringSerde> {
        unsafe { std::mem::transmute(v) }
    }
}

impl serde::Serialize for CStringSerde {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(unsafe { std::str::from_utf8_unchecked(self.0.to_bytes()) })
    }
}

impl<'de> serde::Deserialize<'de> for CStringSerde {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Vis;
        impl serde::de::Visitor<'_> for Vis {
            type Value = CStringSerde;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("a string")
            }
            fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<CStringSerde, E> {
                Ok(CStringSerde(CString::from(v)))
            }
        }
        deserializer.deserialize_str(Vis)
    }
}

struct RunArgs {
    rootfs: Option<String>,
    net_pid: Option<i32>,
    save: bool,
    command: Vec<CString>,
    tty: bool,
    interactive: bool,
}

// ── Datagram receive phases ─────────────────────────────────────────────

#[derive(PartialEq)]
enum RecvPhase {
    Peek,
    Consume,
}

// ── LogCache: single-buffer ring of \n-delimited lines ─────────────────────

struct LogCache {
    buf: Vec<u8>,
    start: usize,
    end: usize,
    bytes: usize,
}

impl LogCache {
    fn new(cap: usize) -> Self {
        Self {
            buf: vec![0u8; cap],
            start: 0,
            end: 0,
            bytes: 0,
        }
    }

    fn is_empty(&self) -> bool {
        self.bytes == 0
    }

    fn push(&mut self, line: &[u8]) {
        let need = line.len() + 1;
        loop {
            let avail = self.buf.len() - self.bytes;
            if avail >= need {
                break;
            }
            let mut i = self.start;
            loop {
                if self.buf[i] == b'\n' {
                    let line_bytes = if i >= self.start {
                        i - self.start + 1
                    } else {
                        (self.buf.len() - self.start) + i + 1
                    };
                    self.start = (self.start + line_bytes) % self.buf.len();
                    self.bytes -= line_bytes;
                    break;
                }
                i = (i + 1) % self.buf.len();
            }
        }
        for &b in line.iter().chain(std::iter::once(&b'\n')) {
            self.buf[self.end] = b;
            self.end = (self.end + 1) % self.buf.len();
        }
        self.bytes += need;
    }

    /// Copy all cached lines as `line\n` chunks into a fresh Vec.
    fn snapshot(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.bytes);
        if self.bytes == 0 {
            return out;
        }
        if self.end > self.start {
            out.extend_from_slice(&self.buf[self.start..self.end]);
        } else {
            out.extend_from_slice(&self.buf[self.start..]);
            out.extend_from_slice(&self.buf[..self.end]);
        }
        out
    }

    /// Non-destructive collect into String lines.
    fn collect_lines(&self) -> Vec<String> {
        let mut lines = Vec::new();
        if self.bytes == 0 {
            return lines;
        }
        let mut pos = self.start;
        let mut remaining = self.bytes;
        let mut i = self.start;
        loop {
            if remaining == 0 {
                break;
            }
            if self.buf[i] == b'\n' {
                let consumed = if i >= pos {
                    lines.push(String::from_utf8_lossy(&self.buf[pos..i]).into_owned());
                    i - pos + 1
                } else {
                    let mut v = Vec::with_capacity((self.buf.len() - pos) + i);
                    v.extend_from_slice(&self.buf[pos..]);
                    v.extend_from_slice(&self.buf[..i]);
                    lines.push(String::from_utf8_lossy(&v).into_owned());
                    (self.buf.len() - pos) + i + 1
                };
                remaining -= consumed;
                pos = (i + 1) % self.buf.len();
                i = pos;
                if remaining == 0 {
                    break;
                }
                continue;
            }
            i = (i + 1) % self.buf.len();
        }
        lines
    }
}

// ── AsyncPipeWriter: io_uring-backed pipe writes ──────────────────────────

struct AsyncPipeWriter {
    id: u64,
    fd: RawFd,
    send_buf: Vec<u8>,
    in_flight: bool,
}

impl AsyncPipeWriter {
    fn new(id: u64, fd: RawFd) -> Self {
        Self {
            id,
            fd,
            send_buf: Vec::with_capacity(4097),
            in_flight: false,
        }
    }

    fn push_write(
        &mut self,
        sq: &mut io_uring::squeue::SubmissionQueue,
        line: &[u8],
        user_data: u64,
    ) {
        if self.in_flight {
            return;
        }
        self.send_buf.clear();
        self.send_buf.extend_from_slice(line);
        self.send_buf.push(b'\n');
        uring::push_write(sq, self.fd, &self.send_buf, user_data);
        self.in_flight = true;
    }

    /// Returns `true` if the pipe is still alive.
    fn complete(&mut self, ret: i32) -> bool {
        self.in_flight = false;
        if ret < 0 {
            let _ = unsafe { libc::close(self.fd) };
            self.fd = -1;
            false
        } else {
            true
        }
    }
}

// ── FollowResponse: one-shot fd-pass buffer ────────────────────────────

struct FollowResponse {
    pid: pid_t,
    pipe_writer: RawFd,
    pipe_reader: RawFd,
    backlog_buf: Vec<u8>,

    // fd-pass state (set up lazily after backlog write completes)
    cmsg_buf: Vec<u8>,
    iov: libc::iovec,
    msghdr: Box<libc::msghdr>,
    datagram_fd: RawFd,
    dest: libc::sockaddr_un,
    dest_len: libc::socklen_t,
}

impl FollowResponse {
    fn new(
        pid: pid_t,
        pipe_writer: RawFd,
        pipe_reader: RawFd,
        backlog_buf: Vec<u8>,
        datagram_fd: RawFd,
        dest: libc::sockaddr_un,
        dest_len: libc::socklen_t,
    ) -> Self {
        Self {
            pid,
            pipe_writer,
            pipe_reader,
            backlog_buf,
            cmsg_buf: Vec::new(),
            iov: unsafe { std::mem::zeroed() },
            msghdr: Box::new(unsafe { std::mem::zeroed() }),
            datagram_fd,
            dest,
            dest_len,
        }
    }

    /// Submit the backlog write SQE — caller calls this first.
    fn push_backlog_write(&self, sq: &mut io_uring::squeue::SubmissionQueue, user_data: u64) {
        if !self.backlog_buf.is_empty() {
            uring::push_write(sq, self.pipe_writer, &self.backlog_buf, user_data);
        }
    }

    /// After backlog CQE, set up and submit the fd-pass SCM_RIGHTS sendmsg.
    fn push_fd_pass(&mut self, sq: &mut io_uring::squeue::SubmissionQueue, user_data: u64) {
        // Build cmsg: cmsghdr + SCM_RIGHTS with pipe_reader fd.
        let cmsg_hdr_sz = std::mem::size_of::<libc::cmsghdr>();
        let fd_size = std::mem::size_of::<RawFd>();
        let cmsg_align = std::mem::align_of::<libc::cmsghdr>();
        let cmsg_len_val = cmsg_hdr_sz + fd_size;
        let cmsg_space = (cmsg_len_val + cmsg_align - 1) & !(cmsg_align - 1);
        self.cmsg_buf = vec![0u8; cmsg_space];
        let hdr = self.cmsg_buf.as_mut_ptr() as *mut libc::cmsghdr;
        unsafe {
            (*hdr).cmsg_len = cmsg_len_val;
            (*hdr).cmsg_level = libc::SOL_SOCKET;
            (*hdr).cmsg_type = libc::SCM_RIGHTS;
            // Data starts right after the header (offset sizeof(cmsghdr)).
            let data = self.cmsg_buf.as_mut_ptr().add(cmsg_hdr_sz) as *mut RawFd;
            std::ptr::write(data, self.pipe_reader);
        }

        self.iov = libc::iovec {
            iov_base: std::ptr::null_mut(),
            iov_len: 0,
        };
        *self.msghdr = libc::msghdr {
            msg_name: &self.dest as *const _ as *mut _,
            msg_namelen: self.dest_len,
            msg_iov: &self.iov as *const _ as *mut _,
            msg_iovlen: 1,
            msg_control: self.cmsg_buf.as_mut_ptr() as *mut _,
            msg_controllen: cmsg_space,
            msg_flags: 0,
        };
        uring::push_sendmsg(sq, self.datagram_fd, &*self.msghdr, user_data);
    }
}

// ── LogGateway: cache + N async pipe writers ─────────────────────────────

struct LogGateway {
    cache: LogCache,
    pipes: Vec<AsyncPipeWriter>,
    lock_count: usize,
}

impl LogGateway {
    fn new(cap: usize) -> Self {
        Self {
            cache: LogCache::new(cap),
            pipes: Vec::new(),
            lock_count: 0,
        }
    }

    fn write(&mut self, sq: &mut io_uring::squeue::SubmissionQueue, line: &[u8]) {
        self.cache.push(line);
        for p in &mut self.pipes {
            if !p.in_flight {
                p.push_write(sq, line, PIPE_WRITE | p.id);
            }
        }
    }

    /// Returns `true` if the pipe was removed (dead).
    fn complete_write(&mut self, pipe_id: u64, ret: i32) -> bool {
        let Some(idx) = self.pipes.iter().position(|p| p.id == pipe_id) else {
            return false;
        };
        let alive = self.pipes[idx].complete(ret);
        if !alive {
            self.pipes.swap_remove(idx);
        }
        !alive
    }

    fn collect_lines(&self) -> Vec<String> {
        self.cache.collect_lines()
    }

    fn snapshot(&self) -> Vec<u8> {
        self.cache.snapshot()
    }

    fn close_all_pipes(&mut self) {
        for p in self.pipes.drain(..) {
            if p.fd >= 0 {
                let _ = unsafe { libc::close(p.fd) };
            }
        }
    }
}

// ── Container State ───────────────────────────────────────────────────────

struct ContainerInfo {
    pid: pid_t,
    command: String,
    overlay_dir: Option<PathBuf>,
    save: bool,
    start_time: SystemTime,
    gateway: LogGateway,
}

// ── Attach Session (interactive/PTY run over Unix stream) ──────────────────

struct AttachSession {
    stream_fd: RawFd,
    ptm_fd: RawFd,
    /// Read end of stdout/stderr pipe (used when no PTY).
    log_read_fd: RawFd,
    child_pid: pid_t,
    child_exited: bool,
    reading_header: bool,
    frame_buf: Vec<u8>,
    frame_type: u8,
    frame_len: u16,
    output_rbuf: Vec<u8>,
    stream_wbuf: Vec<u8>,
    pty_write_pending: bool,
    stream_write_pending: bool,
}

impl AttachSession {
    fn new(stream_fd: RawFd) -> Self {
        Self {
            stream_fd,
            ptm_fd: -1,
            log_read_fd: -1,
            child_pid: 0,
            child_exited: false,
            reading_header: true,
            frame_buf: vec![0u8; 3],
            frame_type: 0,
            frame_len: 0,
            output_rbuf: vec![0u8; 4096],
            stream_wbuf: Vec::with_capacity(4096 + 3),
            pty_write_pending: false,
            stream_write_pending: false,
        }
    }
}

pub struct Daemon {
    ring: IoUring,
    sigchld_fd: RawFd,
    sigchld_buf: Vec<u8>,
    datagram_fd: RawFd,
    attach_listener_fd: RawFd,
    socket_path: PathBuf,
    containers: HashMap<pid_t, ContainerInfo>,
    outputs: HashMap<u64, Output>,
    next_output_id: u64,
    log_graveyard: HashMap<pid_t, LogCache>,
    follow_pend: HashMap<u64, Box<FollowResponse>>,
    pipe_map: HashMap<u64, pid_t>,
    next_follow_id: u64,
    next_pipe_id: u64,
    attach_sessions: HashMap<u64, AttachSession>,
    next_session_id: u64,

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
            attach_listener_fd: -1,
            socket_path,
            containers: HashMap::new(),
            outputs: HashMap::new(),
            next_output_id: 2,
            log_graveyard: HashMap::new(),
            follow_pend: HashMap::new(),
            pipe_map: HashMap::new(),
            next_follow_id: 0,
            next_pipe_id: 0,
            attach_sessions: HashMap::new(),
            next_session_id: 0,
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
        // Self-referential pointers: init AFTER self is at its final location. ??
        self.recv_addr = unsafe { std::mem::zeroed() };
        self.recv_iov = libc::iovec {
            iov_base: self.recv_buf.as_mut_ptr() as *mut libc::c_void,
            iov_len: self.recv_buf.len(),
        };
        self.recv_msghdr = libc::msghdr {
            msg_name: &raw mut self.recv_addr as *mut _,
            msg_namelen: size_of::<libc::sockaddr_un>() as _,
            msg_iov: &raw mut self.recv_iov as *mut _,
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
        tracing::info!(path = %self.socket_path.display(), "daemon listening (datagram)");

        // Stream listener for attach/interactive sessions.
        let mut stream_path = self.socket_path.clone().into_os_string();
        stream_path.push(".stream");
        let stream_path = PathBuf::from(stream_path);
        if stream_path.exists() {
            std::fs::remove_file(&stream_path)?;
        }
        let listener = std::os::unix::net::UnixListener::bind(&stream_path)?;
        listener.set_nonblocking(true)?;
        self.attach_listener_fd = listener.as_raw_fd();
        tracing::info!(path = %stream_path.display(), "daemon listening (stream)");

        let sigchld = setup_sigchld_fd()?;
        self.sigchld_fd = sigchld;
        self.sigchld_buf = vec![0u8; std::mem::size_of::<libc::signalfd_siginfo>()];

        // Prevent listener from being dropped — socket stays bound for the
        // daemon's lifetime.
        std::mem::forget(listener);

        {
            let mut sq = self.ring.submission();
            uring::push_recvmsg(
                &mut sq,
                self.datagram_fd,
                &raw mut self.recv_msghdr,
                (libc::MSG_PEEK | libc::MSG_TRUNC) as u32,
                0,
            );
            uring::push_read(&mut sq, sigchld, &mut self.sigchld_buf, 1);
            uring::push_accept(&mut sq, self.attach_listener_fd, ACCEPT);
        }

        loop {
            self.ring.submit_and_wait(1)?;

            while let Some(cqe) = {
                let mut cq = self.ring.completion();
                cq.next()
            } {
                let user_data = cqe.user_data();
                let ret = cqe.result();
                tracing::trace!(%ret, ?user_data, "cqe completion");
                match user_data {
                    0 => self.handle_datagram_cqe(ret),
                    1 => self.handle_signal(ret),
                    id if id & FOLLOW_FD != 0 => {
                        self.complete_follow_fd_pass(id & PIPE_ID_MASK, ret)
                    }
                    id if id & BACKLOG_WRITE != 0 => {
                        self.complete_backlog_write(id & PIPE_ID_MASK, ret)
                    }
                    id if id & PIPE_WRITE != 0 => self.complete_pipe_write(id & PIPE_ID_MASK, ret),
                    id if id == (ACCEPT) => self.handle_accept(ret),
                    id if id & STREAM_READ != 0 => self.handle_stream_read(id & SESSION_MASK, ret),
                    id if id & STREAM_WRITE != 0 => {
                        self.handle_stream_write(id & SESSION_MASK, ret)
                    }
                    id if id & PTY_READ != 0 => self.handle_pty_read(id & SESSION_MASK, ret),
                    id if id & PTY_WRITE != 0 => self.handle_pty_write(id & SESSION_MASK, ret),
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
        uring::push_recvmsg(&mut sq, self.datagram_fd, &raw mut self.recv_msghdr, 0, 0);
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
            self.recv_msghdr.msg_namelen = size_of::<libc::sockaddr_un>() as _;
            // Re-link the updated iovec pointer to msghdr before giving it to the kernel
            self.recv_msghdr.msg_iov = &raw mut self.recv_iov as *mut _;
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
                    interactive,
                    tty,
                } => self.handle_run(sender, RunArgs {
                    rootfs,
                    net_pid,
                    save,
                    command: CStringSerde::into_inner_vec(command),
                    tty: tty.unwrap_or(false),
                    interactive: interactive.unwrap_or(false),
                }),
                Request::List => self.handle_list(sender),
                Request::Kill { pid } => self.handle_kill(sender, pid),
                Request::Logs { pid, follow } => {
                    if follow {
                        self.handle_follow(sender, pid);
                    } else {
                        self.handle_logs(sender, pid);
                    }
                }
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
        let pid = match self.outputs.get(&id) {
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
                    let line = &output.line_buf[start..i];
                    {
                        let mut sq = self.ring.submission();
                        if let Some(info) = self.containers.get_mut(&pid) {
                            info.gateway.write(&mut sq, line);
                        } else if let Some(cache) = self.log_graveyard.get_mut(&pid) {
                            cache.push(line);
                        }
                    }
                    start = i + 1;
                }
            }

            if start < total {
                output.line_buf.drain(..start);
            } else {
                output.line_buf.clear();
            }
        };

        let fd = match self.outputs.get(&id) {
            Some(o) => o.fd,
            None => return,
        };
        if !self
            .containers
            .get(&pid)
            .is_some_and(|c| c.gateway.lock_count > 0)
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

    /// Run a container in detached mode. All container lifecycle phases run
    /// asynchronously via the daemon's io_uring event loop — stdout/stderr
    /// are captured from a pipe, and the caller receives a `RunResponse`
    /// datagram back. The container PID is returned to the caller for later
    /// `logs`, `follow`, and `kill` operations.
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

        let save = args.save;
        let prep = match prepare_run(args) {
            Ok(p) => p,
            Err(e) => {
                err(&e);
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

        let clone_result = crate::clone3_container(prep.clone_flags);
        match clone_result {
            Err(e) => {
                sys::close(pipe_fds.read);
                sys::close(pipe_fds.write);
                err(&format!("clone3 failed: {e}"));
            }
            Ok(None) => {
                sys::close(pipe_fds.read);
                let _ = sys::dup2(pipe_fds.write, libc::STDOUT_FILENO);
                let _ = sys::dup2(pipe_fds.write, libc::STDERR_FILENO);
                if pipe_fds.write != libc::STDOUT_FILENO && pipe_fds.write != libc::STDERR_FILENO {
                    sys::close(pipe_fds.write);
                }
                let devnull = CString::from("/dev/null");
                let fd = unsafe { libc::open(devnull.as_raw(), libc::O_RDONLY) };
                if fd < 0 {
                    tracing::error!("cannot open /dev/null");
                    std::process::exit(1);
                }
                let _ = sys::dup2(fd, libc::STDIN_FILENO);
                sys::close(fd);

                if let Err(e) = prep.signal.wait() {
                    tracing::error!(%e, "sync wait failed");
                    std::process::exit(1);
                }
                child_init_environment(&prep.rootfs, &prep.overlay_dir, prep.command);
            }
            Ok(Some(pid)) => {
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

                if let Err(e) =
                    parent_setup_maps_and_signal(pid, prep.needs_userns_maps, prep.signal)
                {
                    self.cleanup_output(output_id);
                    err(&format!("container aborted: {e}"));
                    return;
                }

                let cmd_str = prep
                    .command
                    .iter()
                    .map(|c| unsafe { std::str::from_utf8_unchecked(c.to_bytes()) })
                    .collect::<Vec<_>>()
                    .join(" ");
                self.containers.insert(pid, ContainerInfo {
                    pid,
                    command: cmd_str,
                    overlay_dir: prep.overlay_dir,
                    save,
                    start_time: SystemTime::now(),
                    gateway: LogGateway::new(LOG_CAPACITY),
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
        let lines = if let Some(info) = self.containers.get_mut(&pid) {
            info.gateway.collect_lines()
        } else if let Some(cache) = self.log_graveyard.get_mut(&pid) {
            cache.collect_lines()
        } else {
            self.reply(&sender, &ErrorResponse {
                ok: false,
                error: format!("container {pid} not found"),
            });
            return;
        };

        self.reply(&sender, &LogsResponse { lines });
    }

    fn handle_follow(&mut self, sender: libc::sockaddr_un, pid: i32) {
        let pid = pid as pid_t;
        tracing::debug!(%pid, "handle_follow");

        // Lock cache and snapshot backlog (short-lived borrow on containers).
        let backlog_buf = match self.containers.get_mut(&pid) {
            Some(info) => {
                info.gateway.lock_count += 1;
                tracing::debug!(%pid, bytes = %info.gateway.cache.bytes, "follow: about to snapshot");
                let buf = info.gateway.snapshot();
                tracing::debug!(%pid, backlog_len = %buf.len(), "follow backlog snapshot");
                buf
            }
            None => {
                self.reply(&sender, &ErrorResponse {
                    ok: false,
                    error: format!("container {pid} not found"),
                });
                return;
            }
        };

        let follow_id = self.next_follow_id;
        self.next_follow_id += 1;

        // Create pipe.
        let mut pipe_fds = sys::FdPair {
            read: -1,
            write: -1,
        };
        if let Err(e) = sys::pipe2(&mut pipe_fds, libc::O_CLOEXEC) {
            tracing::error!(%e, "follow pipe creation failed");
            // Undo the lock
            if let Some(info) = self.containers.get_mut(&pid) {
                info.gateway.lock_count -= 1;
            }
            self.reply(&sender, &ErrorResponse {
                ok: false,
                error: format!("pipe creation failed: {e}"),
            });
            return;
        }

        let resp = Box::new(FollowResponse::new(
            pid,
            pipe_fds.write,
            pipe_fds.read,
            backlog_buf,
            self.datagram_fd,
            sender,
            self.recv_addr_len,
        ));

        // Submit backlog write.
        {
            let mut sq = self.ring.submission();
            resp.push_backlog_write(&mut sq, BACKLOG_WRITE | follow_id);
        }

        let is_backlog_empty = resp.backlog_buf.is_empty();
        self.follow_pend.insert(follow_id, resp);

        if is_backlog_empty {
            self.complete_backlog_write(follow_id, 0);
        }
    }

    fn complete_backlog_write(&mut self, follow_id: u64, ret: i32) {
        tracing::debug!(%follow_id, %ret, "complete_backlog_write");
        let mut resp = match self.follow_pend.remove(&follow_id) {
            Some(r) => r,
            None => return,
        };
        let pid = resp.pid;

        if ret < 0 {
            tracing::error!(%ret, "backlog write to pipe failed");
            let _ = unsafe { libc::close(resp.pipe_writer) };
            let _ = unsafe { libc::close(resp.pipe_reader) };
            if let Some(info) = self.containers.get_mut(&pid) {
                info.gateway.lock_count -= 1;
            }
            return;
        }

        let pipe_id = self.next_pipe_id;
        self.next_pipe_id += 1;

        // Backlog written. Attach pipe writer to the container's gateway.
        if let Some(info) = self.containers.get_mut(&pid) {
            info.gateway
                .pipes
                .push(AsyncPipeWriter::new(pipe_id, resp.pipe_writer));
            self.pipe_map.insert(pipe_id, pid);
            info.gateway.lock_count -= 1;
        }
        // Resubmit the output read for this pid (it was held during lock).
        let output_id = self
            .outputs
            .iter()
            .find_map(|(&id, o)| (o.pid == pid).then_some(id));
        if let Some(id) = output_id {
            let mut sq = self.ring.submission();
            let output = self.outputs.get_mut(&id).unwrap();
            uring::push_read(&mut sq, output.fd, &mut output.read_buf, id);
        }

        // Set up fd-pass buffer and submit SCM_RIGHTS.
        {
            let mut sq = self.ring.submission();
            resp.push_fd_pass(&mut sq, FOLLOW_FD | follow_id);
        }

        // Keep resp alive — fd-pass is in-flight.
        self.follow_pend.insert(follow_id, resp);
    }

    fn complete_follow_fd_pass(&mut self, follow_id: u64, ret: i32) {
        tracing::debug!(%follow_id, %ret, "complete_follow_fd_pass");
        if let Some(resp) = self.follow_pend.remove(&follow_id) {
            tracing::debug!(pid = %resp.pid, "follow fd-pass done, closing reader");
            let _ = unsafe { libc::close(resp.pipe_reader) };
        }
    }

    fn complete_pipe_write(&mut self, pipe_id: u64, ret: i32) {
        let Some(&pid) = self.pipe_map.get(&pipe_id) else {
            return;
        };
        let Some(info) = self.containers.get_mut(&pid) else {
            self.pipe_map.remove(&pipe_id);
            return;
        };
        if info.gateway.complete_write(pipe_id, ret) {
            self.pipe_map.remove(&pipe_id);
        }
    }

    // ── Stream attach session handlers ────────────────────────────────────

    fn handle_accept(&mut self, ret: i32) {
        if ret < 0 {
            // Accept failed (e.g. EAGAIN with non-blocking). Resubmit.
            tracing::warn!(%ret, "accept failed");
            let mut sq = self.ring.submission();
            uring::push_accept(&mut sq, self.attach_listener_fd, ACCEPT);
            return;
        }
        let stream_fd = ret;
        let session_id = self.next_session_id;
        self.next_session_id += 1;
        tracing::debug!(%session_id, %stream_fd, "attach session accepted");
        self.attach_sessions
            .insert(session_id, AttachSession::new(stream_fd));
        // Read first header.
        self.submit_stream_read_header(session_id);
        // Re-submit accept.
        let mut sq = self.ring.submission();
        uring::push_accept(&mut sq, self.attach_listener_fd, ACCEPT);
    }

    fn submit_stream_read_header(&mut self, session_id: u64) {
        let session = self.attach_sessions.get_mut(&session_id).unwrap();
        session.reading_header = true;
        session.frame_buf.resize(3, 0);
        let mut sq = self.ring.submission();
        uring::push_read(
            &mut sq,
            session.stream_fd,
            &mut session.frame_buf,
            STREAM_READ | session_id,
        );
    }

    fn handle_stream_read(&mut self, session_id: u64, ret: i32) {
        let Some(session) = self.attach_sessions.get_mut(&session_id) else {
            return;
        };

        if ret <= 0 {
            if ret < 0 {
                tracing::error!(%session_id, %ret, "stream read error, closing session");
            } else {
                tracing::info!(%session_id, "stream read EOF, closing session");
            }
            self.close_attach_session(session_id);
            return;
        }

        if session.reading_header {
            // Read 3-byte header.
            session.frame_type = session.frame_buf[0];
            session.frame_len = u16::from_le_bytes([session.frame_buf[1], session.frame_buf[2]]);
            let len = session.frame_len as usize;
            if len == 0 {
                // No payload: dispatch immediately.
                let frame_type = session.frame_type;
                self.dispatch_frame(session_id, frame_type, &[]);
            } else {
                session.reading_header = false;
                session.frame_buf.resize(len, 0);
                let mut sq = self.ring.submission();
                let s = self.attach_sessions.get_mut(&session_id).unwrap();
                uring::push_read(
                    &mut sq,
                    s.stream_fd,
                    &mut s.frame_buf,
                    STREAM_READ | session_id,
                );
            }
        } else {
            // Payload complete.
            let frame_type = session.frame_type;
            let data = session.frame_buf[..session.frame_len as usize].to_vec();
            self.dispatch_frame(session_id, frame_type, &data);
        }
    }

    fn dispatch_frame(&mut self, session_id: u64, frame_type: u8, data: &[u8]) {
        match frame_type {
            0x00 => {
                // RunRequest: parse JSON Request::Run
                let request: Request = match serde_json::from_slice(data) {
                    Ok(r) => r,
                    Err(e) => {
                        tracing::error!(%session_id, %e, "invalid RunRequest JSON");
                        self.close_attach_session(session_id);
                        return;
                    }
                };
                let (rootfs, net_pid, save, command, interactive, tty) = match request {
                    Request::Run {
                        rootfs,
                        net_pid,
                        save,
                        command,
                        interactive,
                        tty,
                    } => (rootfs, net_pid, save, command, interactive, tty),
                    _ => {
                        tracing::error!(%session_id, "expected RunRequest inside frame");
                        self.close_attach_session(session_id);
                        return;
                    }
                };
                self.handle_run_attach(session_id, RunArgs {
                    rootfs,
                    net_pid,
                    save,
                    command: CStringSerde::into_inner_vec(command),
                    tty: tty.unwrap_or(false),
                    interactive: interactive.unwrap_or(false),
                });
            }
            0x10 => {
                // Data from stdin → PTY
                if !data.is_empty() {
                    let mut sq = self.ring.submission();
                    if let Some(session) = self.attach_sessions.get_mut(&session_id) {
                        session.pty_write_pending = true;
                        uring::push_write(&mut sq, session.ptm_fd, data, PTY_WRITE | session_id);
                    }
                }
                // Read next frame header.
                self.submit_stream_read_header(session_id);
            }
            0x11 => {
                // StdinEof: close write side of PTY (send EOF).
                if let Some(session) = self.attach_sessions.get_mut(&session_id)
                    && session.ptm_fd >= 0
                {
                    sys::close(session.ptm_fd);
                    session.ptm_fd = -1;
                }
                self.submit_stream_read_header(session_id);
            }
            0x20 => {
                // WinSize: parse JSON { rows: u16, cols: u16 }
                #[derive(serde::Deserialize)]
                struct WinSize {
                    rows: u16,
                    cols: u16,
                }
                let ws: WinSize = match serde_json::from_slice(data) {
                    Ok(w) => w,
                    Err(e) => {
                        tracing::error!(%session_id, %e, "invalid WinSize JSON");
                        self.submit_stream_read_header(session_id);
                        return;
                    }
                };
                if let Some(session) = self.attach_sessions.get_mut(&session_id)
                    && session.ptm_fd >= 0
                {
                    let mut w: libc::winsize = unsafe { std::mem::zeroed() };
                    w.ws_row = ws.rows;
                    w.ws_col = ws.cols;
                    let _ = unsafe { libc::ioctl(session.ptm_fd, libc::TIOCSWINSZ, &w) };
                }
                self.submit_stream_read_header(session_id);
            }
            _ => {
                tracing::warn!(%session_id, %frame_type, "unknown frame type, closing");
                self.close_attach_session(session_id);
            }
        }
    }

    /// Run a container with an interactive stream session. If `tty` or
    /// `interactive` is set, a PTY is allocated and child I/O flows through
    /// it; otherwise stdout/stderr go through a pipe (same as detached) and
    /// stdin is wired to `/dev/null`. The result is sent as a framed
    /// `RunResponse` on the Unix stream, and the session stays open for
    /// bidirectional data/EOF/WinSize frames until the child exits.
    fn handle_run_attach(&mut self, session_id: u64, args: RunArgs) {
        let Some(session) = self.attach_sessions.get(&session_id) else {
            return;
        };
        let stream_fd = session.stream_fd;

        let mut err = |msg: &str| {
            let payload = serde_json::to_vec(&ErrorResponse {
                ok: false,
                error: msg.to_string(),
            })
            .unwrap();
            let mut frame = Vec::with_capacity(3 + payload.len());
            frame.push(0x01);
            frame.extend_from_slice(&(payload.len() as u16).to_le_bytes());
            frame.extend_from_slice(&payload);
            let _ = sys::write(stream_fd, &frame);
            self.close_attach_session(session_id);
        };

        let use_pty = args.tty || args.interactive;
        let save = args.save;
        let prep = match prepare_run(args) {
            Ok(p) => p,
            Err(e) => {
                err(&e);
                return;
            }
        };

        let (master, mut slave) = if use_pty {
            match crate::pty::open_pty() {
                Ok((m, s)) => (Some(m), Some(s)),
                Err(e) => {
                    err(&format!("pty allocation failed: {e}"));
                    return;
                }
            }
        } else {
            (None, None)
        };

        let mut pipe_fds = sys::FdPair {
            read: -1,
            write: -1,
        };
        if !use_pty && let Err(e) = sys::pipe2(&mut pipe_fds, libc::O_CLOEXEC) {
            err(&format!("log pipe creation failed: {e}"));
            return;
        }

        let clone_result = crate::clone3_container(prep.clone_flags);
        match clone_result {
            Err(e) => {
                if pipe_fds.read >= 0 {
                    sys::close(pipe_fds.read);
                }
                if pipe_fds.write >= 0 {
                    sys::close(pipe_fds.write);
                }
                err(&format!("clone3 failed: {e}"));
            }
            Ok(None) => {
                drop(master);
                if use_pty {
                    if let Some(slave) = slave.take()
                        && let Err(e) = slave.make_controlling()
                    {
                        tracing::error!(%e, "pty setup failed in child");
                        std::process::exit(1);
                    }
                } else {
                    sys::close(pipe_fds.read);
                    let _ = sys::dup2(pipe_fds.write, libc::STDOUT_FILENO);
                    let _ = sys::dup2(pipe_fds.write, libc::STDERR_FILENO);
                    if pipe_fds.write != libc::STDOUT_FILENO
                        && pipe_fds.write != libc::STDERR_FILENO
                    {
                        sys::close(pipe_fds.write);
                    }
                    let devnull = CString::from("/dev/null");
                    let fd = unsafe { libc::open(devnull.as_raw(), libc::O_RDONLY) };
                    if fd < 0 {
                        tracing::error!("cannot open /dev/null");
                        std::process::exit(1);
                    }
                    let _ = sys::dup2(fd, libc::STDIN_FILENO);
                    sys::close(fd);
                }

                if let Err(e) = prep.signal.wait() {
                    tracing::error!(%e, "sync wait failed");
                    std::process::exit(1);
                }
                child_init_environment(&prep.rootfs, &prep.overlay_dir, prep.command);
            }
            Ok(Some(pid)) => {
                drop(slave);

                if let Err(e) =
                    parent_setup_maps_and_signal(pid, prep.needs_userns_maps, prep.signal)
                {
                    err(&format!("container aborted: {e}"));
                    return;
                }

                let ptm_fd = master.map_or(-1, |m| {
                    let fd = m.raw_fd();
                    std::mem::forget(m);
                    fd
                });

                if pipe_fds.write >= 0 {
                    sys::close(pipe_fds.write);
                }

                let cmd_str = prep
                    .command
                    .iter()
                    .map(|c| unsafe { std::str::from_utf8_unchecked(c.to_bytes()) })
                    .collect::<Vec<_>>()
                    .join(" ");

                if let Some(session) = self.attach_sessions.get_mut(&session_id) {
                    session.child_pid = pid;
                    session.ptm_fd = ptm_fd;
                    session.log_read_fd = pipe_fds.read;
                }

                self.containers.insert(pid, ContainerInfo {
                    pid,
                    command: cmd_str,
                    overlay_dir: prep.overlay_dir,
                    save,
                    start_time: SystemTime::now(),
                    gateway: LogGateway::new(LOG_CAPACITY),
                });

                tracing::info!(%pid, %session_id, "attach container started");

                let payload = serde_json::to_vec(&RunResponse {
                    ok: true,
                    pid: Some(pid),
                    error: None,
                })
                .unwrap();
                self.send_attach_frame(session_id, 0x01, &payload);

                self.submit_stream_read_header(session_id);
            }
        }
    }

    fn send_attach_frame(&mut self, session_id: u64, frame_type: u8, payload: &[u8]) {
        let Some(session) = self.attach_sessions.get_mut(&session_id) else {
            return;
        };
        let stream_fd = session.stream_fd;
        session.stream_wbuf.clear();
        session.stream_wbuf.push(frame_type);
        session
            .stream_wbuf
            .extend_from_slice(&(payload.len() as u16).to_le_bytes());
        session.stream_wbuf.extend_from_slice(payload);
        session.stream_write_pending = true;
        let mut sq = self.ring.submission();
        uring::push_write(
            &mut sq,
            stream_fd,
            &session.stream_wbuf,
            STREAM_WRITE | session_id,
        );
    }

    fn submit_output_read(&mut self, session_id: u64) {
        let Some(session) = self.attach_sessions.get_mut(&session_id) else {
            return;
        };
        let fd = if session.ptm_fd >= 0 {
            session.ptm_fd
        } else if session.log_read_fd >= 0 {
            session.log_read_fd
        } else {
            return;
        };
        let mut sq = self.ring.submission();
        uring::push_read(&mut sq, fd, &mut session.output_rbuf, PTY_READ | session_id);
    }

    fn handle_pty_read(&mut self, session_id: u64, ret: i32) {
        // Fast path: successful read → send data frame (no close_attach_session
        // needed).
        if ret > 0 {
            let Some(session) = self.attach_sessions.get_mut(&session_id) else {
                return;
            };
            let n = ret as usize;
            session.stream_wbuf.clear();
            session.stream_wbuf.push(0x10); // Data frame type
            session
                .stream_wbuf
                .extend_from_slice(&(n as u16).to_le_bytes());
            session
                .stream_wbuf
                .extend_from_slice(&session.output_rbuf[..n]);
            session.stream_write_pending = true;
            let fd = session.stream_fd;
            let mut sq = self.ring.submission();
            uring::push_write(&mut sq, fd, &session.stream_wbuf, STREAM_WRITE | session_id);
            // PTY_READ is NOT resubmitted here — it waits for STREAM_WRITE
            // completion to provide backpressure.
            return;
        }
        // EOF/error path: close output fds, then maybe close session.
        let child_exited;
        {
            let Some(session) = self.attach_sessions.get_mut(&session_id) else {
                return;
            };
            if ret < 0 {
                tracing::error!(%session_id, %ret, "PTY/pipe read error");
            } else {
                tracing::info!(%session_id, "PTY/pipe read EOF");
            }
            if session.ptm_fd >= 0 {
                sys::close(session.ptm_fd);
                session.ptm_fd = -1;
            }
            if session.log_read_fd >= 0 {
                sys::close(session.log_read_fd);
                session.log_read_fd = -1;
            }
            child_exited = session.child_exited;
        }
        if child_exited {
            self.close_attach_session(session_id);
        }
    }

    fn handle_stream_write(&mut self, session_id: u64, ret: i32) {
        let (child_exited, has_output, no_output_fds, no_pty_write) = {
            let Some(session) = self.attach_sessions.get_mut(&session_id) else {
                return;
            };
            session.stream_write_pending = false;
            if ret < 0 {
                tracing::warn!(%session_id, %ret, "stream write error, closing");
                let _ = session;
                self.close_attach_session(session_id);
                return;
            }
            // If there's a deferred exit-code frame, send it now.
            if session.child_exited
                && !session.stream_wbuf.is_empty()
                && session.stream_wbuf[0] == 0x02
            {
                let fd = session.stream_fd;
                let buf = session.stream_wbuf.clone();
                session.stream_wbuf.clear();
                session.stream_write_pending = true;
                let _ = session;
                let mut sq = self.ring.submission();
                uring::push_write(&mut sq, fd, &buf, STREAM_WRITE | session_id);
                return;
            }
            (
                session.child_exited,
                session.ptm_fd >= 0 || session.log_read_fd >= 0,
                session.ptm_fd < 0 && session.log_read_fd < 0,
                !session.pty_write_pending,
            )
        };
        // Resubmit output read for more data (ping-pong flow control).
        if has_output {
            self.submit_output_read(session_id);
        }
        // If child exited and no more output reads pending, close.
        if child_exited && no_output_fds && no_pty_write {
            self.close_attach_session(session_id);
        }
    }

    fn handle_pty_write(&mut self, session_id: u64, ret: i32) {
        let Some(session) = self.attach_sessions.get_mut(&session_id) else {
            return;
        };
        session.pty_write_pending = false;
        if ret < 0 {
            tracing::warn!(%session_id, %ret, "PTY write error, closing PTY");
            if session.ptm_fd >= 0 {
                sys::close(session.ptm_fd);
                session.ptm_fd = -1;
            }
            // Child might still be writing output, so PTY_READ stays alive.
        }
    }

    fn close_attach_session(&mut self, session_id: u64) {
        if let Some(session) = self.attach_sessions.remove(&session_id) {
            tracing::info!(%session_id, "closing attach session");
            if session.stream_fd >= 0 {
                sys::close(session.stream_fd);
            }
            if session.ptm_fd >= 0 {
                sys::close(session.ptm_fd);
            }
            if session.log_read_fd >= 0 {
                sys::close(session.log_read_fd);
            }
        }
    }

    fn reap_children(&mut self) {
        loop {
            let mut status: i32 = 0;
            match sys::wait4(-1, &mut status, libc::WNOHANG, None) {
                Ok(pid) if pid > 0 => {
                    tracing::info!(%pid, %status, "container exited");

                    // Check if this pid belongs to an attach session.
                    let session_id = self
                        .attach_sessions
                        .iter()
                        .find_map(|(&id, s)| (s.child_pid == pid).then_some(id));

                    if let Some(sid) = session_id {
                        // Attached container.
                        if let Some(session) = self.attach_sessions.get_mut(&sid) {
                            session.child_exited = true;
                            // Do NOT close output fds here — let the PTY_READ
                            // handler drain remaining data and close on EOF.
                            let exit_code = if libc::WIFEXITED(status) {
                                libc::WEXITSTATUS(status)
                            } else if libc::WIFSIGNALED(status) {
                                128 + libc::WTERMSIG(status)
                            } else {
                                1
                            };
                            let payload =
                                serde_json::to_vec(&serde_json::json!({ "exit_code": exit_code }))
                                    .unwrap();
                            // Send ExitCode frame if no pending stream write.
                            if !session.stream_write_pending {
                                self.send_attach_frame(sid, 0x02, &payload);
                            } else {
                                // Stream write in-flight; defer. We store it.
                                session.stream_wbuf.clear();
                                session.stream_wbuf.push(0x02);
                                session
                                    .stream_wbuf
                                    .extend_from_slice(&(payload.len() as u16).to_le_bytes());
                                session.stream_wbuf.extend_from_slice(&payload);
                                // Mark stream_write_pending so we send it after
                                // current write.
                            }
                        }
                        // Remove from container tracking but don't move to graveyard
                        // (attach sessions don't use log follow).
                        if let Some(mut info) = self.containers.remove(&pid) {
                            let cache = std::mem::replace(
                                &mut info.gateway.cache,
                                LogCache::new(LOG_CAPACITY),
                            );
                            self.log_graveyard.insert(pid, cache);
                            info.gateway.close_all_pipes();
                            if let Some(ref overlay) = info.overlay_dir
                                && !info.save
                            {
                                crate::cleanup_overlay(overlay);
                            }
                        }
                    } else {
                        // Detached container — existing logic.
                        if let Some(mut info) = self.containers.remove(&pid) {
                            let cache = std::mem::replace(
                                &mut info.gateway.cache,
                                LogCache::new(LOG_CAPACITY),
                            );
                            self.log_graveyard.insert(pid, cache);
                            self.pipe_map.retain(|_id, &mut p| p != pid);
                            info.gateway.close_all_pipes();
                            if let Some(ref overlay) = info.overlay_dir
                                && !info.save
                            {
                                crate::cleanup_overlay(overlay);
                            }
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

fn setup_sigchld_fd() -> io::Result<RawFd> {
    let mut mask: libc::sigset_t = unsafe { std::mem::zeroed() };
    unsafe { libc::sigemptyset(&mut mask) };
    unsafe { libc::sigaddset(&mut mask, libc::SIGCHLD) };
    sys::sigprocmask(libc::SIG_BLOCK, Some(&mask), None)?;
    let fd = sys::signalfd(-1, &mask, libc::SFD_CLOEXEC)?;
    Ok(fd)
}

pub fn send_request(socket_path: &Path, request: &Request) -> io::Result<Vec<u8>> {
    let datagram = UnixDatagram::unbound()?;
    let mut addr: libc::sockaddr_un = unsafe { std::mem::zeroed() };
    addr.sun_family = libc::AF_UNIX as _;
    unsafe {
        let r = libc::bind(
            datagram.as_raw_fd(),
            &addr as *const _ as *const libc::sockaddr,
            size_of::<libc::sa_family_t>() as _,
        );
        if r < 0 {
            return Err(io::Error::last_os_error());
        }
    }
    datagram.connect(socket_path)?;

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

struct PreparedResources {
    rootfs: Option<PathBuf>,
    overlay_dir: Option<PathBuf>,
    signal: interprocess::OneshotSignal,
    clone_flags: libc::c_int,
    needs_userns_maps: bool,
    command: Vec<CString>,
}

/// Resolve all container resources needed before `clone3`: canonicalize
/// rootfs, create overlay tempdir, allocate the interprocess OneshotSignal,
/// join the target netns if `net_pid` is set, and compute `clone_flags`.
/// Cleanup of acquired resources (overlay dir, signal) is the caller's
/// responsibility on error.
fn prepare_run(args: RunArgs) -> Result<PreparedResources, String> {
    let rootfs = match args.rootfs {
        Some(p) => match Path::new(&p).canonicalize() {
            Ok(path) => Some(path),
            Err(e) => return Err(format!("invalid rootfs path: {e}")),
        },
        None => None,
    };

    let overlay_dir = match rootfs {
        Some(_) => match crate::create_overlay_tempdir() {
            Ok(dir) => Some(dir),
            Err(e) => return Err(format!("cannot create overlay tempdir: {e}")),
        },
        None => None,
    };

    let signal = match interprocess::OneshotSignal::new() {
        Ok(s) => s,
        Err(e) => return Err(format!("sync pipe creation failed: {e}")),
    };

    let (clone_flags, needs_userns_maps) = match args.net_pid {
        Some(pid) => {
            let user_f = std::fs::File::open(format!("/proc/{pid}/ns/user"))
                .map_err(|e| format!("cannot open pid {pid} user ns: {e}"))?;
            let net_f = std::fs::File::open(format!("/proc/{pid}/ns/net"))
                .map_err(|e| format!("cannot open pid {pid} net ns: {e}"))?;
            sys::setns(user_f.as_raw_fd(), libc::CLONE_NEWUSER)
                .map_err(|e| format!("setns(CLONE_NEWUSER) into pid {pid} failed: {e}"))?;
            sys::setns(net_f.as_raw_fd(), libc::CLONE_NEWNET)
                .map_err(|e| format!("setns(CLONE_NEWNET) into pid {pid} failed: {e}"))?;
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

    Ok(PreparedResources {
        rootfs,
        overlay_dir,
        signal,
        clone_flags,
        needs_userns_maps,
        command: args.command,
    })
}

/// Write uid/gid maps into the child's user namespace (if needed), then
/// signal the child to proceed. The child is stopped after `clone3` and
/// blocked on `signal.wait()` — this function unblocks it only after maps
/// are confirmed written, preventing the child from running with
/// unconfigured namespaces.
fn parent_setup_maps_and_signal(
    pid: pid_t,
    needs_userns_maps: bool,
    signal: interprocess::OneshotSignal,
) -> Result<(), String> {
    if needs_userns_maps {
        crate::setup_userns_maps(pid).map_err(|e| format!("uid_map write failed: {e}"))?;
    }
    signal.signal();
    Ok(())
}

/// Called in the child process after `clone3`. Brings up loopback, sets
/// hostname, mounts overlay rootfs and pivot_root, then `execvp` into
/// the workload. Never returns — either execs successfully or calls
/// `process::exit(1)`.
fn child_init_environment(
    rootfs: &Option<PathBuf>,
    overlay_dir: &Option<PathBuf>,
    command: Vec<CString>,
) -> ! {
    if let Err(e) = sys::bring_up_lo() {
        tracing::warn!(%e, "bring_up_lo failed");
    }

    if let Err(e) = sys::sethostname("conrt") {
        tracing::error!(%e, "sethostname failed");
    }

    if let Some(rootfs_path) = rootfs {
        let overlay = overlay_dir
            .as_ref()
            .expect("overlay_dir is always created when rootfs is provided");

        let container_root = match crate::setup_overlay_rootfs(rootfs_path, overlay) {
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

    let argv = sys::Argv::new(command);
    let errno = crate::execvp(argv.as_slice());
    tracing::error!(%errno, "execvp failed");
    std::process::exit(1)
}

// ── Tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn log_cache_starts_unlocked() {
        let c = LogCache::new(64);
        assert!(c.is_empty());
    }

    #[test]
    fn log_cache_push_and_snapshot() {
        let mut c = LogCache::new(4096);
        c.push(b"hello");
        c.push(b"world");
        let snap = c.snapshot();
        assert_eq!(snap, b"hello\nworld\n");
    }

    #[test]
    fn log_cache_collect_lines_non_destructive() {
        let mut c = LogCache::new(4096);
        c.push(b"foo");
        c.push(b"bar");
        let lines = c.collect_lines();
        assert_eq!(lines, &["foo", "bar"]);
        // Snapshot after collect_lines should still have everything.
        let snap = c.snapshot();
        assert_eq!(snap, b"foo\nbar\n");
    }

    #[test]
    fn log_cache_empty() {
        let c = LogCache::new(64);
        assert!(c.snapshot().is_empty());
        assert!(c.collect_lines().is_empty());
    }

    #[test]
    fn log_cache_evicts_oldest_when_full() {
        let mut c = LogCache::new(16);
        // 4 pushes of 5 bytes each = 20 bytes total → 1st evicted.
        c.push(b"aaaa"); // 5 bytes
        c.push(b"bbbb"); // 5 bytes, total 10
        c.push(b"cccc"); // 5 bytes, total 15 (1 byte free)
        c.push(b"dddd"); // 5 bytes, need 5, avail 1 → evict "aaaa\n" → write "dddd\n"
        let raw = c.snapshot();
        let snap = String::from_utf8_lossy(&raw);
        assert!(
            !snap.contains("aaaa"),
            "oldest line should be evicted: {snap:?}"
        );
        assert!(snap.contains("bbbb"), "bbbb should survive: {snap:?}");
        assert!(snap.contains("cccc"), "cccc should survive: {snap:?}");
        assert!(snap.contains("dddd"), "dddd should survive: {snap:?}");
    }

    #[test]
    fn log_cache_wraparound() {
        let mut c = LogCache::new(16);
        // Fill buffer with wraparound: evict older lines to make room.
        c.push(b"aaaa"); // bytes=5, end=5
        c.push(b"bbbb"); // bytes=10, end=10
        c.push(b"cccc"); // bytes=15, end=15, avail=1
        // This push evicts "aaaa\n" → bytes=10, start=5, then writes "dddd\n" wrapping
        // end to 4.
        c.push(b"dddd");
        let lines = c.collect_lines();
        assert_eq!(lines, &["bbbb", "cccc", "dddd"], "got {lines:?}");
        // One more push evicts "bbbb\n"
        c.push(b"eeee");
        let lines = c.collect_lines();
        assert_eq!(lines, &["cccc", "dddd", "eeee"], "got {lines:?}");
    }

    // ── LogGateway unit tests ─────────────────────────────────────────────

    fn make_pipe_pair() -> (RawFd, RawFd) {
        let mut fds = sys::FdPair {
            read: -1,
            write: -1,
        };
        sys::pipe2(&mut fds, libc::O_CLOEXEC).unwrap();
        (fds.read, fds.write)
    }

    #[test]
    fn log_gateway_starts_empty() {
        let g = LogGateway::new(64);
        assert!(g.pipes.is_empty());
        assert_eq!(g.lock_count, 0);
    }

    #[test]
    fn log_gateway_attach_multiple_pipes() {
        let mut g = LogGateway::new(64);
        let (r1, w1) = make_pipe_pair();
        let (r2, w2) = make_pipe_pair();
        g.pipes.push(AsyncPipeWriter::new(1, w1));
        g.pipes.push(AsyncPipeWriter::new(2, w2));
        assert_eq!(g.pipes.len(), 2);
        let _ = unsafe { libc::close(r1) };
        let _ = unsafe { libc::close(r2) };
    }

    #[test]
    fn log_gateway_complete_write_alive_pipe() {
        let (r, w) = make_pipe_pair();
        let mut g = LogGateway::new(64);
        g.pipes.push(AsyncPipeWriter::new(1, w));
        assert!(!g.complete_write(1, 5)); // 5 bytes written = success
        assert_eq!(g.pipes.len(), 1); // still alive
        assert!(!g.pipes[0].in_flight);
        let _ = unsafe { libc::close(r) };
    }

    #[test]
    fn log_gateway_complete_write_removes_dead_pipe() {
        let (r, w) = make_pipe_pair();
        // Close the read end so writing will fail, but for this test
        // we simulate error by passing ret < 0.
        let _ = unsafe { libc::close(r) };
        let mut g = LogGateway::new(64);
        g.pipes.push(AsyncPipeWriter::new(1, w));
        // Simulate write error
        assert!(g.complete_write(1, -1)); // true = removed
        assert!(g.pipes.is_empty());
        // w fd was closed by complete()
    }

    #[test]
    fn log_gateway_complete_write_unknown_id_does_nothing() {
        let mut g = LogGateway::new(64);
        let (r, w) = make_pipe_pair();
        g.pipes.push(AsyncPipeWriter::new(1, w));
        assert!(!g.complete_write(999, 0)); // not found
        assert_eq!(g.pipes.len(), 1);
        let _ = unsafe { libc::close(r) };
    }

    #[test]
    fn log_gateway_close_all_pipes() {
        let mut g = LogGateway::new(64);
        let (r1, w1) = make_pipe_pair();
        let (r2, w2) = make_pipe_pair();
        g.pipes.push(AsyncPipeWriter::new(1, w1));
        g.pipes.push(AsyncPipeWriter::new(2, w2));
        g.close_all_pipes();
        assert!(g.pipes.is_empty());
        let _ = unsafe { libc::close(r1) };
        let _ = unsafe { libc::close(r2) };
    }

    #[test]
    fn log_gateway_lock_count_blocks_check() {
        let mut g = LogGateway::new(64);
        assert_eq!(g.lock_count, 0);
        g.lock_count += 1;
        assert!(g.lock_count > 0);
        g.lock_count -= 1;
        assert_eq!(g.lock_count, 0);
    }

    #[test]
    fn log_gateway_write_goes_to_all_pipes() {
        use io_uring::IoUring;
        let (r1, w1) = make_pipe_pair();
        let (r2, w2) = make_pipe_pair();
        let mut g = LogGateway::new(4096);

        let mut ring = IoUring::new(8).unwrap();
        let mut sq = ring.submission();

        g.pipes.push(AsyncPipeWriter::new(1, w1));
        g.pipes.push(AsyncPipeWriter::new(2, w2));
        g.write(&mut sq, b"hello");
        assert_eq!(g.cache.snapshot(), b"hello\n");
        // Both pipes should have an SQE in-flight
        assert!(g.pipes[0].in_flight);
        assert!(g.pipes[1].in_flight);
        let _ = unsafe { libc::close(r1) };
        let _ = unsafe { libc::close(r2) };
    }

    #[test]
    fn log_gateway_write_skips_in_flight_pipe() {
        use io_uring::IoUring;
        let (r1, w1) = make_pipe_pair();
        let (r2, w2) = make_pipe_pair();
        let mut g = LogGateway::new(4096);

        let mut ring = IoUring::new(8).unwrap();
        let mut sq = ring.submission();

        g.pipes.push(AsyncPipeWriter::new(1, w1));
        g.pipes.push(AsyncPipeWriter::new(2, w2));
        // Mark pipe 1 as in_flight
        g.pipes[0].in_flight = true;
        g.write(&mut sq, b"world");
        assert_eq!(g.cache.snapshot(), b"world\n");
        // Pipe 1 stays in_flight and didn't get a new SQE (its state unchanged)
        assert!(g.pipes[0].in_flight);
        // Pipe 2 got the write
        assert!(g.pipes[1].in_flight);
        let _ = unsafe { libc::close(r1) };
        let _ = unsafe { libc::close(r2) };
    }
}
