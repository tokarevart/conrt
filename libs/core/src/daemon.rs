use std::collections::HashMap;
use std::collections::VecDeque;
use std::io;
use std::os::fd::AsRawFd;
use std::os::fd::RawFd;
use std::os::unix::net::UnixDatagram;
use std::path::Path;
use std::path::PathBuf;
use std::time::SystemTime;

use coio::Bytes;
use coio::BytesMut;
use coio::Msg;
use coio::MsgMut;
use coio::accept;
use coio::alloc_bytes;
use coio::read;
use coio::recvmsg;
use coio::runtime::Runtime;
use coio::runtime::block_on_default;
use coio::sendmsg;
use coio::write;
use conrt_cstring::CString;
use conrt_sys as sys;
use libc::pid_t;
use serde::Deserialize;
use serde::Serialize;

use crate::cleanup_overlay;
use crate::clone3_container;
use crate::create_overlay_tempdir;
use crate::execvp;
use crate::interprocess;
use crate::pty;
use crate::setup_container_root;
use crate::setup_overlay_rootfs;
use crate::setup_userns_maps;

const CACHE_CAPACITY: usize = 65536;
const LOG_CAPACITY: usize = CACHE_CAPACITY;

/// The largest payload a single fixed-pool write buffer can carry: one byte
/// short of the biggest size class, so a trailing `\n` always fits.
const WRITE_CHUNK_PAYLOAD: usize = 65535;

/// Allocates a fixed-pool buffer of `size` bytes, panicking on failure.
///
/// Pool allocation is expected to succeed unless the allocator itself fails:
/// an exhausted size class indicates a sizing bug. The pools will grow on
/// demand in coio in the future; until then, degrading gracefully on
/// exhaustion (dropping log lines, closing sessions) is worse than aborting.
fn pool_alloc(size: usize) -> BytesMut {
    alloc_bytes(size).expect("fixed buffer pool exhausted or no size class fits")
}

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
        #[serde(default)]
        no_init: bool,
        #[serde(default)]
        init_path: Option<String>,
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
    Attach {
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
    no_init: bool,
    init_path: Option<String>,
}

// ── OpLocal: the per-op local state `L` ─────────────────────────────────────
//
// There are no spawned tasks: every long-lived loop is a plain `fn` callback
// that re-arms its own next op when the current one completes. Each op's
// private state — the buffers it is filling, the line it is assembling, the
// remaining chunks of a partially written frame — travels as `OpLocal`, moved
// into the op on dispatch and handed back on completion.
//
// Anything shared lives in the runtime's global `G` (`DaemonState`), reached
// through `rt.global()` / `rt.global_mut()`. A callback may mutate it freely,
// but must finish its borrow before dispatching the next op, so no borrow is
// live across a completion.

enum OpLocal {
    /// A datagram request is being received. Owns the `msghdr` and the byte
    /// buffer the kernel fills; re-armed after each request is handled.
    Datagram {
        recv: MsgMut,
        buf: Vec<u8>,
        sender: Box<libc::sockaddr_un>,
    },
    /// An SCM_RIGHTS fd-pass reply is being sent to a `Logs { follow }`
    /// client. Owns the message, the destination address the kernel reads on
    /// completion, and the pipe read end to close afterwards.
    FollowSend {
        /// Kept alive only so the SCM_RIGHTS payload outlives the op.
        #[allow(dead_code)]
        slot: Msg,
        /// The kernel dereferences `msg_name` when the op runs, so the
        /// destination must outlive the op — not just the dispatch.
        #[allow(dead_code)]
        sender: Box<libc::sockaddr_un>,
        close_after: RawFd,
    },
    /// An attach client is being accepted.
    Accept,
    /// A `signalfd_siginfo` record is being read.
    Sigchld {
        /// Kept alive only so the kernel's write target outlives the op.
        #[allow(dead_code)]
        buf: BytesMut,
    },
    /// A container's stdout/stderr pipe is being drained. `line_buf` holds
    /// the trailing partial line between reads.
    ContainerOutput { pid: pid_t, line_buf: Vec<u8> },
    /// A detached TTY container's pty master is being drained.
    ContainerPtyOutput { pid: pid_t, line_buf: Vec<u8> },
    /// A log line is being written to one follow pipe. `chunks` is the
    /// remaining work: `write` covers one chunk at a time, so this is the
    /// `write_all` state machine, re-armed until the queue empties.
    FollowWrite {
        pid: pid_t,
        pipe_id: u64,
        fd: RawFd,
        chunks: VecDeque<Bytes>,
    },
    /// A session's 3-byte frame header is being accumulated.
    SessionHeader { session_id: u64, acc: Vec<u8> },
    /// A session frame payload is being accumulated (`read_exact` semantics:
    /// short reads are resumed until `acc` reaches `need`).
    SessionPayload {
        session_id: u64,
        frame_type: u8,
        acc: Vec<u8>,
        need: usize,
    },
    /// A framed message is being written to a session stream. `buf` holds the
    /// complete frame (3-byte header + payload) and `off` the bytes already
    /// written, so a partial write resumes exactly where it stopped.
    SessionFrame {
        session_id: u64,
        stream_fd: RawFd,
        buf: Bytes,
        off: usize,
    },
    /// A `0x10` data frame is going out in two writes: the 3-byte header, then
    /// the payload straight out of the read's provided buffer. The whole slab
    /// is registered as fixed buffer index 0, so `write_fixed` addresses a
    /// provided buffer and the payload never has to be copied into the fixed
    /// pool — only the 3-byte header is.
    SessionDataHeader {
        session_id: u64,
        stream_fd: RawFd,
        header: Bytes,
        /// The read's provided buffer, written once the header is on the wire.
        payload: Bytes,
        off: usize,
    },
    /// The payload half of a `0x10` data frame, written directly from the
    /// provided slot the read filled.
    SessionDataPayload {
        session_id: u64,
        stream_fd: RawFd,
        payload: Bytes,
        off: usize,
    },
    /// A session's output source (pty master or follow pipe) is being drained.
    SessionOutputRead { session_id: u64 },
    /// Client stdin bytes are being written to the container.
    SessionStdin {
        session_id: u64,
        buf: Bytes,
        off: usize,
    },
    /// A cancelled op's completion; carries nothing, but `cancel` requires a
    /// local for its acknowledgement callback.
    Cancelled,
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

    #[allow(dead_code)]
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

// ── LogGateway: cache + follow pipes ───────────────────────────────────────
//
// The cache and pipe bookkeeping are synchronous; the actual pipe writes are
// dispatched as ops from the container-output loop. `push` marks each idle
// pipe in-flight so at most one write is outstanding per pipe at a time, and
// returns the ids the caller must write to.

struct AsyncPipeWriter {
    id: u64,
    fd: RawFd,
    in_flight: bool,
}

impl AsyncPipeWriter {
    fn new(id: u64, fd: RawFd) -> Self {
        Self {
            id,
            fd,
            in_flight: false,
        }
    }

    /// Returns `true` if the pipe is still alive after this completion.
    fn complete(&mut self, ok: bool) -> bool {
        self.in_flight = false;
        if !ok {
            let _ = unsafe { libc::close(self.fd) };
            self.fd = -1;
            false
        } else {
            true
        }
    }
}

struct LogGateway {
    cache: LogCache,
    pipes: Vec<AsyncPipeWriter>,
}

impl LogGateway {
    fn new(cap: usize) -> Self {
        Self {
            cache: LogCache::new(cap),
            pipes: Vec::new(),
        }
    }

    /// Records `line` in the cache and marks every idle pipe in-flight for it.
    /// Returns the ids of the pipes the caller must write to (the caller drops
    /// the borrow before dispatching, so no borrow is live across a
    /// completion).
    fn push(&mut self, line: &[u8]) -> Vec<u64> {
        self.cache.push(line);
        self.pipes
            .iter_mut()
            .filter(|p| !p.in_flight)
            .map(|p| {
                p.in_flight = true;
                p.id
            })
            .collect()
    }

    /// `ok == true` clears the in-flight flag; a failed write removes the pipe.
    fn complete_write(&mut self, pipe_id: u64, ok: bool) {
        if let Some(idx) = self.pipes.iter().position(|p| p.id == pipe_id) {
            let alive = self.pipes[idx].complete(ok);
            if !alive {
                self.pipes.swap_remove(idx);
            }
        }
    }

    /// The current fd of `pipe_id`, or `None` if it was removed.
    fn pipe_fd(&self, pipe_id: u64) -> Option<RawFd> {
        self.pipes.iter().find(|p| p.id == pipe_id).map(|p| p.fd)
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
    stdin_fd: RawFd, // write end of stdin pipe (-1 if none)
    ptm_fd: RawFd,   // PTY master fd (-1 if none)
    /// Write end of the death pipe (-1 if none). Held open for the container's
    /// lifetime; the kernel closes it when the daemon exits, which wakes the
    /// built-in init's `poll()` with `POLLHUP` and kills the namespace.
    death_pipe_write: RawFd,
    /// Number of output-drain tasks still running for this container (the
    /// stdout/stderr drain and, for `--tty`, the pty-echo drain). The container
    /// is kept in `containers` until the count reaches zero, so the drains can
    /// flush their remaining output to the follow pipes before cleanup closes
    /// them. Attach-only containers have no drain tasks and are cleaned up by
    /// the reaper directly.
    drains_pending: u32,
}

/// One container's stdout/stderr pipe being drained. The read buffer and line
/// assembly live in the container-output loop's `OpLocal`; the pty echo for
/// detached TTY containers has its own sibling loop.
struct Output {
    fd: RawFd,
}

// ── Attach Session ────────────────────────────────────────────────────────

struct AttachSession {
    stream_fd: RawFd,
    ptm_fd: RawFd,      // PTY master for a tty run_attach (-1 otherwise)
    input_fd: RawFd,    // fd to write stdin to (ptm or stdin pipe, -1 if none)
    log_read_fd: RawFd, // follow-pipe / log-pipe reader for output (-1 if none)
    container_pid: pid_t,
    child_exited: bool,
    exit_code: Option<i32>,
    /// The output drain hit EOF. Together with `child_exited` this releases
    /// the deferred `0x02` exit frame: whichever of the two happens last sends
    /// it, so no loop has to park waiting for the other.
    drain_done: bool,
    /// The `0x02` exit frame write is in flight (or already sent).
    exit_sent: bool,
    /// The session's client-frame read op, so `close_session` can cancel it
    /// instead of closing an fd out from under an in-flight read.
    read_op: Option<coio::Op>,
    /// The session's output-drain read op, cancelled the same way.
    output_op: Option<coio::Op>,
}

// ── Follow (fd-pass) ───────────────────────────────────────────────────────
//
// A `Logs { follow: true }` request runs entirely inside the datagram loop: it
// snapshots the backlog into an owned `Vec`, writes it synchronously to a
// fresh pipe (the backlog is at most the cache capacity, which fits the default
// pipe capacity, and no op is dispatched during the write so no line can slip
// in between the snapshot and the pipe being attached), then passes the read
// end to the client with SCM_RIGHTS via a `sendmsg` op. The gateway pipe is
// registered only after the backlog write, so live output then flows to the
// pipe through the normal container-output loop.

// ── DaemonState: the runtime's global `G` ─────────────────────────────────

struct DaemonState {
    socket_path: PathBuf,
    datagram_fd: RawFd,
    attach_listener_fd: RawFd,
    sigchld_fd: RawFd,
    containers: HashMap<pid_t, ContainerInfo>,
    outputs: HashMap<pid_t, Output>,
    log_graveyard: HashMap<pid_t, LogCache>,
    next_pipe_id: u64,
    attach_sessions: HashMap<u64, AttachSession>,
    next_session_id: u64,
}

impl DaemonState {
    fn new(socket_path: PathBuf) -> Self {
        Self {
            socket_path,
            datagram_fd: -1,
            attach_listener_fd: -1,
            sigchld_fd: -1,
            containers: HashMap::new(),
            outputs: HashMap::new(),
            log_graveyard: HashMap::new(),
            next_pipe_id: 0,
            attach_sessions: HashMap::new(),
            next_session_id: 0,
        }
    }
}

// ── Daemon entry point ────────────────────────────────────────────────────

/// The daemon's runtime: shared state as `G`, [`OpLocal`] as `L`. Every
/// callback below takes this one type, which is why they can all be plain
/// function pointers.
type Rt = Runtime<DaemonState, OpLocal>;

pub struct Daemon {
    socket_path: PathBuf,
}

impl Daemon {
    pub fn new(socket_path: PathBuf) -> Self {
        Self { socket_path }
    }

    pub fn run(&mut self) -> io::Result<()> {
        let state = DaemonState::new(self.socket_path.clone());
        // `boot` runs exactly once, before the runtime drives anything. It
        // binds the sockets and arms the three persistent loops; each of those
        // re-arms itself from its completion callback, so `block_on` returns
        // only when every loop has finished.
        block_on_default(state, boot);
        Ok(())
    }
}

/// The bootstrap: bind the sockets, arm the SIGCHLD signalfd, store the fds
/// in the shared state, then arm the three persistent loops.
///
/// Every persistent loop keeps one op in flight at all times, so the runtime
/// never runs dry while the daemon should stay up.
fn boot(rt: &mut Rt) {
    let socket_path = rt.global().socket_path.clone();
    let dir = socket_path.parent().expect("socket path has a parent");
    if let Err(e) = std::fs::create_dir_all(dir) {
        tracing::error!(%e, path = %dir.display(), "cannot create socket dir");
        return;
    }
    if socket_path.exists() {
        let _ = std::fs::remove_file(&socket_path);
    }

    let datagram = match UnixDatagram::bind(&socket_path) {
        Ok(d) => d,
        Err(e) => {
            tracing::error!(%e, path = %socket_path.display(), "cannot bind datagram socket");
            return;
        }
    };
    let datagram_fd = datagram.as_raw_fd();
    tracing::info!(path = %socket_path.display(), "daemon listening (datagram)");

    let mut stream_path = socket_path.clone().into_os_string();
    stream_path.push(".stream");
    let stream_path = PathBuf::from(stream_path);
    if stream_path.exists() {
        let _ = std::fs::remove_file(&stream_path);
    }
    let listener = match std::os::unix::net::UnixListener::bind(&stream_path) {
        Ok(l) => l,
        Err(e) => {
            tracing::error!(%e, path = %stream_path.display(), "cannot bind stream listener");
            return;
        }
    };
    let attach_listener_fd = listener.as_raw_fd();
    tracing::info!(path = %stream_path.display(), "daemon listening (stream)");

    let sigchld_fd = match setup_sigchld_fd() {
        Ok(fd) => fd,
        Err(e) => {
            tracing::error!(%e, "cannot set up SIGCHLD signalfd");
            return;
        }
    };

    // Keep the sockets bound for the daemon's lifetime (same trick as the
    // original run(): the owning objects are forgotten, the fds stay open).
    std::mem::forget(datagram);
    std::mem::forget(listener);

    {
        let s = rt.global_mut();
        {
            s.datagram_fd = datagram_fd;
            s.attach_listener_fd = attach_listener_fd;
            s.sigchld_fd = sigchld_fd;
        }
    };

    arm_datagram(rt);
    arm_accept(rt);
    arm_sigchld(rt);
}

// ── Signal reaper ─────────────────────────────────────────────────────────

/// Arms one `signalfd_siginfo` read. The callback reaps and re-arms, so the
/// daemon always has a SIGCHLD read outstanding.
fn arm_sigchld(rt: &mut Rt) {
    let sigchld_fd = rt.global().sigchld_fd;
    let buf = match alloc_bytes(128) {
        Ok(b) => b,
        Err(e) => {
            tracing::warn!(%e, "cannot allocate sigchld buffer, reaper exiting");
            return;
        }
    };
    let _ = read(rt, sigchld_fd, 128, OpLocal::Sigchld { buf }, on_sigchld);
}

fn on_sigchld(rt: &mut Rt, local: OpLocal, res: io::Result<Bytes>) {
    let OpLocal::Sigchld { .. } = local else {
        return;
    };
    match res {
        Ok(bytes) if !bytes.is_empty() => reap_children(rt),
        Ok(_) => {}
        Err(e) => {
            tracing::warn!(%e, "sigchld read failed, reaper exiting");
            return;
        }
    }
    arm_sigchld(rt);
}

fn reap_children(rt: &mut Rt) {
    let mut finished_sessions = Vec::new();
    loop {
        let mut status: i32 = 0;
        match sys::wait4(-1, &mut status, libc::WNOHANG, None) {
            Ok(pid) if pid > 0 => {
                tracing::info!(%pid, %status, "container exited");
                let exit_code = if libc::WIFEXITED(status) {
                    libc::WEXITSTATUS(status)
                } else if libc::WIFSIGNALED(status) {
                    128 + libc::WTERMSIG(status)
                } else {
                    1
                };

                let maybe_session = {
                    let s = rt.global();
                    let found = s
                        .attach_sessions
                        .iter()
                        .find_map(|(&id, sess)| (sess.container_pid == pid).then_some(id));
                    // A pid we neither manage nor attach to is not ours; the
                    // caller skips it.
                    if found.is_some() || s.containers.contains_key(&pid) {
                        Some(found)
                    } else {
                        None
                    }
                };
                let Some(session_id) = maybe_session else {
                    continue;
                };

                if let Some(sid) = session_id {
                    // Record the exit so the session's deferred `0x02` frame can
                    // be sent once its output drain also reaches EOF. Whichever
                    // of the two events happens last sends it, which is what
                    // `maybe_finish_session` below decides.
                    let ready = {
                        let s = rt.global_mut();
                        let Some(sess) = s.attach_sessions.get_mut(&sid) else {
                            continue;
                        };
                        sess.child_exited = true;
                        sess.exit_code = Some(exit_code);
                        Some(sid)
                    };
                    if let Some(sid) = ready {
                        finished_sessions.push(sid);
                    }
                }

                // Remove the container: move its log cache to the graveyard,
                // close its fds and follow pipes, clean up the overlay. If the
                // container still has output drains running (detached
                // containers finish draining after exit), defer
                // the cleanup to `finish_drain` so the follow pipes stay open
                // until the last of the buffered output has been delivered.
                let removed = {
                    let s = rt.global_mut();
                    if s.containers.get(&pid).is_none_or(|i| i.drains_pending > 0) {
                        None
                    } else {
                        let mut info = s.containers.remove(&pid).expect("checked above");
                        let cache =
                            std::mem::replace(&mut info.gateway.cache, LogCache::new(LOG_CAPACITY));
                        s.log_graveyard.insert(pid, cache);
                        if info.ptm_fd >= 0 {
                            sys::close(info.ptm_fd);
                            info.ptm_fd = -1;
                        }
                        if info.stdin_fd >= 0 {
                            sys::close(info.stdin_fd);
                            info.stdin_fd = -1;
                        }
                        if info.death_pipe_write >= 0 {
                            sys::close(info.death_pipe_write);
                            info.death_pipe_write = -1;
                        }
                        info.gateway.close_all_pipes();
                        Some(info)
                    }
                };
                let Some(removed) = removed else {
                    continue;
                };
                if let Some(ref overlay) = removed.overlay_dir
                    && !removed.save
                {
                    cleanup_overlay(overlay);
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
    // Reap is a loop, so a session may have had both its exit and its drain
    // EOF observed here; check once the sweep is done.
    for sid in finished_sessions {
        maybe_finish_session(rt, sid);
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
            std::mem::size_of::<libc::sa_family_t>() as _,
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
    /// Death pipe for the default init: `(read, write)`. Present only when
    /// running the built-in init. The daemon keeps the write end; the init
    /// monitors the read end for `POLLHUP` on daemon exit.
    death_pipe: Option<sys::FdPair>,
    /// Prebuilt argv for a custom init: `[init_path, "--", cmd, ...]`.
    init_argv: Option<Vec<CString>>,
    /// Run the user command directly as PID 1 (no init, no death pipe).
    no_init: bool,
}

/// Resolve all container resources needed before `clone3`: canonicalize
/// rootfs, create overlay tempdir, allocate the interprocess OneshotSignal,
/// join the target netns if `net_pid` is set, and compute `clone_flags`.
/// Cleanup of acquired resources (overlay dir, signal) is the caller's
/// responsibility on error.
fn prepare_run(args: RunArgs) -> Result<PreparedResources, String> {
    if args.no_init && args.init_path.is_some() {
        return Err("--init and --no-init are mutually exclusive".into());
    }

    let rootfs = match args.rootfs {
        Some(p) => match Path::new(&p).canonicalize() {
            Ok(path) => Some(path),
            Err(e) => return Err(format!("invalid rootfs path: {e}")),
        },
        None => None,
    };

    let overlay_dir = match rootfs {
        Some(_) => match create_overlay_tempdir() {
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

    // Death pipe for the built-in init. Created before `clone3` so the child
    // inherits the read end; the daemon keeps the write end and the kernel
    // closes it when the daemon exits (SIGTERM, SIGKILL, crash — anything).
    let death_pipe = if args.init_path.is_none() && !args.no_init {
        let mut pipe_fds = sys::FdPair {
            read: -1,
            write: -1,
        };
        match sys::pipe2(&mut pipe_fds, libc::O_CLOEXEC) {
            Ok(_) => Some(pipe_fds),
            Err(e) => return Err(format!("death pipe creation failed: {e}")),
        }
    } else {
        None
    };

    // Custom init: build the full argv so the child can exec it directly.
    // The user command follows `--`, matching the tini/s6 convention.
    let init_argv = args.init_path.as_ref().map(|path| {
        let mut argv = Vec::with_capacity(args.command.len() + 2);
        argv.push(CString::from(path.as_str()));
        argv.push(CString::from("--"));
        argv.extend(args.command.iter().cloned());
        argv
    });

    Ok(PreparedResources {
        rootfs,
        overlay_dir,
        signal,
        clone_flags,
        needs_userns_maps,
        command: args.command,
        death_pipe,
        init_argv,
        no_init: args.no_init,
    })
}

/// Write uid/gid maps into the child's user namespace (if needed), then
/// signal the child to proceed.
fn parent_setup_maps_and_signal(
    pid: pid_t,
    needs_userns_maps: bool,
    signal: interprocess::OneshotSignal,
) -> Result<(), String> {
    if needs_userns_maps {
        setup_userns_maps(pid).map_err(|e| format!("uid_map write failed: {e}"))?;
    }
    signal.signal();
    Ok(())
}

/// Called in the child process after `clone3`. Never returns — either execs
/// successfully or calls `process::exit(1)`.
///
/// Three modes:
/// - `init_argv: Some(...)` — exec a custom init (user command follows `--`)
/// - `no_init` — exec the user command directly as PID 1
/// - otherwise — run the built-in init: fork the user command as PID 2 and
///   monitor the death pipe (`death_pipe_read`) for daemon exit
fn child_init_environment(
    rootfs: &Option<PathBuf>,
    overlay_dir: &Option<PathBuf>,
    command: Vec<CString>,
    death_pipe_read: Option<RawFd>,
    init_argv: Option<Vec<CString>>,
    no_init: bool,
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

        let container_root = match setup_overlay_rootfs(rootfs_path, overlay) {
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

    if let Some(argv) = init_argv {
        let argv = sys::Argv::new(argv);
        let errno = execvp(argv.as_slice());
        tracing::error!(%errno, "custom init exec failed");
        std::process::exit(127);
    }

    if no_init {
        let argv = sys::Argv::new(command);
        let errno = execvp(argv.as_slice());
        tracing::error!(%errno, "execvp failed");
        std::process::exit(1);
    }

    let death_pipe_read = death_pipe_read.expect("death pipe read end for default init");
    run_init_loop(death_pipe_read, command)
}

/// Built-in init: PID 1 in the container namespace. Forks the user command as
/// PID 2, then loops on the death pipe and a signalfd.
///
/// - `POLLHUP`/`POLLERR` on the death pipe read end means the daemon exited and
///   the kernel closed the write end: kill the whole namespace and exit
///   (SIGKILL cannot harm PID 1, so `_exit` is required).
/// - A signal from the signalfd is forwarded to the grandchild, except
///   `SIGCHLD`, which means the grandchild exited: reap it and exit with its
///   status so the daemon reports the real exit code.
fn run_init_loop(death_pipe_read: RawFd, command: Vec<CString>) -> ! {
    // Block catchable signals and SIGCHLD so they arrive via the signalfd.
    // The grandchild resets its mask before exec, so the user command sees a
    // normal signal environment.
    let mut mask: libc::sigset_t = unsafe { std::mem::zeroed() };
    unsafe {
        libc::sigemptyset(&mut mask);
        libc::sigaddset(&mut mask, libc::SIGTERM);
        libc::sigaddset(&mut mask, libc::SIGINT);
        libc::sigaddset(&mut mask, libc::SIGHUP);
        libc::sigaddset(&mut mask, libc::SIGCHLD);
    }
    if let Err(e) = sys::sigprocmask(libc::SIG_BLOCK, Some(&mask), None) {
        tracing::error!(%e, "sigprocmask failed in init");
        std::process::exit(1);
    }
    let sigfd = match sys::signalfd(-1, &mask, libc::SFD_CLOEXEC) {
        Ok(fd) => fd,
        Err(e) => {
            tracing::error!(%e, "signalfd failed in init");
            std::process::exit(1);
        }
    };

    let gpid = unsafe { libc::fork() };
    if gpid < 0 {
        tracing::error!("fork failed in init");
        std::process::exit(1);
    }
    if gpid == 0 {
        // Grandchild: exec the user command as PID 2.
        sys::close(death_pipe_read);
        sys::close(sigfd);
        let mut empty: libc::sigset_t = unsafe { std::mem::zeroed() };
        unsafe { libc::sigemptyset(&mut empty) };
        let _ = sys::sigprocmask(libc::SIG_SETMASK, Some(&empty), None);
        let argv = sys::Argv::new(command);
        let errno = execvp(argv.as_slice());
        tracing::error!(%errno, "execvp failed in init grandchild");
        std::process::exit(127);
    }

    let mut pollfds = [
        libc::pollfd {
            fd: death_pipe_read,
            events: libc::POLLIN,
            revents: 0,
        },
        libc::pollfd {
            fd: sigfd,
            events: libc::POLLIN,
            revents: 0,
        },
    ];

    loop {
        let ret = unsafe { libc::poll(pollfds.as_mut_ptr(), pollfds.len() as libc::nfds_t, -1) };
        if ret < 0 {
            let e = io::Error::last_os_error();
            if e.raw_os_error() == Some(libc::EINTR) {
                continue;
            }
            tracing::error!(%e, "poll failed in init");
            std::process::exit(1);
        }

        if pollfds[0].revents & (libc::POLLHUP | libc::POLLERR | libc::POLLIN) != 0 {
            unsafe { libc::kill(-1, libc::SIGKILL) };
            std::process::exit(137);
        }

        if pollfds[1].revents & libc::POLLIN != 0 {
            let mut info: libc::signalfd_siginfo = unsafe { std::mem::zeroed() };
            let info_buf = unsafe {
                std::slice::from_raw_parts_mut(
                    (&mut info as *mut libc::signalfd_siginfo).cast::<u8>(),
                    std::mem::size_of::<libc::signalfd_siginfo>(),
                )
            };
            if let Err(e) = sys::read(sigfd, info_buf) {
                tracing::error!(%e, "signalfd read failed in init");
                std::process::exit(1);
            }

            if info.ssi_signo as libc::c_int == libc::SIGCHLD {
                let mut status: libc::c_int = 0;
                let reaped = sys::wait4(gpid, &mut status, libc::WNOHANG, None).unwrap_or(-1);
                if reaped == gpid {
                    let exit_code = if libc::WIFEXITED(status) {
                        libc::WEXITSTATUS(status)
                    } else if libc::WIFSIGNALED(status) {
                        128 + libc::WTERMSIG(status)
                    } else {
                        1
                    };
                    std::process::exit(exit_code);
                }
            } else {
                unsafe { libc::kill(gpid, info.ssi_signo as libc::c_int) };
            }
        }
    }
}

// ── Async request handlers ──────────────────────────────────────────────────

/// Arms one datagram receive. The completion callback handles the request and
/// immediately re-arms, so the control socket always has a receive in flight.
fn arm_datagram(rt: &mut Rt) {
    let datagram_fd = rt.global().datagram_fd;
    let Some(mut recv) = MsgMut::new() else {
        return;
    };
    let buf = vec![0u8; 65536];
    recv.push_iov(libc::iovec {
        iov_base: buf.as_ptr() as *mut libc::c_void,
        iov_len: buf.len(),
    });
    // Allocate the sender buffer first so `msg_name` can point at it. The box's
    // contents never move when the box itself is moved into the op local, so
    // the pointer stays valid for the kernel to fill.
    let mut sender: Box<libc::sockaddr_un> = Box::new(unsafe { std::mem::zeroed() });
    {
        let msg = recv.msg();
        msg.msg_name = (&raw mut *sender).cast();
        msg.msg_namelen = std::mem::size_of::<libc::sockaddr_un>() as libc::socklen_t;
    }
    let ptr = recv.msg_ptr();
    // SAFETY: `recv` lives in the op local below, so the `msghdr` it owns
    // outlives the op.
    let _ = unsafe {
        recvmsg(
            rt,
            datagram_fd,
            ptr,
            0,
            OpLocal::Datagram { recv, buf, sender },
            on_datagram,
        )
    };
}

fn on_datagram(rt: &mut Rt, local: OpLocal, res: io::Result<usize>) {
    let OpLocal::Datagram {
        mut recv,
        buf,
        sender,
    } = local
    else {
        return;
    };
    let n = match res {
        Ok(n) => n,
        Err(e) => {
            tracing::warn!(%e, "datagram recv failed");
            arm_datagram(rt);
            return;
        }
    };
    let sender_len = recv.msg().msg_namelen;
    if n > 0 {
        let request: Request = match serde_json::from_slice(&buf[..n]) {
            Ok(r) => r,
            Err(e) => {
                reply_datagram(rt, &sender, sender_len, &ErrorResponse {
                    ok: false,
                    error: format!("invalid request: {e}"),
                });
                arm_datagram(rt);
                return;
            }
        };
        handle_datagram(rt, &sender, sender_len, request);
    }
    arm_datagram(rt);
}

fn handle_datagram(
    rt: &mut Rt,
    sender: &libc::sockaddr_un,
    sender_len: libc::socklen_t,
    request: Request,
) {
    match request {
        Request::Run {
            rootfs,
            net_pid,
            save,
            command,
            interactive,
            tty,
            no_init,
            init_path,
        } => handle_run(rt, sender, sender_len, RunArgs {
            rootfs,
            net_pid,
            save,
            command: CStringSerde::into_inner_vec(command),
            tty: tty.unwrap_or(false),
            interactive: interactive.unwrap_or(false),
            no_init,
            init_path,
        }),
        Request::List => {
            let now = SystemTime::now();
            let containers: Vec<ContainerSummary> = rt
                .global()
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
            reply_datagram(rt, sender, sender_len, &ListResponse { containers });
        }
        Request::Kill { pid } => {
            let pid = pid as pid_t;
            if !{
                let s = rt.global();
                s.containers.contains_key(&pid)
            } {
                reply_datagram(rt, sender, sender_len, &KillResponse {
                    ok: false,
                    error: Some(format!("container {pid} not found")),
                });
                return;
            }
            let ret = unsafe { libc::kill(pid, libc::SIGKILL) };
            if ret < 0 {
                let e = io::Error::last_os_error();
                reply_datagram(rt, sender, sender_len, &KillResponse {
                    ok: false,
                    error: Some(format!("kill failed: {e}")),
                });
                return;
            }
            tracing::info!(%pid, "sent SIGKILL");
            reply_datagram(rt, sender, sender_len, &KillResponse {
                ok: true,
                error: None,
            });
        }
        Request::Logs { pid, follow } => {
            let pid = pid as pid_t;
            if follow {
                handle_follow(rt, sender, sender_len, pid);
            } else {
                let s = rt.global_mut();
                let lines = if let Some(info) = s.containers.get_mut(&pid) {
                    Some(info.gateway.collect_lines())
                } else {
                    s.log_graveyard
                        .get_mut(&pid)
                        .map(|cache| cache.collect_lines())
                };
                let Some(lines) = lines else {
                    reply_datagram(rt, sender, sender_len, &ErrorResponse {
                        ok: false,
                        error: format!("container {pid} not found"),
                    });
                    return;
                };
                reply_datagram(rt, sender, sender_len, &LogsResponse { lines });
            }
        }
        Request::Attach { .. } => {
            reply_datagram(rt, sender, sender_len, &ErrorResponse {
                ok: false,
                error: "attach must be sent over the stream socket".into(),
            });
        }
    }
}

fn reply_datagram(
    rt: &Rt,
    sender: &libc::sockaddr_un,
    sender_len: libc::socklen_t,
    resp: &impl Serialize,
) {
    let data = serde_json::to_vec(resp).unwrap();
    let datagram_fd = rt.global().datagram_fd;
    if let Err(e) = send_datagram_raw(datagram_fd, sender, sender_len, &data) {
        tracing::error!(%e, "reply sendto failed");
    }
}

/// The `Logs { follow: true }` path. The backlog is snapshotted and written to
/// a fresh pipe synchronously (the snapshot is at most the cache capacity,
/// which fits the default pipe capacity, and no dispatch happens between the
/// snapshot and the write, so no other loop can push a line into the gap) — no
/// lock is needed. The read end is then passed to the client with an
/// SCM_RIGHTS `sendmsg` op, and the pipe writer is attached to the container's
/// gateway so live output flows to it through the normal container-output loop.
fn handle_follow(
    rt: &mut Rt,
    sender_ref: &libc::sockaddr_un,
    sender_len: libc::socklen_t,
    pid: pid_t,
) {
    tracing::debug!(%pid, "handle_follow");

    let backlog = rt
        .global_mut()
        .containers
        .get_mut(&pid)
        .map(|info| info.gateway.snapshot());
    let Some(backlog) = backlog else {
        reply_datagram(rt, sender_ref, sender_len, &ErrorResponse {
            ok: false,
            error: format!("container {pid} not found"),
        });
        return;
    };

    let mut pipe_fds = sys::FdPair {
        read: -1,
        write: -1,
    };
    if let Err(e) = sys::pipe2(&mut pipe_fds, libc::O_CLOEXEC) {
        tracing::error!(%e, "follow pipe creation failed");
        reply_datagram(rt, sender_ref, sender_len, &ErrorResponse {
            ok: false,
            error: format!("pipe creation failed: {e}"),
        });
        return;
    }

    if !backlog.is_empty() {
        let mut written = 0usize;
        while written < backlog.len() {
            let n = unsafe {
                libc::write(
                    pipe_fds.write,
                    backlog[written..].as_ptr() as *const _,
                    backlog.len() - written,
                )
            };
            if n < 0 {
                let e = io::Error::last_os_error();
                tracing::error!(%e, "backlog write to pipe failed");
                let _ = unsafe { libc::close(pipe_fds.write) };
                let _ = unsafe { libc::close(pipe_fds.read) };
                return;
            }
            if n == 0 {
                break;
            }
            written += n as usize;
        }
    }

    let attached = {
        let s = rt.global_mut();
        let pipe_id = s.next_pipe_id;
        s.next_pipe_id += 1;
        // The container may have been reaped while the backlog was written.
        match s.containers.get_mut(&pid) {
            Some(info) => {
                info.gateway
                    .pipes
                    .push(AsyncPipeWriter::new(pipe_id, pipe_fds.write));
                true
            }
            None => false,
        }
    };
    if !attached {
        let _ = unsafe { libc::close(pipe_fds.write) };
        let _ = unsafe { libc::close(pipe_fds.read) };
        return;
    }

    // Pass the read end to the client with SCM_RIGHTS.
    let Some(mut slot) = Msg::new() else {
        return;
    };
    if !slot.push_scm_rights(&[pipe_fds.read]) {
        tracing::error!("follow fd-pass cmsg did not fit");
        let _ = unsafe { libc::close(pipe_fds.write) };
        let _ = unsafe { libc::close(pipe_fds.read) };
        return;
    }
    // `msg_name` must stay valid until the kernel consumes it, so the
    // destination address is moved into the op local rather than borrowed from
    // this frame.
    let mut sender: Box<libc::sockaddr_un> = Box::new(unsafe { std::mem::zeroed() });
    *sender = *sender_ref;
    {
        let msg = slot.msg();
        msg.msg_name = (&raw mut *sender).cast();
        msg.msg_namelen = sender_len;
    }
    let ptr = slot.msg_ptr();
    let _ = sendmsg(
        rt,
        rt.global().datagram_fd,
        ptr,
        OpLocal::FollowSend {
            slot,
            sender,
            close_after: pipe_fds.read,
        },
        on_follow_send,
    );
}

fn on_follow_send(_rt: &mut Rt, local: OpLocal, res: io::Result<usize>) {
    let OpLocal::FollowSend { close_after, .. } = local else {
        return;
    };
    if let Err(e) = res {
        tracing::error!(%e, "fd-pass sendmsg failed");
    }
    let _ = unsafe { libc::close(close_after) };
}

/// Run a container in detached mode. Fully synchronous except for the armed
/// output drains: stdout/stderr are captured from a pipe and drained by the
/// container-output loop (and, for `--tty`, the pty echo by its sibling). The
/// caller receives a `RunResponse` datagram.
fn handle_run(rt: &mut Rt, sender: &libc::sockaddr_un, sender_len: libc::socklen_t, args: RunArgs) {
    let err = |rt: &Rt, msg: &str| {
        reply_datagram(rt, sender, sender_len, &ErrorResponse {
            ok: false,
            error: msg.to_string(),
        });
    };

    let save = args.save;
    let use_pty = args.tty;
    let interactive = args.interactive;
    let use_stdin_pipe = interactive && !use_pty;
    let prep = match prepare_run(args) {
        Ok(p) => p,
        Err(e) => {
            err(rt, &e);
            return;
        }
    };

    let mut stdin_pipe_fds = sys::FdPair {
        read: -1,
        write: -1,
    };
    if use_stdin_pipe && let Err(e) = sys::pipe2(&mut stdin_pipe_fds, libc::O_CLOEXEC) {
        err(rt, &format!("stdin pipe creation failed: {e}"));
        return;
    }

    let (pty_master, pty_slave) = if use_pty {
        match pty::open_pty() {
            Ok((m, s)) => (Some(m), Some(s)),
            Err(e) => {
                err(rt, &format!("pty allocation failed: {e}"));
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
    if let Err(e) = sys::pipe2(&mut pipe_fds, libc::O_CLOEXEC) {
        err(rt, &format!("log pipe creation failed: {e}"));
        return;
    }

    let clone_result = clone3_container(prep.clone_flags);
    match clone_result {
        Err(e) => {
            sys::close(pipe_fds.read);
            sys::close(pipe_fds.write);
            if stdin_pipe_fds.read >= 0 {
                sys::close(stdin_pipe_fds.read);
            }
            if stdin_pipe_fds.write >= 0 {
                sys::close(stdin_pipe_fds.write);
            }
            drop(pty_master);
            drop(pty_slave);
            if let Some(dp) = &prep.death_pipe {
                sys::close(dp.read);
                sys::close(dp.write);
            }
            err(rt, &format!("clone3 failed: {e}"));
        }
        Ok(None) => {
            // Child.
            sys::close(pipe_fds.read);
            let _ = sys::dup2(pipe_fds.write, libc::STDOUT_FILENO);
            let _ = sys::dup2(pipe_fds.write, libc::STDERR_FILENO);
            if pipe_fds.write != libc::STDOUT_FILENO && pipe_fds.write != libc::STDERR_FILENO {
                sys::close(pipe_fds.write);
            }

            if use_pty {
                drop(pty_master);
                if let Some(slave) = pty_slave
                    && let Err(e) = slave.make_controlling()
                {
                    tracing::error!(%e, "pty setup failed in child");
                    std::process::exit(1);
                }
                // After make_controlling, dup2 the pipe write to stdout/stderr
                // too so output goes to the log gateway.
                let _ = sys::dup2(pipe_fds.write, libc::STDOUT_FILENO);
                let _ = sys::dup2(pipe_fds.write, libc::STDERR_FILENO);
                if pipe_fds.write != libc::STDOUT_FILENO && pipe_fds.write != libc::STDERR_FILENO {
                    sys::close(pipe_fds.write);
                }
                if stdin_pipe_fds.read >= 0 {
                    sys::close(stdin_pipe_fds.read);
                }
                if stdin_pipe_fds.write >= 0 {
                    sys::close(stdin_pipe_fds.write);
                }
            } else if use_stdin_pipe {
                sys::close(stdin_pipe_fds.write);
                let _ = sys::dup2(stdin_pipe_fds.read, libc::STDIN_FILENO);
                sys::close(stdin_pipe_fds.read);
            } else {
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
            // Default init: drop the write end so the daemon is the sole
            // holder; keep the read end for the init loop.
            let death_pipe_read = prep.death_pipe.map(|dp| {
                sys::close(dp.write);
                dp.read
            });
            child_init_environment(
                &prep.rootfs,
                &prep.overlay_dir,
                prep.command,
                death_pipe_read,
                prep.init_argv,
                prep.no_init,
            );
        }
        Ok(Some(pid)) => {
            // Parent.
            sys::close(pipe_fds.write);
            drop(pty_slave);

            // Keep only the write end of the death pipe; the init holds the
            // read end. The kernel closes the write end when the daemon dies.
            let death_pipe_write = match prep.death_pipe {
                Some(dp) => {
                    sys::close(dp.read);
                    dp.write
                }
                None => -1,
            };

            let stdin_write_fd = if use_stdin_pipe {
                sys::close(stdin_pipe_fds.read);
                stdin_pipe_fds.write
            } else {
                -1
            };
            let ptm_fd = pty_master.map_or(-1, |m| {
                let fd = m.raw_fd();
                std::mem::forget(m);
                fd
            });

            if let Err(e) = parent_setup_maps_and_signal(pid, prep.needs_userns_maps, prep.signal) {
                sys::close(pipe_fds.read);
                if stdin_write_fd >= 0 {
                    sys::close(stdin_write_fd);
                }
                if ptm_fd >= 0 {
                    sys::close(ptm_fd);
                }
                if death_pipe_write >= 0 {
                    sys::close(death_pipe_write);
                }
                err(rt, &format!("container aborted: {e}"));
                return;
            }

            let cmd_str = prep
                .command
                .iter()
                .map(|c| unsafe { std::str::from_utf8_unchecked(c.to_bytes()) })
                .collect::<Vec<_>>()
                .join(" ");
            let has_pty = ptm_fd >= 0;

            {
                let s = rt.global_mut();
                {
                    s.outputs.insert(pid, Output { fd: pipe_fds.read });
                    s.containers.insert(pid, ContainerInfo {
                        pid,
                        command: cmd_str,
                        overlay_dir: prep.overlay_dir,
                        save,
                        start_time: SystemTime::now(),
                        gateway: LogGateway::new(LOG_CAPACITY),
                        stdin_fd: stdin_write_fd,
                        ptm_fd,
                        death_pipe_write,
                        drains_pending: 0,
                    });
                }
            };

            // Arm the output drains. The counter is incremented first so the
            // reaper defers container cleanup until both drains have flushed.
            {
                let s = rt.global_mut();
                {
                    if let Some(info) = s.containers.get_mut(&pid) {
                        info.drains_pending += 1;
                    }
                }
            };
            arm_container_output(rt, pid);

            if has_pty {
                {
                    let s = rt.global_mut();
                    {
                        if let Some(info) = s.containers.get_mut(&pid) {
                            info.drains_pending += 1;
                        }
                    }
                };
                arm_container_pty_output(rt, pid);
            }

            tracing::info!(%pid, "container started");
            reply_datagram(rt, sender, sender_len, &RunResponse {
                ok: true,
                pid: Some(pid),
                error: None,
            });
        }
    }
}

// ── Container output drains ───────────────────────────────────────────────

/// Accumulate `bytes` into `line_buf`, extracting complete `\n`-terminated
/// lines into `lines`. A trailing partial line stays in `line_buf`.
fn drain_lines(line_buf: &mut Vec<u8>, bytes: &[u8], lines: &mut Vec<Vec<u8>>) {
    line_buf.extend_from_slice(bytes);
    let mut start = 0usize;
    let total = line_buf.len();
    for i in 0..total {
        if line_buf[i] == b'\n' {
            lines.push(line_buf[start..i].to_vec());
            start = i + 1;
        }
    }
    if start < total {
        line_buf.drain(..start);
    } else {
        line_buf.clear();
    }
}

/// Split `line` plus its trailing `\n` into fixed-pool-sized write buffers.
/// Each non-final buffer carries exactly [`WRITE_CHUNK_PAYLOAD`] bytes; the
/// final buffer carries the remainder and the `\n`. Pipes are byte streams, so
/// writing the chunks in order delivers exactly `line` + `\n`.
fn split_line(line: &[u8]) -> Vec<Bytes> {
    let mut chunks = Vec::new();
    let mut rest = line;
    loop {
        let take = rest.len().min(WRITE_CHUNK_PAYLOAD);
        let last = take == rest.len();
        let mut buf = pool_alloc(take + 1);
        buf.set_len((take + 1) as u32);
        buf[..take].copy_from_slice(&rest[..take]);
        if last {
            buf[take] = b'\n';
        }
        chunks.push(buf.into_bytes());
        if last {
            return chunks;
        }
        rest = &rest[take..];
    }
}

/// Pushes `lines` into each pending follow pipe: records them in the cache,
/// then arms one [`OpLocal::FollowWrite`] per idle pipe. All state access is a
/// short synchronous borrow, and the dispatch happens after the borrow is
/// released. A line's in-flight mark is held across all of its chunks, so the
/// next line can never interleave with it on the same pipe.
fn write_lines(rt: &mut Rt, pid: pid_t, lines: Vec<Vec<u8>>) {
    for line in lines {
        let s = rt.global_mut();
        let pending = match s.containers.get_mut(&pid) {
            Some(info) => {
                let mut pend = Vec::new();
                for id in info.gateway.push(&line) {
                    if let Some(fd) = info.gateway.pipe_fd(id) {
                        pend.push((id, fd));
                    }
                }
                pend
            }
            None => {
                if let Some(cache) = s.log_graveyard.get_mut(&pid) {
                    cache.push(&line);
                }
                Vec::new()
            }
        };
        for (pipe_id, fd) in pending {
            arm_follow_write(rt, pid, pipe_id, fd, split_line(&line).into());
        }
    }
}

/// Arms the next chunk of one line's write to one follow pipe. The op's
/// completion callback feeds the following chunk, so the whole line drains
/// one write at a time.
fn arm_follow_write(rt: &mut Rt, pid: pid_t, pipe_id: u64, fd: RawFd, chunks: VecDeque<Bytes>) {
    let Some(chunk) = chunks.front() else {
        // Nothing to write (an empty line still yields one chunk, so this is
        // only reachable for a genuinely empty queue).

        let s = rt.global_mut();
        if let Some(info) = s.containers.get_mut(&pid) {
            info.gateway.complete_write(pipe_id, true);
        }
        return;
    };
    let addr = chunk.as_ptr().as_ptr();
    let len = chunk.len();
    let _ = write(
        rt,
        fd,
        addr,
        len,
        OpLocal::FollowWrite {
            pid,
            pipe_id,
            fd,
            chunks,
        },
        on_follow_write,
    );
}

fn on_follow_write(rt: &mut Rt, local: OpLocal, res: io::Result<usize>) {
    let OpLocal::FollowWrite {
        pid,
        pipe_id,
        fd,
        mut chunks,
    } = local
    else {
        return;
    };
    match res {
        Ok(n) => {
            let len = chunks.front().map_or(0, Bytes::len);
            if n >= len {
                // This chunk is fully delivered; the next one takes over.
                chunks.pop_front();
            } else if n > 0 {
                // Partial write: re-issue the remainder of this chunk. `sub`
                // registers one more borrower, so dropping `rest` below leaves
                // exactly one view on the remainder.
                let rest = chunks.pop_front().expect("chunk present");
                let slice = rest.sub(n, len).expect("remainder in bounds");
                chunks.push_front(slice);
            }
            if chunks.is_empty() {
                {
                    let s = rt.global_mut();
                    {
                        if let Some(info) = s.containers.get_mut(&pid) {
                            info.gateway.complete_write(pipe_id, true);
                        }
                    }
                };
            } else {
                arm_follow_write(rt, pid, pipe_id, fd, chunks);
            }
        }
        Err(e) => {
            tracing::warn!(%e, "follow pipe write failed");
            {
                let s = rt.global_mut();
                {
                    if let Some(info) = s.containers.get_mut(&pid) {
                        info.gateway.complete_write(pipe_id, false);
                    }
                }
            };
        }
    }
}

/// Decrements a container's drain counter. When the last output/pty drain loop
/// finishes, removes the container, moves its log cache to the graveyard,
/// closes its fds and follow pipes, and cleans up its overlay. No-op when the
/// container is already gone (reaper cleaned it up, or it had no drain loops).
fn finish_drain(rt: &mut Rt, pid: pid_t) {
    let removed = {
        let s = rt.global_mut();
        let Some(info) = s.containers.get_mut(&pid) else {
            return;
        };
        info.drains_pending = info.drains_pending.saturating_sub(1);
        if info.drains_pending > 0 {
            return;
        }
        let mut info = s.containers.remove(&pid).expect("just found");
        let cache = std::mem::replace(&mut info.gateway.cache, LogCache::new(LOG_CAPACITY));
        s.log_graveyard.insert(pid, cache);
        if info.ptm_fd >= 0 {
            sys::close(info.ptm_fd);
            info.ptm_fd = -1;
        }
        if info.stdin_fd >= 0 {
            sys::close(info.stdin_fd);
            info.stdin_fd = -1;
        }
        info.gateway.close_all_pipes();
        info
    };
    if let Some(ref overlay) = removed.overlay_dir
        && !removed.save
    {
        cleanup_overlay(overlay);
    }
}

/// Drains one container's stdout/stderr pipe into its log gateway. The read
/// buffer and line assembly live in this loop's local; the gateway and pipe
/// bookkeeping are touched only inside short synchronous borrows.
fn arm_container_output(rt: &mut Rt, pid: pid_t) {
    let fd = rt.global().outputs.get(&pid).map(|o| o.fd);
    let Some(fd) = fd else {
        tracing::warn!(%pid, "container output task: no output entry");
        finish_drain(rt, pid);
        return;
    };
    let _ = read(
        rt,
        fd,
        4096,
        OpLocal::ContainerOutput {
            pid,
            line_buf: Vec::new(),
        },
        on_container_output,
    );
}

fn on_container_output(rt: &mut Rt, local: OpLocal, res: io::Result<Bytes>) {
    let OpLocal::ContainerOutput { pid, mut line_buf } = local else {
        return;
    };
    let bytes = match res {
        Ok(b) if !b.is_empty() => b,
        Ok(_) => {
            finish_container_output(rt, pid);
            return;
        }
        Err(e) => {
            tracing::warn!(%pid, %e, "output read failed, closing output");
            finish_container_output(rt, pid);
            return;
        }
    };
    let mut lines = Vec::new();
    drain_lines(&mut line_buf, &bytes, &mut lines);
    if !lines.is_empty() {
        write_lines(rt, pid, lines);
    }
    let _ = read(
        rt,
        rt.global().outputs.get(&pid).map(|o| o.fd).unwrap_or(-1),
        4096,
        OpLocal::ContainerOutput { pid, line_buf },
        on_container_output,
    );
}

/// EOF: drop the output entry, close the pipe, and finish this drain.
fn finish_container_output(rt: &mut Rt, pid: pid_t) {
    {
        let s = rt.global_mut();
        {
            if let Some(o) = s.outputs.remove(&pid)
                && o.fd >= 0
            {
                sys::close(o.fd);
            }
        }
    };
    finish_drain(rt, pid);
}

/// Drains a detached TTY container's pty master echo into its log gateway.
/// The pipe drain captures stdout/stderr; this captures the terminal echo.
fn arm_container_pty_output(rt: &mut Rt, pid: pid_t) {
    let ptm_fd = rt.global().containers.get(&pid).map(|info| info.ptm_fd);
    let Some(ptm_fd) = ptm_fd.filter(|&fd| fd >= 0) else {
        finish_drain(rt, pid);
        return;
    };
    let _ = read(
        rt,
        ptm_fd,
        4096,
        OpLocal::ContainerPtyOutput {
            pid,
            line_buf: Vec::new(),
        },
        on_container_pty_output,
    );
}

fn on_container_pty_output(rt: &mut Rt, local: OpLocal, res: io::Result<Bytes>) {
    let OpLocal::ContainerPtyOutput { pid, mut line_buf } = local else {
        return;
    };
    let bytes = match res {
        Ok(b) if !b.is_empty() => b,
        Ok(_) => {
            finish_container_pty_output(rt, pid);
            return;
        }
        Err(e) => {
            tracing::warn!(%pid, %e, "pty output read failed");
            finish_container_pty_output(rt, pid);
            return;
        }
    };
    let mut lines = Vec::new();
    drain_lines(&mut line_buf, &bytes, &mut lines);
    if !lines.is_empty() {
        write_lines(rt, pid, lines);
    }
    let fd = rt
        .global()
        .containers
        .get(&pid)
        .map(|i| i.ptm_fd)
        .unwrap_or(-1);
    let _ = read(
        rt,
        fd,
        4096,
        OpLocal::ContainerPtyOutput { pid, line_buf },
        on_container_pty_output,
    );
}

/// EOF/error: close the pty master, clear it on the container, and finish
/// this drain.
fn finish_container_pty_output(rt: &mut Rt, pid: pid_t) {
    {
        let s = rt.global_mut();
        {
            if let Some(info) = s.containers.get_mut(&pid)
                && info.ptm_fd >= 0
            {
                sys::close(info.ptm_fd);
                info.ptm_fd = -1;
            }
        }
    };
    finish_drain(rt, pid);
}

// ── Accept + attach sessions ────────────────────────────────────────────────

/// Arms one accept on the attach listener. Each accepted client becomes a
/// session whose own frame-read loop is armed from here.
fn arm_accept(rt: &mut Rt) {
    let listener_fd = rt.global().attach_listener_fd;
    let _ = accept(rt, listener_fd, OpLocal::Accept, on_accept);
}

fn on_accept(rt: &mut Rt, local: OpLocal, res: io::Result<RawFd>) {
    let OpLocal::Accept = local else {
        return;
    };
    let stream_fd = match res {
        Ok(fd) => fd,
        Err(e) => {
            tracing::warn!(%e, "accept failed, retrying");
            arm_accept(rt);
            return;
        }
    };

    let session_id = {
        let s = rt.global_mut();
        let session_id = s.next_session_id;
        s.next_session_id += 1;
        s.attach_sessions.insert(session_id, AttachSession {
            stream_fd,
            ptm_fd: -1,
            input_fd: -1,
            log_read_fd: -1,
            container_pid: 0,
            child_exited: false,
            exit_code: None,
            drain_done: false,
            exit_sent: false,
            read_op: None,
            output_op: None,
        });
        session_id
    };

    arm_session_read(rt, session_id);
    arm_accept(rt);
}

/// Tears down a session and cancels its in-flight ops.
///
/// The client-frame read and the output-drain read are cancelled rather than
/// merely having their fds closed underneath, so no completion can be
/// delivered against an fd number the kernel may already have handed out to
/// something else.
fn close_session(rt: &mut Rt, session_id: u64) {
    let (stream_fd, read_op, output_op) = {
        let s = rt.global_mut();
        let Some(mut sess) = s.attach_sessions.remove(&session_id) else {
            return;
        };
        tracing::info!(%session_id, "closing attach session");
        (sess.stream_fd, sess.read_op.take(), sess.output_op.take())
    };
    for op in [read_op, output_op].into_iter().flatten() {
        rt.cancel(op, OpLocal::Cancelled, on_cancelled);
    }
    if stream_fd >= 0 {
        sys::close(stream_fd);
    }
}

fn on_cancelled(_rt: &mut Rt, _local: OpLocal) {}

/// Sends the deferred `0x02` exit frame once *both* preconditions hold: the
/// output drain reached EOF and the child was reaped. Either event can be the
/// last, so both call this; whichever arrives second arms the frame.
///
/// No loop parks waiting: the handshake is driven by whichever event
/// completes it.
fn maybe_finish_session(rt: &mut Rt, session_id: u64) {
    let ready = {
        let s = rt.global_mut();
        let Some(sess) = s.attach_sessions.get_mut(&session_id) else {
            return;
        };
        if !(sess.drain_done && sess.child_exited) || sess.exit_sent {
            return;
        }
        sess.exit_sent = true;
        (sess.stream_fd, sess.exit_code.unwrap_or(0))
    };
    let (stream_fd, exit_code) = ready;
    let payload = serde_json::to_vec(&serde_json::json!({ "exit_code": exit_code })).unwrap();
    arm_session_frame(rt, session_id, stream_fd, 0x02, payload);
}

/// Arms one framed message (type + u16 length + payload) on a session stream.
/// `write` covers one buffer at a time, so the frame is built whole and
/// written from `off`, resuming after a short write.
fn arm_session_frame(
    rt: &mut Rt,
    session_id: u64,
    stream_fd: RawFd,
    frame_type: u8,
    payload: Vec<u8>,
) {
    let len = 3 + payload.len();
    let mut buf = pool_alloc(len);
    buf.set_len(len as u32);
    buf[0] = frame_type;
    buf[1..3].copy_from_slice(&(payload.len() as u16).to_le_bytes());
    buf[3..].copy_from_slice(&payload);
    let addr = buf.as_ptr().as_ptr();
    let _ = write(
        rt,
        stream_fd,
        addr,
        len,
        OpLocal::SessionFrame {
            session_id,
            stream_fd,
            buf: buf.into_bytes(),
            off: 0,
        },
        on_session_frame,
    );
}

fn on_session_frame(rt: &mut Rt, local: OpLocal, res: io::Result<usize>) {
    let OpLocal::SessionFrame {
        session_id,
        stream_fd,
        buf,
        off,
    } = local
    else {
        return;
    };
    match res {
        Ok(n) => {
            let off = off + n;
            if off >= buf.len() {
                // The frame is fully delivered. The exit frame is the last
                // thing a session sends, so the session ends here.
                if session_is_exit(rt, session_id) {
                    close_session(rt, session_id);
                }
            } else {
                let addr = buf.as_ptr().as_ptr();
                let len = buf.len();
                let addr = unsafe { addr.add(off) };
                let _ = write(
                    rt,
                    stream_fd,
                    addr,
                    len - off,
                    OpLocal::SessionFrame {
                        session_id,
                        stream_fd,
                        buf,
                        off,
                    },
                    on_session_frame,
                );
            }
        }
        Err(e) => {
            tracing::warn!(%session_id, %e, "session stream write failed");
            close_session(rt, session_id);
        }
    }
}

/// True when the session's most recent frame is the deferred exit frame, which
/// is the one that must be followed by a close.
fn session_is_exit(rt: &Rt, session_id: u64) -> bool {
    {
        let s = rt.global();
        {
            s.attach_sessions
                .get(&session_id)
                .is_some_and(|s| s.exit_sent)
        }
    }
}

/// Which of a session's two in-flight ops a handle refers to.
#[derive(Clone, Copy)]
enum SessionOp {
    Read,
    Output,
}

/// Records (or clears) a session's in-flight op handle so `close_session` can
/// cancel it instead of closing an fd out from under it.
fn store_session_op(rt: &mut Rt, session_id: u64, which: SessionOp, op: Option<coio::Op>) {
    {
        let s = rt.global_mut();
        {
            if let Some(sess) = s.attach_sessions.get_mut(&session_id) {
                match which {
                    SessionOp::Read => sess.read_op = op,
                    SessionOp::Output => sess.output_op = op,
                }
            }
        }
    };
}

/// Reads client frames from the session's stream and dispatches them. Keeps
/// reading until the client disconnects (or a fatal frame error closes the
/// session).
fn arm_session_read(rt: &mut Rt, session_id: u64) {
    let stream_fd = rt
        .global()
        .attach_sessions
        .get(&session_id)
        .map(|s| s.stream_fd);
    let Some(stream_fd) = stream_fd else {
        return;
    };
    // A fixed-size scratch buffer would need slab-backed memory; the header is
    // only 3 bytes, so accumulate into the local and let `read` fill a pooled
    // buffer each time.
    let op = read(
        rt,
        stream_fd,
        3,
        OpLocal::SessionHeader {
            session_id,
            acc: Vec::new(),
        },
        on_session_header,
    );
    if let Ok(op) = op {
        store_session_op(rt, session_id, SessionOp::Read, Some(op));
    }
}

fn on_session_header(rt: &mut Rt, local: OpLocal, res: io::Result<Bytes>) {
    let OpLocal::SessionHeader {
        session_id,
        mut acc,
    } = local
    else {
        return;
    };
    match res {
        Ok(b) if !b.is_empty() => acc.extend_from_slice(&b),
        Ok(_) => {
            tracing::debug!(%session_id, "stream read EOF, closing session");
            close_session(rt, session_id);
            return;
        }
        Err(e) => {
            tracing::debug!(%session_id, %e, "stream read error/EOF, closing session");
            close_session(rt, session_id);
            return;
        }
    }
    if acc.len() < 3 {
        // Short read: keep accumulating until the 3-byte header is complete.
        let stream_fd = rt
            .global()
            .attach_sessions
            .get(&session_id)
            .map(|s| s.stream_fd)
            .unwrap_or(-1);
        let op = read(
            rt,
            stream_fd,
            3,
            OpLocal::SessionHeader { session_id, acc },
            on_session_header,
        );
        if let Ok(op) = op {
            store_session_op(rt, session_id, SessionOp::Read, Some(op));
        }
        return;
    }

    let frame_type = acc[0];
    let frame_len = u16::from_le_bytes([acc[1], acc[2]]) as usize;
    let stream_fd = rt
        .global()
        .attach_sessions
        .get(&session_id)
        .map(|s| s.stream_fd)
        .unwrap_or(-1);
    store_session_op(rt, session_id, SessionOp::Read, None);
    let op = read(
        rt,
        stream_fd,
        frame_len.max(1),
        OpLocal::SessionPayload {
            session_id,
            frame_type,
            acc: Vec::new(),
            need: frame_len,
        },
        on_session_payload,
    );
    if let Ok(op) = op {
        store_session_op(rt, session_id, SessionOp::Read, Some(op));
    }
}

fn on_session_payload(rt: &mut Rt, local: OpLocal, res: io::Result<Bytes>) {
    let OpLocal::SessionPayload {
        session_id,
        frame_type,
        mut acc,
        need,
    } = local
    else {
        return;
    };
    match res {
        Ok(b) if !b.is_empty() => acc.extend_from_slice(&b),
        Ok(_) => {
            tracing::debug!(%session_id, "frame payload EOF");
            close_session(rt, session_id);
            return;
        }
        Err(e) => {
            tracing::debug!(%session_id, %e, "frame payload read error/EOF");
            close_session(rt, session_id);
            return;
        }
    }
    if acc.len() < need {
        // Short read: resume until the whole payload has arrived.
        let stream_fd = rt
            .global()
            .attach_sessions
            .get(&session_id)
            .map(|s| s.stream_fd)
            .unwrap_or(-1);
        let op = read(
            rt,
            stream_fd,
            (need - acc.len()).max(1),
            OpLocal::SessionPayload {
                session_id,
                frame_type,
                acc,
                need,
            },
            on_session_payload,
        );
        if let Ok(op) = op {
            store_session_op(rt, session_id, SessionOp::Read, Some(op));
        }
        return;
    }
    store_session_op(rt, session_id, SessionOp::Read, None);
    // A zero-length frame carries no payload; `write` needs a slab-backed
    // address, so use a one-byte pooled buffer with its length set to zero.
    let payload = if acc.is_empty() {
        let mut empty = pool_alloc(1);
        empty.set_len(0);
        empty.into_bytes()
    } else {
        let mut buf = pool_alloc(acc.len());
        buf.set_len(acc.len() as u32);
        buf.copy_from_slice(&acc);
        buf.into_bytes()
    };
    if !dispatch_frame(rt, session_id, frame_type, payload) {
        close_session(rt, session_id);
    } else {
        // Keep reading: the next header read is this loop's steady state.
        arm_session_read(rt, session_id);
    }
}

/// Dispatch one stream frame. Returns `false` when the session must close.
fn dispatch_frame(rt: &mut Rt, session_id: u64, frame_type: u8, payload: Bytes) -> bool {
    match frame_type {
        0x00 => {
            let request: Request = match serde_json::from_slice(&payload) {
                Ok(r) => r,
                Err(e) => {
                    tracing::error!(%session_id, %e, "invalid RunRequest JSON");
                    return false;
                }
            };
            match request {
                Request::Run { .. } => handle_run_attach(rt, session_id, request),
                Request::Attach { pid } => handle_attach(rt, session_id, pid),
                _ => {
                    tracing::error!(%session_id, "expected Run or Attach inside frame");
                    false
                }
            }
        }
        0x10 => {
            if !payload.is_empty() {
                let fd = match rt.global_mut().attach_sessions.get_mut(&session_id) {
                    Some(sess) => {
                        if sess.ptm_fd >= 0 {
                            sess.ptm_fd
                        } else {
                            sess.input_fd
                        }
                    }
                    None => -1,
                };
                if fd >= 0 {
                    arm_session_stdin(rt, session_id, fd, payload);
                }
            }
            true
        }
        0x11 => {
            {
                let s = rt.global_mut();
                {
                    if let Some(sess) = s.attach_sessions.get_mut(&session_id) {
                        let fd = if sess.ptm_fd >= 0 {
                            let fd = sess.ptm_fd;
                            sess.ptm_fd = -1;
                            fd
                        } else if sess.input_fd >= 0 {
                            let fd = sess.input_fd;
                            sess.input_fd = -1;
                            fd
                        } else {
                            -1
                        };
                        if fd >= 0 {
                            sys::close(fd);
                        }
                    }
                }
            };
            true
        }
        0x20 => {
            #[derive(serde::Deserialize)]
            struct WinSize {
                rows: u16,
                cols: u16,
            }
            match serde_json::from_slice::<WinSize>(&payload) {
                Ok(ws) => {
                    let ptm_fd = rt
                        .global()
                        .attach_sessions
                        .get(&session_id)
                        .map(|s| s.ptm_fd)
                        .unwrap_or(-1);
                    if ptm_fd >= 0 {
                        let mut w: libc::winsize = unsafe { std::mem::zeroed() };
                        w.ws_row = ws.rows;
                        w.ws_col = ws.cols;
                        let _ = unsafe { libc::ioctl(ptm_fd, libc::TIOCSWINSZ, &w) };
                    }
                    true
                }
                Err(e) => {
                    tracing::error!(%session_id, %e, "invalid WinSize JSON");
                    true
                }
            }
        }
        _ => {
            tracing::warn!(%session_id, %frame_type, "unknown frame type, closing");
            false
        }
    }
}

/// Writes client stdin bytes to the container, resuming after a short write.
fn arm_session_stdin(rt: &mut Rt, session_id: u64, fd: RawFd, buf: Bytes) {
    let addr = buf.as_ptr().as_ptr();
    let len = buf.len();
    let _ = write(
        rt,
        fd,
        addr,
        len,
        OpLocal::SessionStdin {
            session_id,
            buf,
            off: 0,
        },
        on_session_stdin,
    );
}

fn on_session_stdin(rt: &mut Rt, local: OpLocal, res: io::Result<usize>) {
    let OpLocal::SessionStdin {
        session_id,
        buf,
        off,
    } = local
    else {
        return;
    };
    match res {
        Ok(n) => {
            let off = off + n;
            if off < buf.len() {
                let len = buf.len();
                let addr = buf.as_ptr().as_ptr();
                let addr = unsafe { addr.add(off) };
                let _ = write(
                    rt,
                    rt.global()
                        .attach_sessions
                        .get(&session_id)
                        .map(|sess| {
                            if sess.ptm_fd >= 0 {
                                sess.ptm_fd
                            } else {
                                sess.input_fd
                            }
                        })
                        .unwrap_or(-1),
                    addr,
                    len - off,
                    OpLocal::SessionStdin {
                        session_id,
                        buf,
                        off,
                    },
                    on_session_stdin,
                );
            }
        }
        Err(e) => {
            tracing::warn!(%session_id, %e, "stdin write failed");
        }
    }
}

/// Run a container inside an attach session (0x00 run frame). The pty (for
/// `--tty`/`--interactive`) or log pipe lives in the session; the child is
/// inserted into the container map so the reaper can report its exit code.
/// Sends a `0x01` response frame and arms the session output drain.
fn handle_run_attach(rt: &mut Rt, session_id: u64, request: Request) -> bool {
    let args = match request {
        Request::Run {
            rootfs,
            net_pid,
            save,
            command,
            interactive,
            tty,
            no_init,
            init_path,
        } => RunArgs {
            rootfs,
            net_pid,
            save,
            command: CStringSerde::into_inner_vec(command),
            tty: tty.unwrap_or(false),
            interactive: interactive.unwrap_or(false),
            no_init,
            init_path,
        },
        _ => return false,
    };
    let Some(stream_fd) = rt
        .global()
        .attach_sessions
        .get(&session_id)
        .map(|s| s.stream_fd)
    else {
        return false;
    };

    // The error path writes synchronously (it is already tearing the session
    // down) and must not re-enter the frame writer.
    let err = |rt: &mut Rt, msg: &str| {
        let payload = serde_json::to_vec(&ErrorResponse {
            ok: false,
            error: msg.to_string(),
        })
        .unwrap();
        write_frame_now(stream_fd, 0x01, &payload);
        close_session(rt, session_id);
    };

    let use_pty = args.tty || args.interactive;
    let save = args.save;
    let prep = match prepare_run(args) {
        Ok(p) => p,
        Err(e) => {
            err(rt, &e);
            return false;
        }
    };

    let (master, mut slave) = if use_pty {
        match pty::open_pty() {
            Ok((m, s)) => (Some(m), Some(s)),
            Err(e) => {
                err(rt, &format!("pty allocation failed: {e}"));
                return false;
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
        err(rt, &format!("log pipe creation failed: {e}"));
        return false;
    }

    let clone_result = clone3_container(prep.clone_flags);
    match clone_result {
        Err(e) => {
            if pipe_fds.read >= 0 {
                sys::close(pipe_fds.read);
            }
            if pipe_fds.write >= 0 {
                sys::close(pipe_fds.write);
            }
            if let Some(dp) = &prep.death_pipe {
                sys::close(dp.read);
                sys::close(dp.write);
            }
            err(rt, &format!("clone3 failed: {e}"));
            false
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
            }
            if let Err(e) = prep.signal.wait() {
                tracing::error!(%e, "sync wait failed");
                std::process::exit(1);
            }
            let death_pipe_read = prep.death_pipe.map(|dp| {
                sys::close(dp.write);
                dp.read
            });
            child_init_environment(
                &prep.rootfs,
                &prep.overlay_dir,
                prep.command,
                death_pipe_read,
                prep.init_argv,
                prep.no_init,
            );
        }
        Ok(Some(pid)) => {
            drop(slave);

            let death_pipe_write = match prep.death_pipe {
                Some(dp) => {
                    sys::close(dp.read);
                    dp.write
                }
                None => -1,
            };

            if let Err(e) = parent_setup_maps_and_signal(pid, prep.needs_userns_maps, prep.signal) {
                if death_pipe_write >= 0 {
                    sys::close(death_pipe_write);
                }
                err(rt, &format!("container aborted: {e}"));
                return false;
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

            {
                let s = rt.global_mut();
                {
                    s.containers.insert(pid, ContainerInfo {
                        pid,
                        command: cmd_str,
                        overlay_dir: prep.overlay_dir,
                        save,
                        start_time: SystemTime::now(),
                        gateway: LogGateway::new(LOG_CAPACITY),
                        stdin_fd: -1,
                        ptm_fd: -1, // run_attach holds the ptm in the session
                        death_pipe_write,
                        drains_pending: 0,
                    });
                    if let Some(sess) = s.attach_sessions.get_mut(&session_id) {
                        sess.ptm_fd = ptm_fd;
                        sess.log_read_fd = pipe_fds.read;
                        sess.container_pid = pid;
                    }
                }
            };

            tracing::info!(%pid, %session_id, "attach container started");

            let payload = serde_json::to_vec(&RunResponse {
                ok: true,
                pid: Some(pid),
                error: None,
            })
            .unwrap();
            arm_session_frame(rt, session_id, stream_fd, 0x01, payload);
            arm_session_output(rt, session_id);

            true
        }
    }
}

/// Late-attach to an already-running container (0x00 attach frame): creates a
/// fresh follow pipe, registers its writer with the container's gateway (no
/// backlog, matching the old daemon), wires the session to it, sends a `0x01`
/// response frame, and arms the session output drain.
fn handle_attach(rt: &mut Rt, session_id: u64, pid: i32) -> bool {
    let pid = pid as pid_t;
    let (found, input_fd) = match rt.global().containers.get(&pid) {
        Some(info) => (
            true,
            if info.ptm_fd >= 0 {
                info.ptm_fd
            } else {
                info.stdin_fd
            },
        ),
        None => (false, -1),
    };
    if !found {
        // Written synchronously: `close_session` below closes the stream fd, so
        // an armed frame write would be cancelled along with the session.
        let payload = serde_json::to_vec(&ErrorResponse {
            ok: false,
            error: format!("container {pid} not found"),
        })
        .unwrap();
        if let Some(stream_fd) = session_stream_fd(rt, session_id) {
            write_frame_now(stream_fd, 0x01, &payload);
        }
        close_session(rt, session_id);
        return false;
    }

    let mut pipe_fds = sys::FdPair {
        read: -1,
        write: -1,
    };
    if let Err(e) = sys::pipe2(&mut pipe_fds, libc::O_CLOEXEC) {
        tracing::error!(%e, "attach follow pipe creation failed");
        close_session(rt, session_id);
        return false;
    }

    let attached = {
        let s = rt.global_mut();
        let pipe_id = s.next_pipe_id;
        s.next_pipe_id += 1;
        if let Some(info) = s.containers.get_mut(&pid) {
            info.gateway
                .pipes
                .push(AsyncPipeWriter::new(pipe_id, pipe_fds.write));
            if let Some(sess) = s.attach_sessions.get_mut(&session_id) {
                sess.log_read_fd = pipe_fds.read;
                sess.input_fd = input_fd;
                sess.container_pid = pid;
            }
            true
        } else {
            false
        }
    };
    if !attached {
        sys::close(pipe_fds.read);
        sys::close(pipe_fds.write);
        close_session(rt, session_id);
        return false;
    }

    let payload = serde_json::to_vec(&RunResponse {
        ok: true,
        pid: Some(pid),
        error: None,
    })
    .unwrap();
    let Some(stream_fd) = session_stream_fd(rt, session_id) else {
        close_session(rt, session_id);
        return false;
    };
    arm_session_frame(rt, session_id, stream_fd, 0x01, payload);
    arm_session_output(rt, session_id);

    true
}

/// Writes one frame straight to the session stream, bypassing the runtime.
///
/// Only for teardown paths: the caller is about to close the session, so an
/// armed write would be cancelled along with the fd it targets.
fn write_frame_now(stream_fd: RawFd, frame_type: u8, payload: &[u8]) {
    let mut frame = Vec::with_capacity(3 + payload.len());
    frame.push(frame_type);
    frame.extend_from_slice(&(payload.len() as u16).to_le_bytes());
    frame.extend_from_slice(payload);
    let _ = sys::write(stream_fd, &frame);
}

fn session_stream_fd(rt: &Rt, session_id: u64) -> Option<RawFd> {
    {
        let s = rt.global();
        s.attach_sessions.get(&session_id).map(|s| s.stream_fd)
    }
    .filter(|&fd| fd >= 0)
}

/// Drains the session's output (PTY or follow pipe) into `0x10` data frames.
///
/// The loop ends at EOF and records `drain_done`; the deferred `0x02` exit
/// frame is sent by [`maybe_finish_session`] once the reaper has also seen the
/// child exit, so every `0x10` frame is flushed before the exit frame.
fn arm_session_output(rt: &mut Rt, session_id: u64) {
    let fd = match rt.global_mut().attach_sessions.get(&session_id) {
        Some(sess) => {
            if sess.ptm_fd >= 0 {
                sess.ptm_fd
            } else {
                sess.log_read_fd
            }
        }
        None => -1,
    };
    if fd < 0 {
        // No output to drain (e.g. the client sent a 0x11 EOF frame that closed
        // the pty master before this loop sampled the session). The exit
        // handshake still has to run.
        finish_session_output(rt, session_id);
        return;
    };
    let op = read(
        rt,
        fd,
        4096,
        OpLocal::SessionOutputRead { session_id },
        on_session_output,
    );
    if let Ok(op) = op {
        store_session_op(rt, session_id, SessionOp::Output, Some(op));
    }
}

fn on_session_output(rt: &mut Rt, local: OpLocal, res: io::Result<Bytes>) {
    let OpLocal::SessionOutputRead { session_id } = local else {
        return;
    };
    store_session_op(rt, session_id, SessionOp::Output, None);
    let bytes = match res {
        Ok(b) if !b.is_empty() => b,
        Ok(_) => {
            finish_session_output(rt, session_id);
            return;
        }
        Err(e) => {
            tracing::warn!(%session_id, %e, "session output read failed");
            finish_session_output(rt, session_id);
            return;
        }
    };
    let Some(stream_fd) = session_stream_fd(rt, session_id) else {
        finish_session_output(rt, session_id);
        return;
    };
    // The frame header is not contiguous with the payload, so the two halves go
    // out as separate writes. The payload needs no copy: `write_fixed` can
    // address the provided region, so it is written straight from the slot the
    // read filled, which also keeps that slot out of the pool until the frame
    // is delivered.
    let mut header = pool_alloc(3);
    header.set_len(3);
    header[0] = 0x10;
    header[1..3].copy_from_slice(&(bytes.len() as u16).to_le_bytes());
    let addr = header.as_ptr().as_ptr();
    let op = write(
        rt,
        stream_fd,
        addr,
        3,
        OpLocal::SessionDataHeader {
            session_id,
            stream_fd,
            header: header.into_bytes(),
            payload: bytes,
            off: 0,
        },
        on_session_data_header,
    );
    if op.is_err() {
        finish_session_output(rt, session_id);
    }
}

/// The 3-byte header is on the wire; write the payload from the provided slot.
fn on_session_data_header(rt: &mut Rt, local: OpLocal, res: io::Result<usize>) {
    let OpLocal::SessionDataHeader {
        session_id,
        stream_fd,
        header,
        payload,
        off,
    } = local
    else {
        return;
    };
    match res {
        Ok(n) => {
            let off = off + n;
            if off < header.len() {
                // Short write: finish the header, then move on to the payload.
                let len = header.len();
                let addr = header.as_ptr().as_ptr();
                let addr = unsafe { addr.add(off) };
                let _ = write(
                    rt,
                    stream_fd,
                    addr,
                    len - off,
                    OpLocal::SessionDataHeader {
                        session_id,
                        stream_fd,
                        header,
                        payload,
                        off,
                    },
                    on_session_data_header,
                );
            } else {
                let len = payload.len();
                let addr = payload.as_ptr().as_ptr();
                let _ = write(
                    rt,
                    stream_fd,
                    addr,
                    len,
                    OpLocal::SessionDataPayload {
                        session_id,
                        stream_fd,
                        payload,
                        off: 0,
                    },
                    on_session_data_payload,
                );
            }
        }
        Err(e) => {
            tracing::warn!(%session_id, %e, "session frame header write failed");
            finish_session_output(rt, session_id);
        }
    }
}

/// The frame is on the wire; continue draining.
fn on_session_data_payload(rt: &mut Rt, local: OpLocal, res: io::Result<usize>) {
    let OpLocal::SessionDataPayload {
        session_id,
        stream_fd,
        payload,
        off,
    } = local
    else {
        return;
    };
    match res {
        Ok(n) => {
            let off = off + n;
            if off >= payload.len() {
                arm_session_output(rt, session_id);
            } else {
                let len = payload.len();
                let addr = payload.as_ptr().as_ptr();
                let addr = unsafe { addr.add(off) };
                let _ = write(
                    rt,
                    stream_fd,
                    addr,
                    len - off,
                    OpLocal::SessionDataPayload {
                        session_id,
                        stream_fd,
                        payload,
                        off,
                    },
                    on_session_data_payload,
                );
            }
        }
        Err(e) => {
            tracing::warn!(%session_id, %e, "session stream write failed");
            finish_session_output(rt, session_id);
        }
    }
}

/// The output drain is done; check whether the exit handshake can complete.
fn finish_session_output(rt: &mut Rt, session_id: u64) {
    {
        let s = rt.global_mut();
        {
            if let Some(sess) = s.attach_sessions.get_mut(&session_id) {
                sess.drain_done = true;
            }
        }
    };
    maybe_finish_session(rt, session_id);
}

// ── Tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::io::Read;

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
        c.push(b"aaaa"); // 5 bytes
        c.push(b"bbbb"); // 5 bytes, total 10
        c.push(b"cccc"); // 5 bytes, total 15 (1 byte free)
        c.push(b"dddd"); // need 5, avail 1 → evict "aaaa\n" → write "dddd\n"
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
        c.push(b"aaaa"); // bytes=5, end=5
        c.push(b"bbbb"); // bytes=10, end=10
        c.push(b"cccc"); // bytes=15, end=15, avail=1
        // evicts "aaaa\n" → bytes=10, start=5, then writes "dddd\n" wrapping
        c.push(b"dddd");
        let lines = c.collect_lines();
        assert_eq!(lines, &["bbbb", "cccc", "dddd"], "got {lines:?}");
        c.push(b"eeee");
        let lines = c.collect_lines();
        assert_eq!(lines, &["cccc", "dddd", "eeee"], "got {lines:?}");
    }

    // ── LogGateway unit tests (sync bookkeeping; the op-driven writes are
    //    exercised by the integration tests) ─────────────────────────────

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
    fn log_gateway_push_marks_idle_pipes_in_flight() {
        let mut g = LogGateway::new(4096);
        let (r1, w1) = make_pipe_pair();
        let (r2, w2) = make_pipe_pair();
        g.pipes.push(AsyncPipeWriter::new(1, w1));
        g.pipes.push(AsyncPipeWriter::new(2, w2));
        let ids = g.push(b"hello");
        assert_eq!(ids, &[1, 2]);
        assert!(g.pipes[0].in_flight);
        assert!(g.pipes[1].in_flight);
        assert_eq!(g.cache.snapshot(), b"hello\n");
        // A second push skips both in-flight pipes.
        let ids = g.push(b"world");
        assert!(ids.is_empty());
        let _ = unsafe { libc::close(r1) };
        let _ = unsafe { libc::close(r2) };
    }

    #[test]
    fn log_gateway_complete_write_removes_dead_pipe() {
        let (r, w) = make_pipe_pair();
        let _ = unsafe { libc::close(r) };
        let mut g = LogGateway::new(64);
        g.pipes.push(AsyncPipeWriter::new(1, w));
        let ids = g.push(b"hello");
        assert_eq!(ids, &[1]);
        g.complete_write(1, false); // write failed → pipe removed, fd closed
        assert!(g.pipes.is_empty());
        assert!(g.pipe_fd(1).is_none());
    }

    #[test]
    fn log_gateway_complete_write_unknown_id_does_nothing() {
        let mut g = LogGateway::new(64);
        let (r, w) = make_pipe_pair();
        g.pipes.push(AsyncPipeWriter::new(1, w));
        g.complete_write(999, true);
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

    // ── Session output task ────────────────────────────────────────────

    fn read_available(mut client: &std::os::unix::net::UnixStream) -> Vec<u8> {
        client
            .set_read_timeout(Some(std::time::Duration::from_millis(300)))
            .unwrap();
        let mut out = Vec::new();
        let mut tmp = [0u8; 256];
        loop {
            match client.read(&mut tmp) {
                Ok(0) => break,
                Ok(n) => out.extend_from_slice(&tmp[..n]),
                Err(_) => break,
            }
        }
        out
    }

    // Regression test: when a 0x11 EOF frame closes the pty master before the
    // output task samples the session, the task has no fd to drain (ptm_fd and
    // log_read_fd are both -1). It must still send the deferred 0x02 exit frame
    // and close the session, instead of returning early and leaking it.
    #[test]
    fn session_output_sends_exit_frame_when_no_output_fd() {
        let mut state = DaemonState::new(PathBuf::from("/tmp/x.sock"));

        let (daemon_end, client_end) = std::os::unix::net::UnixStream::pair().unwrap();
        let stream_fd = daemon_end.as_raw_fd();
        std::mem::forget(daemon_end); // keep the fd open; close_session closes it

        const SESSION_ID: u64 = 7;
        state.attach_sessions.insert(SESSION_ID, AttachSession {
            stream_fd,
            ptm_fd: -1, // closed by a 0x11 EOF frame before the loop sampled it
            input_fd: -1,
            log_read_fd: -1, // no follow/log pipe (pty run_attach)
            container_pid: 12345,
            child_exited: true,
            exit_code: Some(0),
            drain_done: false,
            exit_sent: false,
            read_op: None,
            output_op: None,
        });

        // The child has already been reaped, so the loop's immediate
        // `finish_session_output` is the last of the two preconditions and
        // releases the exit frame.
        block_on_default(state, |rt| arm_session_output(rt, SESSION_ID));

        let frames = read_available(&client_end);
        assert!(!frames.is_empty(), "expected a 0x02 exit frame, got none");
        assert_eq!(frames[0], 0x02);
        let payload_len = u16::from_le_bytes([frames[1], frames[2]]) as usize;
        assert_eq!(
            &frames[3..3 + payload_len],
            br#"{"exit_code":0}"#,
            "unexpected exit frame payload"
        );
    }
}
