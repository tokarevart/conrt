use std::cell::RefCell;
use std::collections::HashMap;
use std::io;
use std::os::fd::AsRawFd;
use std::os::fd::RawFd;
use std::os::unix::net::UnixDatagram;
use std::path::Path;
use std::path::PathBuf;
use std::rc::Rc;
use std::time::SystemTime;

use coio::Bytes;
use coio::BytesMut;
use coio::Msg;
use coio::MsgMut;
use coio::Notify;
use coio::accept;
use coio::alloc_bytes;
use coio::io::write_all;
use coio::read;
use coio::read_exact;
use coio::recvmsg;
use coio::runtime::RuntimeContext;
use coio::runtime::block_on_default;
use coio::sendmsg;
use coio::task::TaskContext;
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
}

// ── TaskKinds: coio spawn user_data ───────────────────────────────────────
//
// One uniform `async move` dispatches on this; each variant maps to one
// self-contained async task. Every persistent task keeps an io op in flight
// (recvmsg / accept / sigchld read / output read) so the runtime loop never
// sees `has_io_in_flight() == false` while the daemon should run.

enum TaskKind {
    /// The bootstrap task: binds sockets, then spawns the persistent tasks.
    Main,
    /// Serves datagram requests (run/list/kill/logs/follow) over the control
    /// socket.
    Datagram,
    /// Accepts Unix-stream attach clients.
    Accept,
    /// Reaps dead containers from the SIGCHLD signalfd.
    SignalReaper,
    /// Drains one container's stdout/stderr pipe into its log gateway.
    ContainerOutput { pid: pid_t },
    /// Drains a detached TTY container's pty master into its log gateway (the
    /// pipe drain above captures stdout/stderr; this captures tty echo).
    ContainerPtyOutput { pid: pid_t },
    /// Reads client frames from an attach session's stream and forwards stdin
    /// data/EOF/win-size to the container.
    SessionRead { session_id: u64 },
    /// Drains an attach session's output (PTY or follow pipe) into `0x10`
    /// data frames, then sends the `0x02` exit frame and closes the session.
    SessionOutput { session_id: u64 },
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
// The coio port keeps the cache and pipe bookkeeping synchronous; the actual
// pipe writes happen in the async container-output task via `coio::io::write`.
// `push` marks each idle pipe in-flight so at most one write is outstanding
// per pipe at a time (matching the old AsyncPipeWriter contract), and returns
// the ids the caller must write to.

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
    /// Returns the ids of the pipes the caller must `write` to (no borrow of
    /// the gateway may span the await, so the write happens outside).
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
    /// Number of output-drain tasks still running for this container (the
    /// stdout/stderr drain and, for `--tty`, the pty-echo drain). The container
    /// is kept in `containers` until the count reaches zero, so the drains can
    /// flush their remaining output to the follow pipes before cleanup closes
    /// them. Attach-only containers have no drain tasks and are cleaned up by
    /// the reaper directly.
    drains_pending: u32,
}

/// One container's stdout/stderr pipe being drained asynchronously. The read
/// buffer and line assembly live in the `ContainerOutput` task's locals; the
/// pty echo for detached TTY containers lives in `ContainerPtyOutput`.
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
}

/// Cross-task signal channels for one session.
struct SessionNotify {
    /// Fired by the reaper once the session's container is reaped. The value
    /// is stored in the notify, so the output task observes the exit even if
    /// it only waits after the reaper ran.
    child_exit: Notify<()>,
}

// ── Follow (fd-pass) ───────────────────────────────────────────────────────
//
// A `Logs { follow: true }` request runs entirely inside the datagram task: it
// snapshots the backlog into an owned `Vec`, writes it synchronously to a
// fresh pipe (the backlog is at most the cache capacity, which fits the default
// pipe capacity, and no await happens during the write so no line can slip in
// between the snapshot and the pipe being attached), then passes the read end
// to the client with SCM_RIGHTS via an async `sendmsg`. The gateway pipe is
// registered only after the backlog write, so live output then flows to the
// pipe through the normal container-output task.

// ── DaemonState: shared, borrow-disciplined ────────────────────────────────

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
    session_notify: HashMap<u64, SessionNotify>,
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
            session_notify: HashMap::new(),
            next_session_id: 0,
        }
    }
}

// ── Daemon entry point ────────────────────────────────────────────────────

pub struct Daemon {
    socket_path: PathBuf,
}

impl Daemon {
    pub fn new(socket_path: PathBuf) -> Self {
        Self { socket_path }
    }

    pub fn run(&mut self) -> io::Result<()> {
        let state = Rc::new(RefCell::new(DaemonState::new(self.socket_path.clone())));

        // `make_fut` is the single spawn entry point: every task (main and
        // spawned) is built by calling it, so the closure captures the shared
        // state once and hands each task its own owned `Rc` clone. The returned
        // future is `'static` because it owns the clone; the closure itself may
        // borrow the stack `state` since `block_on` does not require `S` to be
        // `'static`. `run()` returns only once the runtime drains, i.e. when
        // every persistent task's io has completed (at process exit).
        block_on_default(
            |ctx: TaskContext, rt: RuntimeContext<TaskKind, ()>, kind: TaskKind| {
                let state = Rc::clone(&state);
                async move { dispatcher(ctx, rt, kind, state).await }
            },
            TaskKind::Main,
        );
        Ok(())
    }
}

/// Dispatches one spawned task to its handler. `rt` is passed to every task
/// (the runtime hands it to each spawned closure) so session tasks can spawn
/// their output siblings.
async fn dispatcher(
    ctx: TaskContext,
    rt: RuntimeContext<TaskKind, ()>,
    kind: TaskKind,
    state: Rc<RefCell<DaemonState>>,
) {
    match kind {
        TaskKind::Main => main_task(ctx, rt, &state).await,
        TaskKind::Datagram => datagram_task(ctx, rt, &state).await,
        TaskKind::Accept => accept_task(ctx, rt, &state).await,
        TaskKind::SignalReaper => reaper_task(ctx, &state).await,
        TaskKind::ContainerOutput { pid } => container_output_task(ctx, &state, pid).await,
        TaskKind::ContainerPtyOutput { pid } => container_pty_output_task(ctx, &state, pid).await,
        TaskKind::SessionRead { session_id } => {
            session_read_task(ctx, rt, &state, session_id).await
        }
        TaskKind::SessionOutput { session_id } => {
            session_output_task(ctx, &state, session_id).await
        }
    }
}

/// The bootstrap task: bind the sockets, arm the SIGCHLD signalfd, store the
/// fds in the shared state, then spawn the persistent tasks. Returns once the
/// daemon's background io is all up; the runtime stays alive while any io is
/// in flight, and `run()` only returns when everything drains.
async fn main_task(
    ctx: TaskContext,
    rt: RuntimeContext<TaskKind, ()>,
    state: &Rc<RefCell<DaemonState>>,
) {
    let socket_path = state.borrow().socket_path.clone();
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
        let mut s = state.borrow_mut();
        s.datagram_fd = datagram_fd;
        s.attach_listener_fd = attach_listener_fd;
        s.sigchld_fd = sigchld_fd;
    }

    // Spawn the persistent tasks. Each keeps an io op in flight, so the
    // runtime keeps running.
    for kind in [TaskKind::Datagram, TaskKind::Accept, TaskKind::SignalReaper] {
        match rt.spawn(kind) {
            Some(_handle) => {}
            None => tracing::error!("task slab full, failed to spawn task"),
        }
    }

    let _ = ctx;
}

// ── Signal reaper ─────────────────────────────────────────────────────────

async fn reaper_task(ctx: TaskContext, state: &Rc<RefCell<DaemonState>>) {
    let sigchld_fd = state.borrow().sigchld_fd;
    loop {
        // One 128-byte `signalfd_siginfo` record; the read future owns its own
        // pooled buffer, so nothing must be borrowed across the await.
        match coio::io::read(ctx, sigchld_fd, 128).await {
            Ok(bytes) if !bytes.is_empty() => reap_children(state),
            Ok(_) => {}
            Err(e) => {
                tracing::warn!(%e, "sigchld read failed, reaper exiting");
                break;
            }
        }
    }
}

fn reap_children(state: &Rc<RefCell<DaemonState>>) {
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

                let session_id = {
                    let s = state.borrow();
                    match s
                        .attach_sessions
                        .iter()
                        .find_map(|(&id, sess)| (sess.container_pid == pid).then_some(id))
                    {
                        Some(id) => Some(id),
                        None => {
                            if !s.containers.contains_key(&pid) {
                                continue;
                            }
                            None
                        }
                    }
                };

                if let Some(sid) = session_id {
                    // Notify the session's output task: the child exited and
                    // the exit code is ready. The output task sends the 0x02
                    // frame once its output drain hits EOF, so all 0x10 data
                    // frames are flushed before the exit frame.
                    {
                        let mut s = state.borrow_mut();
                        if let Some(sess) = s.attach_sessions.get_mut(&sid) {
                            sess.child_exited = true;
                            sess.exit_code = Some(exit_code);
                        }
                        if let Some(notify) = s.session_notify.get(&sid) {
                            notify.child_exit.notify(());
                        }
                    }
                }

                // Remove the container: move its log cache to the graveyard,
                // close its fds and follow pipes, clean up the overlay. If the
                // container still has output drains running (detached
                // containers finish draining asynchronously after exit), defer
                // the cleanup to `finish_drain` so the follow pipes stay open
                // until the last of the buffered output has been delivered.
                let removed = {
                    let mut s = state.borrow_mut();
                    let Some(info) = s.containers.get(&pid) else {
                        continue;
                    };
                    if info.drains_pending > 0 {
                        continue;
                    }
                    let Some(mut info) = s.containers.remove(&pid) else {
                        continue;
                    };
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
                    info.gateway.close_all_pipes();
                    info
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

    let argv = sys::Argv::new(command);
    let errno = execvp(argv.as_slice());
    tracing::error!(%errno, "execvp failed");
    std::process::exit(1)
}

// ── Async request handlers ──────────────────────────────────────────────────

async fn datagram_task(
    ctx: TaskContext,
    rt: RuntimeContext<TaskKind, ()>,
    state: &Rc<RefCell<DaemonState>>,
) {
    let datagram_fd = state.borrow().datagram_fd;

    let mut recv = MsgMut::new().expect("recvmsg slot");
    let mut recv_buf = vec![0u8; 65536];
    recv.push_iov(libc::iovec {
        iov_base: recv_buf.as_mut_ptr() as *mut libc::c_void,
        iov_len: recv_buf.len(),
    });
    let mut sender: libc::sockaddr_un = unsafe { std::mem::zeroed() };

    loop {
        // Offer the sender-address buffer; the kernel fills it on recvmsg.
        {
            let msg = recv.msg();
            msg.msg_name = (&mut sender as *mut libc::sockaddr_un).cast();
            msg.msg_namelen = std::mem::size_of::<libc::sockaddr_un>() as libc::socklen_t;
        }
        let n = match recvmsg(ctx, datagram_fd, &mut recv, 0).await {
            Ok(n) => n,
            Err(e) => {
                tracing::warn!(%e, "datagram recv failed");
                continue;
            }
        };
        if n == 0 {
            continue;
        }
        let sender_len = recv.msg().msg_namelen;

        let request: Request = match serde_json::from_slice(&recv_buf[..n]) {
            Ok(r) => r,
            Err(e) => {
                reply_datagram(datagram_fd, &sender, sender_len, &ErrorResponse {
                    ok: false,
                    error: format!("invalid request: {e}"),
                });
                continue;
            }
        };

        handle_datagram(ctx, rt, state, datagram_fd, &sender, sender_len, request).await;
    }
}

async fn handle_datagram(
    ctx: TaskContext,
    rt: RuntimeContext<TaskKind, ()>,
    state: &Rc<RefCell<DaemonState>>,
    datagram_fd: RawFd,
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
        } => handle_run(rt, state, datagram_fd, sender, sender_len, RunArgs {
            rootfs,
            net_pid,
            save,
            command: CStringSerde::into_inner_vec(command),
            tty: tty.unwrap_or(false),
            interactive: interactive.unwrap_or(false),
        }),
        Request::List => {
            let now = SystemTime::now();
            let containers: Vec<ContainerSummary> = state
                .borrow()
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
            reply_datagram(datagram_fd, sender, sender_len, &ListResponse {
                containers,
            });
        }
        Request::Kill { pid } => {
            let pid = pid as pid_t;
            if !state.borrow().containers.contains_key(&pid) {
                reply_datagram(datagram_fd, sender, sender_len, &KillResponse {
                    ok: false,
                    error: Some(format!("container {pid} not found")),
                });
                return;
            }
            let ret = unsafe { libc::kill(pid, libc::SIGKILL) };
            if ret < 0 {
                let e = io::Error::last_os_error();
                reply_datagram(datagram_fd, sender, sender_len, &KillResponse {
                    ok: false,
                    error: Some(format!("kill failed: {e}")),
                });
                return;
            }
            tracing::info!(%pid, "sent SIGKILL");
            reply_datagram(datagram_fd, sender, sender_len, &KillResponse {
                ok: true,
                error: None,
            });
        }
        Request::Logs { pid, follow } => {
            let pid = pid as pid_t;
            if follow {
                handle_follow(ctx, state, datagram_fd, sender, sender_len, pid).await;
            } else {
                let lines = {
                    let mut s = state.borrow_mut();
                    if let Some(info) = s.containers.get_mut(&pid) {
                        info.gateway.collect_lines()
                    } else if let Some(cache) = s.log_graveyard.get_mut(&pid) {
                        cache.collect_lines()
                    } else {
                        reply_datagram(datagram_fd, sender, sender_len, &ErrorResponse {
                            ok: false,
                            error: format!("container {pid} not found"),
                        });
                        return;
                    }
                };
                reply_datagram(datagram_fd, sender, sender_len, &LogsResponse { lines });
            }
        }
        Request::Attach { .. } => {
            reply_datagram(datagram_fd, sender, sender_len, &ErrorResponse {
                ok: false,
                error: "attach must be sent over the stream socket".into(),
            });
        }
    }
}

fn reply_datagram(
    datagram_fd: RawFd,
    sender: &libc::sockaddr_un,
    sender_len: libc::socklen_t,
    resp: &impl Serialize,
) {
    let data = serde_json::to_vec(resp).unwrap();
    if let Err(e) = send_datagram_raw(datagram_fd, sender, sender_len, &data) {
        tracing::error!(%e, "reply sendto failed");
    }
}

/// The `Logs { follow: true }` path. Runs entirely in the datagram task: the
/// backlog is snapshotted and written to a fresh pipe synchronously (the
/// snapshot is at most the cache capacity, which fits the default pipe
/// capacity, and no await happens between the snapshot and the write, so no
/// other task can push a line into the gap) — no lock is needed. The read end
/// is then passed to the client with an async SCM_RIGHTS `sendmsg`, and the
/// pipe writer is attached to the container's gateway so live output flows to
/// it through the normal container-output task.
async fn handle_follow(
    ctx: TaskContext,
    state: &Rc<RefCell<DaemonState>>,
    datagram_fd: RawFd,
    sender: &libc::sockaddr_un,
    sender_len: libc::socklen_t,
    pid: pid_t,
) {
    tracing::debug!(%pid, "handle_follow");

    let backlog = match state.borrow_mut().containers.get_mut(&pid) {
        Some(info) => info.gateway.snapshot(),
        None => {
            reply_datagram(datagram_fd, sender, sender_len, &ErrorResponse {
                ok: false,
                error: format!("container {pid} not found"),
            });
            return;
        }
    };

    let mut pipe_fds = sys::FdPair {
        read: -1,
        write: -1,
    };
    if let Err(e) = sys::pipe2(&mut pipe_fds, libc::O_CLOEXEC) {
        tracing::error!(%e, "follow pipe creation failed");
        reply_datagram(datagram_fd, sender, sender_len, &ErrorResponse {
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

    {
        let mut s = state.borrow_mut();
        let pipe_id = s.next_pipe_id;
        s.next_pipe_id += 1;
        let Some(info) = s.containers.get_mut(&pid) else {
            let _ = unsafe { libc::close(pipe_fds.write) };
            let _ = unsafe { libc::close(pipe_fds.read) };
            return;
        };
        info.gateway
            .pipes
            .push(AsyncPipeWriter::new(pipe_id, pipe_fds.write));
    }

    // Pass the read end to the client with SCM_RIGHTS.
    let mut slot = Msg::new().expect("sendmsg slot");
    slot.push_scm_rights(&[pipe_fds.read]);
    {
        let msg = slot.msg();
        msg.msg_name = (sender as *const libc::sockaddr_un as *mut libc::c_void).cast();
        msg.msg_namelen = sender_len;
    }
    match sendmsg(ctx, datagram_fd, &mut slot).await {
        Ok(_) => {}
        Err(e) => tracing::error!(%e, "fd-pass sendmsg failed"),
    }
    let _ = unsafe { libc::close(pipe_fds.read) };
}

/// Run a container in detached mode. Fully synchronous except for the spawns:
/// stdout/stderr are captured from a pipe and drained by the
/// `ContainerOutput` task (and, for `--tty`, the pty echo by
/// `ContainerPtyOutput`). The caller receives a `RunResponse` datagram.
fn handle_run(
    rt: RuntimeContext<TaskKind, ()>,
    state: &Rc<RefCell<DaemonState>>,
    datagram_fd: RawFd,
    sender: &libc::sockaddr_un,
    sender_len: libc::socklen_t,
    args: RunArgs,
) {
    let err = |msg: &str| {
        reply_datagram(datagram_fd, sender, sender_len, &ErrorResponse {
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
            err(&e);
            return;
        }
    };

    let mut stdin_pipe_fds = sys::FdPair {
        read: -1,
        write: -1,
    };
    if use_stdin_pipe && let Err(e) = sys::pipe2(&mut stdin_pipe_fds, libc::O_CLOEXEC) {
        err(&format!("stdin pipe creation failed: {e}"));
        return;
    }

    let (pty_master, pty_slave) = if use_pty {
        match pty::open_pty() {
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
    if let Err(e) = sys::pipe2(&mut pipe_fds, libc::O_CLOEXEC) {
        err(&format!("log pipe creation failed: {e}"));
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
            err(&format!("clone3 failed: {e}"));
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
            child_init_environment(&prep.rootfs, &prep.overlay_dir, prep.command);
        }
        Ok(Some(pid)) => {
            // Parent.
            sys::close(pipe_fds.write);
            drop(pty_slave);

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
                err(&format!("container aborted: {e}"));
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
                let mut s = state.borrow_mut();
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
                    drains_pending: 0,
                });
            }

            match rt.spawn(TaskKind::ContainerOutput { pid }) {
                Some(_) => {
                    let mut s = state.borrow_mut();
                    if let Some(info) = s.containers.get_mut(&pid) {
                        info.drains_pending += 1;
                    }
                }
                None => tracing::error!("task slab full, failed to spawn container output task"),
            }
            if has_pty {
                match rt.spawn(TaskKind::ContainerPtyOutput { pid }) {
                    Some(_) => {
                        let mut s = state.borrow_mut();
                        if let Some(info) = s.containers.get_mut(&pid) {
                            info.drains_pending += 1;
                        }
                    }
                    None => tracing::error!("task slab full, failed to spawn pty output task"),
                }
            }

            tracing::info!(%pid, "container started");
            reply_datagram(datagram_fd, sender, sender_len, &RunResponse {
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

/// Push `lines` into the container's gateway (or the graveyard cache if the
/// container was already reaped), writing each to its idle follow pipes. All
/// awaits happen without holding a borrow on the shared state. A line's
/// in-flight mark is held across all of its chunks, so the next line can never
/// interleave with it on the same pipe.
async fn write_lines(
    ctx: TaskContext,
    state: &Rc<RefCell<DaemonState>>,
    pid: pid_t,
    lines: Vec<Vec<u8>>,
) {
    for line in lines {
        let pending = {
            let mut s = state.borrow_mut();
            match s.containers.get_mut(&pid) {
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
            }
        };
        if pending.is_empty() {
            continue;
        }
        let chunks = split_line(&line);
        for (id, fd) in pending {
            let mut ok = true;
            for chunk in &chunks {
                let view = chunk.sub(0, chunk.len()).expect("line chunk in bounds");
                if let Err(e) = write_all(ctx, fd, view).await {
                    tracing::warn!(%e, "follow pipe write failed");
                    ok = false;
                    break;
                }
            }
            {
                let mut s = state.borrow_mut();
                if let Some(info) = s.containers.get_mut(&pid) {
                    info.gateway.complete_write(id, ok);
                }
            }
        }
    }
}

/// Decrements a container's drain counter. When the last output/pty drain task
/// finishes, removes the container, moves its log cache to the graveyard,
/// closes its fds and follow pipes, and cleans up its overlay. No-op when the
/// container is already gone (reaper cleaned it up, or it had no drain tasks).
fn finish_drain(state: &Rc<RefCell<DaemonState>>, pid: pid_t) {
    let removed = {
        let mut s = state.borrow_mut();
        let Some(info) = s.containers.get_mut(&pid) else {
            return;
        };
        info.drains_pending = info.drains_pending.saturating_sub(1);
        if info.drains_pending > 0 {
            return;
        }
        let Some(mut info) = s.containers.remove(&pid) else {
            return;
        };
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
/// buffer and line assembly live in this task's locals; the gateway and pipe
/// bookkeeping are touched only inside short synchronous borrows.
async fn container_output_task(ctx: TaskContext, state: &Rc<RefCell<DaemonState>>, pid: pid_t) {
    let fd = match state.borrow().outputs.get(&pid) {
        Some(o) => o.fd,
        None => {
            tracing::warn!(%pid, "container output task: no output entry");
            finish_drain(state, pid);
            return;
        }
    };
    let mut line_buf: Vec<u8> = Vec::new();
    loop {
        let bytes = match read(ctx, fd, 4096).await {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!(%pid, %e, "output read failed, closing output");
                break;
            }
        };
        if bytes.is_empty() {
            break;
        }
        let mut lines = Vec::new();
        drain_lines(&mut line_buf, &bytes, &mut lines);
        if !lines.is_empty() {
            write_lines(ctx, state, pid, lines).await;
        }
    }
    // EOF: drop the output entry, close the pipe, and finish this drain.
    {
        let mut s = state.borrow_mut();
        if let Some(o) = s.outputs.remove(&pid)
            && o.fd >= 0
        {
            sys::close(o.fd);
        }
    }
    finish_drain(state, pid);
}

/// Drains a detached TTY container's pty master echo into its log gateway.
/// The pipe drain captures stdout/stderr; this captures the terminal echo.
async fn container_pty_output_task(ctx: TaskContext, state: &Rc<RefCell<DaemonState>>, pid: pid_t) {
    let ptm_fd = match state.borrow().containers.get(&pid) {
        Some(info) => info.ptm_fd,
        None => {
            finish_drain(state, pid);
            return;
        }
    };
    if ptm_fd < 0 {
        finish_drain(state, pid);
        return;
    }
    let mut line_buf: Vec<u8> = Vec::new();
    loop {
        let bytes = match read(ctx, ptm_fd, 4096).await {
            Ok(b) if !b.is_empty() => b,
            Ok(_) => break,
            Err(e) => {
                tracing::warn!(%pid, %e, "pty output read failed");
                break;
            }
        };
        let mut lines = Vec::new();
        drain_lines(&mut line_buf, &bytes, &mut lines);
        if !lines.is_empty() {
            write_lines(ctx, state, pid, lines).await;
        }
    }
    // EOF/error: close the pty master, clear it on the container, and finish
    // this drain.
    {
        let mut s = state.borrow_mut();
        if let Some(info) = s.containers.get_mut(&pid)
            && info.ptm_fd >= 0
        {
            sys::close(info.ptm_fd);
            info.ptm_fd = -1;
        }
    }
    finish_drain(state, pid);
}

// ── Accept + attach sessions ────────────────────────────────────────────────

async fn accept_task(
    ctx: TaskContext,
    rt: RuntimeContext<TaskKind, ()>,
    state: &Rc<RefCell<DaemonState>>,
) {
    let listener_fd = state.borrow().attach_listener_fd;
    loop {
        let stream_fd = match accept(ctx, listener_fd).await {
            Ok(fd) => fd,
            Err(e) => {
                tracing::warn!(%e, "accept failed, retrying");
                continue;
            }
        };
        let session_id = {
            let mut s = state.borrow_mut();
            let id = s.next_session_id;
            s.next_session_id += 1;
            s.attach_sessions.insert(id, AttachSession {
                stream_fd,
                ptm_fd: -1,
                input_fd: -1,
                log_read_fd: -1,
                container_pid: 0,
                child_exited: false,
                exit_code: None,
            });
            s.session_notify.insert(id, SessionNotify {
                child_exit: Notify::new(),
            });
            id
        };
        match rt.spawn(TaskKind::SessionRead { session_id }) {
            Some(_) => {}
            None => {
                tracing::error!("task slab full, closing attach session");
                let mut s = state.borrow_mut();
                s.attach_sessions.remove(&session_id);
                s.session_notify.remove(&session_id);
                sys::close(stream_fd);
            }
        }
    }
}

fn close_session(state: &Rc<RefCell<DaemonState>>, session_id: u64) {
    let mut s = state.borrow_mut();
    if let Some(sess) = s.attach_sessions.remove(&session_id) {
        tracing::info!(%session_id, "closing attach session");
        // Wake the session's output task if it is waiting on the child exit,
        // so it can stop even when the client disconnects before the reaper.
        if let Some(notify) = s.session_notify.get(&session_id) {
            notify.child_exit.notify(());
        }
        s.session_notify.remove(&session_id);
        if sess.stream_fd >= 0 {
            sys::close(sess.stream_fd);
        }
        if sess.ptm_fd >= 0 {
            sys::close(sess.ptm_fd);
        }
        if sess.log_read_fd >= 0 {
            sys::close(sess.log_read_fd);
        }
        if sess.input_fd >= 0 {
            sys::close(sess.input_fd);
        }
    }
}

/// Reads client frames from the session's stream and dispatches them. Keeps
/// reading until the client disconnects (or a fatal frame error closes the
/// session).
async fn session_read_task(
    ctx: TaskContext,
    rt: RuntimeContext<TaskKind, ()>,
    state: &Rc<RefCell<DaemonState>>,
    session_id: u64,
) {
    let stream_fd = match state.borrow().attach_sessions.get(&session_id) {
        Some(s) => s.stream_fd,
        None => return,
    };
    loop {
        // 3-byte header: frame type + u16 payload length (LE).
        let header = match read_exact(ctx, stream_fd, 3).await {
            Ok(b) => b,
            Err(e) => {
                tracing::debug!(%session_id, %e, "stream read error/EOF, closing session");
                close_session(state, session_id);
                return;
            }
        };
        let frame_type = header[0];
        let frame_len = u16::from_le_bytes([header[1], header[2]]) as usize;
        if frame_len == 0 {
            let empty = pool_alloc(1);
            let mut empty = empty;
            empty.set_len(0);
            if !dispatch_frame(ctx, rt, state, session_id, frame_type, empty.into_bytes()).await {
                close_session(state, session_id);
                return;
            }
            continue;
        }
        let payload = match read_exact(ctx, stream_fd, frame_len).await {
            Ok(b) => b,
            Err(e) => {
                tracing::debug!(%session_id, %e, "frame payload read error/EOF");
                close_session(state, session_id);
                return;
            }
        };
        if !dispatch_frame(ctx, rt, state, session_id, frame_type, payload).await {
            close_session(state, session_id);
            return;
        }
    }
}

/// Dispatch one stream frame. Returns `false` when the session must close.
async fn dispatch_frame(
    ctx: TaskContext,
    rt: RuntimeContext<TaskKind, ()>,
    state: &Rc<RefCell<DaemonState>>,
    session_id: u64,
    frame_type: u8,
    payload: Bytes,
) -> bool {
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
                Request::Run { .. } => handle_run_attach(ctx, rt, state, session_id, request).await,
                Request::Attach { pid } => handle_attach(ctx, rt, state, session_id, pid).await,
                _ => {
                    tracing::error!(%session_id, "expected Run or Attach inside frame");
                    false
                }
            }
        }
        0x10 => {
            if !payload.is_empty() {
                let fd = {
                    let s = state.borrow();
                    match s.attach_sessions.get(&session_id) {
                        Some(sess) => {
                            if sess.ptm_fd >= 0 {
                                sess.ptm_fd
                            } else {
                                sess.input_fd
                            }
                        }
                        None => -1,
                    }
                };
                if fd >= 0
                    && let Err(e) = write_all(ctx, fd, payload).await
                {
                    tracing::warn!(%session_id, %e, "stdin write failed");
                }
            }
            true
        }
        0x11 => {
            {
                let mut s = state.borrow_mut();
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
                    let ptm_fd = state
                        .borrow()
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

/// Send one framed message (type + u16 length + payload) on the session's
/// stream. Resolves the stream fd first so no borrow spans the await.
async fn send_attach_frame(
    ctx: TaskContext,
    state: &Rc<RefCell<DaemonState>>,
    session_id: u64,
    frame_type: u8,
    payload: &[u8],
) -> io::Result<()> {
    let stream_fd = state
        .borrow()
        .attach_sessions
        .get(&session_id)
        .map(|s| s.stream_fd)
        .unwrap_or(-1);
    if stream_fd < 0 {
        return Err(io::Error::new(io::ErrorKind::BrokenPipe, "session closed"));
    }
    let len = 3 + payload.len();
    let mut buf = pool_alloc(len);
    buf.set_len(len as u32);
    buf[0] = frame_type;
    buf[1..3].copy_from_slice(&(payload.len() as u16).to_le_bytes());
    buf[3..].copy_from_slice(payload);
    write_all(ctx, stream_fd, buf.into_bytes()).await
}

/// Send a `0x10` data frame carrying one read buffer.
async fn send_data_frame(
    ctx: TaskContext,
    state: &Rc<RefCell<DaemonState>>,
    session_id: u64,
    data: &Bytes,
) -> io::Result<()> {
    let stream_fd = state
        .borrow()
        .attach_sessions
        .get(&session_id)
        .map(|s| s.stream_fd)
        .unwrap_or(-1);
    if stream_fd < 0 {
        return Err(io::Error::new(io::ErrorKind::BrokenPipe, "session closed"));
    }
    let len = 3 + data.len();
    let mut buf = pool_alloc(len);
    buf.set_len(len as u32);
    buf[0] = 0x10;
    buf[1..3].copy_from_slice(&(data.len() as u16).to_le_bytes());
    buf[3..].copy_from_slice(data);
    write_all(ctx, stream_fd, buf.into_bytes()).await
}

/// Run a container inside an attach session (0x00 run frame). The pty (for
/// `--tty`/`--interactive`) or log pipe lives in the session; the child is
/// inserted into the container map so the reaper can report its exit code.
/// Sends a `0x01` response frame and spawns the `SessionOutput` task.
async fn handle_run_attach(
    ctx: TaskContext,
    rt: RuntimeContext<TaskKind, ()>,
    state: &Rc<RefCell<DaemonState>>,
    session_id: u64,
    request: Request,
) -> bool {
    let args = match request {
        Request::Run {
            rootfs,
            net_pid,
            save,
            command,
            interactive,
            tty,
        } => RunArgs {
            rootfs,
            net_pid,
            save,
            command: CStringSerde::into_inner_vec(command),
            tty: tty.unwrap_or(false),
            interactive: interactive.unwrap_or(false),
        },
        _ => return false,
    };
    let Some(stream_fd) = state
        .borrow()
        .attach_sessions
        .get(&session_id)
        .map(|s| s.stream_fd)
    else {
        return false;
    };

    let err = |msg: &str| {
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
        close_session(state, session_id);
    };

    let use_pty = args.tty || args.interactive;
    let save = args.save;
    let prep = match prepare_run(args) {
        Ok(p) => p,
        Err(e) => {
            err(&e);
            return false;
        }
    };

    let (master, mut slave) = if use_pty {
        match pty::open_pty() {
            Ok((m, s)) => (Some(m), Some(s)),
            Err(e) => {
                err(&format!("pty allocation failed: {e}"));
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
        err(&format!("log pipe creation failed: {e}"));
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
            err(&format!("clone3 failed: {e}"));
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
            child_init_environment(&prep.rootfs, &prep.overlay_dir, prep.command);
        }
        Ok(Some(pid)) => {
            drop(slave);

            if let Err(e) = parent_setup_maps_and_signal(pid, prep.needs_userns_maps, prep.signal) {
                err(&format!("container aborted: {e}"));
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
                let mut s = state.borrow_mut();
                s.containers.insert(pid, ContainerInfo {
                    pid,
                    command: cmd_str,
                    overlay_dir: prep.overlay_dir,
                    save,
                    start_time: SystemTime::now(),
                    gateway: LogGateway::new(LOG_CAPACITY),
                    stdin_fd: -1,
                    ptm_fd: -1, // run_attach holds the ptm in the session
                    drains_pending: 0,
                });
                if let Some(sess) = s.attach_sessions.get_mut(&session_id) {
                    sess.ptm_fd = ptm_fd;
                    sess.log_read_fd = pipe_fds.read;
                    sess.container_pid = pid;
                }
            }

            tracing::info!(%pid, %session_id, "attach container started");

            let payload = serde_json::to_vec(&RunResponse {
                ok: true,
                pid: Some(pid),
                error: None,
            })
            .unwrap();
            let _ = send_attach_frame(ctx, state, session_id, 0x01, &payload).await;

            match rt.spawn(TaskKind::SessionOutput { session_id }) {
                Some(_) => {}
                None => tracing::error!("task slab full, failed to spawn session output task"),
            }

            true
        }
    }
}

/// Late-attach to an already-running container (0x00 attach frame): creates a
/// fresh follow pipe, registers its writer with the container's gateway (no
/// backlog, matching the old daemon), wires the session to it, sends a `0x01`
/// response frame, and spawns the `SessionOutput` task.
async fn handle_attach(
    ctx: TaskContext,
    rt: RuntimeContext<TaskKind, ()>,
    state: &Rc<RefCell<DaemonState>>,
    session_id: u64,
    pid: i32,
) -> bool {
    let pid = pid as pid_t;
    let (found, input_fd) = {
        let s = state.borrow();
        match s.containers.get(&pid) {
            Some(info) => (
                true,
                if info.ptm_fd >= 0 {
                    info.ptm_fd
                } else {
                    info.stdin_fd
                },
            ),
            None => (false, -1),
        }
    };
    if !found {
        let payload = serde_json::to_vec(&ErrorResponse {
            ok: false,
            error: format!("container {pid} not found"),
        })
        .unwrap();
        let _ = send_attach_frame(ctx, state, session_id, 0x01, &payload).await;
        close_session(state, session_id);
        return false;
    }

    let mut pipe_fds = sys::FdPair {
        read: -1,
        write: -1,
    };
    if let Err(e) = sys::pipe2(&mut pipe_fds, libc::O_CLOEXEC) {
        tracing::error!(%e, "attach follow pipe creation failed");
        close_session(state, session_id);
        return false;
    }

    {
        let mut s = state.borrow_mut();
        let pipe_id = s.next_pipe_id;
        s.next_pipe_id += 1;
        let Some(info) = s.containers.get_mut(&pid) else {
            sys::close(pipe_fds.read);
            sys::close(pipe_fds.write);
            close_session(state, session_id);
            return false;
        };
        info.gateway
            .pipes
            .push(AsyncPipeWriter::new(pipe_id, pipe_fds.write));
    }
    {
        let mut s = state.borrow_mut();
        if let Some(sess) = s.attach_sessions.get_mut(&session_id) {
            sess.log_read_fd = pipe_fds.read;
            sess.input_fd = input_fd;
            sess.container_pid = pid;
        }
    }

    let payload = serde_json::to_vec(&RunResponse {
        ok: true,
        pid: Some(pid),
        error: None,
    })
    .unwrap();
    let _ = send_attach_frame(ctx, state, session_id, 0x01, &payload).await;

    match rt.spawn(TaskKind::SessionOutput { session_id }) {
        Some(_) => {}
        None => tracing::error!("task slab full, failed to spawn session output task"),
    }

    true
}

/// Drains the session's output (PTY or follow pipe) into `0x10` data frames,
/// then — once the drain hits EOF and the child has been reaped — sends the
/// deferred `0x02` exit frame and closes the session.
async fn session_output_task(ctx: TaskContext, state: &Rc<RefCell<DaemonState>>, session_id: u64) {
    let fd = {
        let s = state.borrow();
        match s.attach_sessions.get(&session_id) {
            Some(sess) => {
                if sess.ptm_fd >= 0 {
                    sess.ptm_fd
                } else {
                    sess.log_read_fd
                }
            }
            None => return,
        }
    };
    if fd < 0 {
        return;
    }
    let child_notify = match state.borrow().session_notify.get(&session_id) {
        Some(n) => n.child_exit.clone(),
        None => return,
    };

    // Drain output into 0x10 frames until EOF/error.
    loop {
        let bytes = match read(ctx, fd, 4096).await {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!(%session_id, %e, "session output read failed");
                break;
            }
        };
        if bytes.is_empty() {
            break;
        }
        let stream_fd = {
            let s = state.borrow();
            s.attach_sessions
                .get(&session_id)
                .map(|se| se.stream_fd)
                .unwrap_or(-1)
        };
        if stream_fd < 0 {
            break;
        }
        if let Err(e) = send_data_frame(ctx, state, session_id, &bytes).await {
            tracing::warn!(%session_id, %e, "session stream write failed");
            break;
        }
    }

    // Wait for the child to exit (fired by the reaper, or by close_session
    // when the client disconnected first), then send the deferred 0x02 frame.
    let exit_code = {
        let s = state.borrow();
        match s.attach_sessions.get(&session_id) {
            Some(sess) if sess.child_exited => sess.exit_code,
            Some(_) => None,
            None => return,
        }
    };
    if exit_code.is_none() {
        child_notify.wait(ctx).await;
    }
    let stream_fd = {
        let s = state.borrow();
        s.attach_sessions
            .get(&session_id)
            .map(|se| se.stream_fd)
            .unwrap_or(-1)
    };
    if stream_fd < 0 {
        return;
    }
    let exit_code = {
        let s = state.borrow();
        s.attach_sessions
            .get(&session_id)
            .and_then(|se| se.exit_code)
    };
    let payload =
        serde_json::to_vec(&serde_json::json!({ "exit_code": exit_code.unwrap_or(0) })).unwrap();
    let _ = send_attach_frame(ctx, state, session_id, 0x02, &payload).await;
    close_session(state, session_id);
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

    // ── LogGateway unit tests (sync bookkeeping; the async writes are
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
}
