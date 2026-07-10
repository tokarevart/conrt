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

// user_data namespaces
const CLIENT_BASE: u64 = 0x0001_0000_0000_0000;
const STREAM_W_BASE: u64 = 0x0002_0000_0000_0000;

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
    ring: IoUring,
    sigchld_fd: RawFd,
    sigchld_buf: Vec<u8>,
    listener: Option<UnixListener>,
    socket_path: PathBuf,
    containers: HashMap<pid_t, ContainerInfo>,
    outputs: HashMap<u64, Output>,
    clients: HashMap<u64, Client>,
    stream_clients: HashMap<u64, (RawFd, pid_t)>,
    pending_writes: HashMap<u64, StreamWrite>,
    next_output_id: u64,
    next_client_id: u64,
    next_write_id: u64,
    next_stream_id_counter: u64,
    log_graveyard: HashMap<pid_t, Churrasco<LOG_CAPACITY>>,
}

/// One container stdout/stderr pipe being drained asynchronously.
struct Output {
    fd: RawFd,
    pid: pid_t,
    read_buf: Vec<u8>,
    line_buf: Vec<u8>,
}

/// One connected client mid-request, driven entirely by completions.
struct Client {
    fd: RawFd,
    len_buf: Vec<u8>,
    payload: Vec<u8>,
    state: ClientState,
}

enum ClientState {
    ReadingLen,
    ReadingPayload { total: usize, offset: usize },
    WritingResponse { buf: Vec<u8>, offset: usize },
}

/// An in-flight async write to a streaming log client.
struct StreamWrite {
    fd: RawFd,
    stream_id: u64,
    buf: Vec<u8>,
    offset: usize,
}

impl Daemon {
    pub fn new(socket_path: PathBuf) -> Self {
        let ring = IoUring::new(RING_SIZE).expect("failed to create io_uring");
        Self {
            ring,
            sigchld_fd: -1,
            sigchld_buf: Vec::new(),
            listener: None,
            socket_path,
            containers: HashMap::new(),
            outputs: HashMap::new(),
            clients: HashMap::new(),
            stream_clients: HashMap::new(),
            pending_writes: HashMap::new(),
            next_output_id: 2,
            next_client_id: 0,
            next_write_id: 0,
            next_stream_id_counter: 1,
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
        tracing::info!(path = %self.socket_path.display(), "daemon listening");

        let listener_fd = listener.as_raw_fd();
        let sigchld = setup_sigchld_fd()?;
        self.sigchld_fd = sigchld;
        self.sigchld_buf = vec![0u8; std::mem::size_of::<libc::signalfd_siginfo>()];
        self.listener = Some(listener);

        {
            let mut sq = self.ring.submission();
            uring::push_accept(&mut sq, listener_fd, 0);
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
                match user_data {
                    0 => self.handle_listener(ret),
                    1 => self.handle_signal(ret),
                    id if id < CLIENT_BASE => self.handle_output(id, ret),
                    id if id < STREAM_W_BASE => {
                        self.handle_client_completion(id - CLIENT_BASE, ret)
                    }
                    id => self.handle_write_completion(id - STREAM_W_BASE, ret),
                }
            }
        }
    }

    // ── Listener (POLL multi-shot, re-armed by the kernel) ────────────────

    fn handle_listener(&mut self, ret: i32) {
        if ret >= 0 {
            self.accept_client(ret);
        }
        let listener_fd = self.listener.as_ref().unwrap().as_raw_fd();
        let mut sq = self.ring.submission();
        uring::push_accept(&mut sq, listener_fd, 0);
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

    // ── Client lifecycle ──────────────────────────────────────────────────

    fn accept_client(&mut self, fd: RawFd) {
        let client_id = self.next_client_id;
        self.next_client_id += 1;
        let user_data = CLIENT_BASE + client_id;

        let len_buf = vec![0u8; 4];

        let client = Client {
            fd,
            len_buf,
            payload: Vec::new(),
            state: ClientState::ReadingLen,
        };
        self.clients.insert(user_data, client);

        {
            let mut sq = self.ring.submission();
            let client = self.clients.get_mut(&user_data).unwrap();
            uring::push_read(&mut sq, fd, &mut client.len_buf, user_data);
        }
    }

    fn handle_client_completion(&mut self, client_id: u64, result: i32) {
        let user_data = CLIENT_BASE + client_id;
        let client = match self.clients.remove(&user_data) {
            Some(c) => c,
            None => return,
        };

        if result < 0 {
            sys::close(client.fd);
            return;
        }

        match client.state {
            ClientState::ReadingLen => self.client_read_len(client, user_data, result),
            ClientState::ReadingPayload { total, offset } => {
                self.client_read_payload(client, user_data, result, total, offset)
            }
            ClientState::WritingResponse { offset, .. } => {
                self.client_write(client, user_data, result, offset)
            }
        }
    }

    fn client_read_len(&mut self, mut client: Client, user_data: u64, result: i32) {
        if result as usize != 4 {
            sys::close(client.fd);
            return;
        }
        let payload_len = u32::from_le_bytes([
            client.len_buf[0],
            client.len_buf[1],
            client.len_buf[2],
            client.len_buf[3],
        ]) as usize;

        let payload = vec![0u8; payload_len];
        client.payload = payload;
        client.state = ClientState::ReadingPayload {
            total: payload_len,
            offset: 0,
        };
        let fd = client.fd;
        self.clients.insert(user_data, client);

        {
            let mut sq = self.ring.submission();
            let client = self.clients.get_mut(&user_data).unwrap();
            uring::push_read(&mut sq, fd, &mut client.payload, user_data);
        }
    }

    fn client_read_payload(
        &mut self,
        mut client: Client,
        user_data: u64,
        result: i32,
        total: usize,
        offset: usize,
    ) {
        let n = result as usize;
        let new_offset = offset + n;
        if new_offset < total {
            let fd = client.fd;
            let offset = new_offset;
            client.state = ClientState::ReadingPayload {
                total,
                offset: new_offset,
            };
            self.clients.insert(user_data, client);
            {
                let mut sq = self.ring.submission();
                let client = self.clients.get_mut(&user_data).unwrap();
                uring::push_read(&mut sq, fd, &mut client.payload[offset..], user_data);
            }
            return;
        }

        // full payload arrived — parse and dispatch
        let request: Request = match serde_json::from_slice(&client.payload) {
            Ok(r) => r,
            Err(e) => {
                client.send_response(&ErrorResponse {
                    ok: false,
                    error: format!("invalid request: {e}"),
                });
                self.clients.insert(user_data, client);
                self.submit_client_write(user_data);
                return;
            }
        };

        match request {
            Request::Run {
                rootfs,
                net_pid,
                save,
                command,
            } => self.handle_run(&mut client, rootfs, net_pid, save, command),
            Request::List => self.handle_list(&mut client),
            Request::Kill { pid } => self.handle_kill(&mut client, pid),
            Request::Logs { pid } => self.handle_logs(&mut client, pid),
        }

        self.clients.insert(user_data, client);
        self.submit_client_write(user_data);
    }

    fn client_write(&mut self, mut client: Client, user_data: u64, result: i32, offset: usize) {
        if result < 0 {
            sys::close(client.fd);
            return;
        }
        let new_offset = offset + result as usize;

        let done = match client.state {
            ClientState::WritingResponse { ref buf, .. } => new_offset >= buf.len(),
            _ => true,
        };

        if done {
            sys::close(client.fd);
            return;
        }

        if let ClientState::WritingResponse { ref mut offset, .. } = client.state {
            *offset = new_offset;
        }
        let fd = client.fd;
        self.clients.insert(user_data, client);
        {
            let mut sq = self.ring.submission();
            let client = self.clients.get_mut(&user_data).unwrap();
            if let ClientState::WritingResponse { ref buf, offset } = client.state {
                uring::push_write(&mut sq, fd, &buf[offset..], user_data);
            }
        }
    }

    fn submit_client_write(&mut self, user_data: u64) {
        let fd = match self.clients.get(&user_data) {
            Some(c) => c.fd,
            None => return,
        };
        let mut sq = self.ring.submission();
        if let Some(client) = self.clients.get_mut(&user_data)
            && let ClientState::WritingResponse { ref buf, offset } = client.state
        {
            uring::push_write(&mut sq, fd, &buf[offset..], user_data);
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

        self.forward_to_stream_clients(pid);

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

    // ── Stream-client async writes ─────────────────────────────────────────

    fn handle_write_completion(&mut self, write_id: u64, result: i32) {
        let user_data = STREAM_W_BASE + write_id;
        let mut sw = match self.pending_writes.remove(&user_data) {
            Some(sw) => sw,
            None => return,
        };

        if result < 0 {
            self.cleanup_stream_client(sw.stream_id);
            return;
        }

        sw.offset += result as usize;
        if sw.offset < sw.buf.len() {
            let fd = sw.fd;
            let offset = sw.offset;
            self.pending_writes.insert(user_data, sw);
            {
                let mut sq = self.ring.submission();
                let sw = self.pending_writes.get_mut(&user_data).unwrap();
                uring::push_write(&mut sq, fd, &sw.buf[offset..], user_data);
            }
        }
    }

    fn forward_to_stream_clients(&mut self, pid: pid_t) {
        let has_clients = self.stream_clients.values().any(|(_, p)| *p == pid);
        if !has_clients {
            return;
        }

        let lines = if let Some(info) = self.containers.get(&pid) {
            drain_bbq(&info.bbq)
        } else if let Some(bbq) = self.log_graveyard.get(&pid) {
            drain_bbq(bbq)
        } else {
            return;
        };

        if lines.is_empty() {
            return;
        }

        let resp = LogsResponse { lines };
        let payload = serde_json::to_vec(&resp).unwrap();
        let len = payload.len() as u32;
        let header = len.to_le_bytes();

        let matching: Vec<(u64, RawFd)> = self
            .stream_clients
            .iter()
            .filter(|(_, (_, p))| *p == pid)
            .map(|(&sid, &(cfd, _))| (sid, cfd))
            .collect();

        for (sid, cfd) in matching {
            let mut buf = Vec::with_capacity(4 + payload.len());
            buf.extend_from_slice(&header);
            buf.extend_from_slice(&payload);

            let write_id = self.next_write_id;
            self.next_write_id += 1;
            let user_data = STREAM_W_BASE + write_id;

            let sw = StreamWrite {
                fd: cfd,
                stream_id: sid,
                buf,
                offset: 0,
            };
            self.pending_writes.insert(user_data, sw);
            {
                let mut sq = self.ring.submission();
                let sw = self.pending_writes.get_mut(&user_data).unwrap();
                uring::push_write(&mut sq, cfd, &sw.buf, user_data);
            }
        }
    }

    fn cleanup_stream_client(&mut self, id: u64) {
        if let Some((cfd, _)) = self.stream_clients.remove(&id) {
            sys::close(cfd);
        }
    }

    // ── Request handlers (set client response asynchronously) ──────────────

    fn handle_run(
        &mut self,
        client: &mut Client,
        rootfs: Option<String>,
        net_pid: Option<i32>,
        save: bool,
        command: Vec<String>,
    ) {
        let rootfs = match rootfs {
            Some(p) => match Path::new(&p).canonicalize() {
                Ok(path) => Some(path),
                Err(e) => {
                    client.send_response(&ErrorResponse {
                        ok: false,
                        error: format!("invalid rootfs path: {e}"),
                    });
                    return;
                }
            },
            None => None,
        };

        let overlay_dir = match rootfs {
            Some(_) => match crate::create_overlay_tempdir() {
                Ok(dir) => Some(dir),
                Err(e) => {
                    client.send_response(&ErrorResponse {
                        ok: false,
                        error: format!("cannot create overlay tempdir: {e}"),
                    });
                    return;
                }
            },
            None => None,
        };

        let signal = match interprocess::OneshotSignal::new() {
            Ok(s) => s,
            Err(e) => {
                client.send_response(&ErrorResponse {
                    ok: false,
                    error: format!("sync pipe creation failed: {e}"),
                });
                return;
            }
        };

        let mut pipe_fds = sys::FdPair {
            read: -1,
            write: -1,
        };
        if let Err(e) = sys::pipe2(&mut pipe_fds, libc::O_CLOEXEC) {
            client.send_response(&ErrorResponse {
                ok: false,
                error: format!("log pipe creation failed: {e}"),
            });
            return;
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
                client.send_response(&ErrorResponse {
                    ok: false,
                    error: format!("invalid command (null byte): {e}"),
                });
                return;
            }
        };

        let (clone_flags, needs_userns_maps) = match net_pid {
            Some(pid) => {
                let user_f = match std::fs::File::open(format!("/proc/{pid}/ns/user")) {
                    Ok(f) => f,
                    Err(e) => {
                        sys::close(pipe_fds.read);
                        sys::close(pipe_fds.write);
                        client.send_response(&ErrorResponse {
                            ok: false,
                            error: format!("cannot open pid {pid} user ns: {e}"),
                        });
                        return;
                    }
                };
                let net_f = match std::fs::File::open(format!("/proc/{pid}/ns/net")) {
                    Ok(f) => f,
                    Err(e) => {
                        sys::close(pipe_fds.read);
                        sys::close(pipe_fds.write);
                        client.send_response(&ErrorResponse {
                            ok: false,
                            error: format!("cannot open pid {pid} net ns: {e}"),
                        });
                        return;
                    }
                };
                if let Err(e) = sys::setns(user_f.as_raw_fd(), libc::CLONE_NEWUSER) {
                    sys::close(pipe_fds.read);
                    sys::close(pipe_fds.write);
                    client.send_response(&ErrorResponse {
                        ok: false,
                        error: format!("setns(CLONE_NEWUSER) into pid {pid} failed: {e}"),
                    });
                    return;
                }
                if let Err(e) = sys::setns(net_f.as_raw_fd(), libc::CLONE_NEWNET) {
                    sys::close(pipe_fds.read);
                    sys::close(pipe_fds.write);
                    client.send_response(&ErrorResponse {
                        ok: false,
                        error: format!("setns(CLONE_NEWNET) into pid {pid} failed: {e}"),
                    });
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
                client.send_response(&ErrorResponse {
                    ok: false,
                    error: format!("clone3 failed: {e}"),
                });
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

                let output_id = self.next_output_id;
                self.next_output_id += 1;

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
                    client.send_response(&ErrorResponse {
                        ok: false,
                        error: format!("container aborted: {e}"),
                    });
                    return;
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

                client.send_response(&RunResponse {
                    ok: true,
                    pid: Some(pid),
                    error: None,
                });
            }
        }
    }

    fn handle_list(&self, client: &mut Client) {
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

        client.send_response(&ListResponse { containers });
    }

    fn handle_kill(&self, client: &mut Client, pid: i32) {
        if !self.containers.contains_key(&(pid as pid_t)) {
            client.send_response(&KillResponse {
                ok: false,
                error: Some(format!("container {pid} not found")),
            });
            return;
        }

        let ret = unsafe { libc::kill(pid as pid_t, libc::SIGKILL) };
        if ret < 0 {
            let err = io::Error::last_os_error();
            client.send_response(&KillResponse {
                ok: false,
                error: Some(format!("kill failed: {err}")),
            });
            return;
        }

        tracing::info!(%pid, "sent SIGKILL");
        client.send_response(&KillResponse {
            ok: true,
            error: None,
        });
    }

    fn handle_logs(&mut self, client: &mut Client, pid: i32) {
        let pid = pid as pid_t;
        let in_containers = self.containers.contains_key(&pid);
        let initial = if let Some(info) = self.containers.get_mut(&pid) {
            drain_bbq(&info.bbq)
        } else if let Some(bbq) = self.log_graveyard.get_mut(&pid) {
            drain_bbq(bbq)
        } else {
            client.send_response(&ErrorResponse {
                ok: false,
                error: format!("container {pid} not found"),
            });
            return;
        };

        client.send_response(&LogsResponse { lines: initial });

        if in_containers {
            let client_fd = match sys::dup(client.fd) {
                Ok(fd) => fd,
                Err(_) => return,
            };
            let stream_id = self.next_stream_id();
            self.stream_clients.insert(stream_id, (client_fd, pid));
        }
    }

    fn reap_children(&mut self) {
        loop {
            let mut status: i32 = 0;
            match sys::wait4(-1, &mut status, libc::WNOHANG, None) {
                Ok(pid) if pid > 0 => {
                    tracing::info!(%pid, %status, "container exited");
                    if let Some(mut info) = self.containers.remove(&pid) {
                        let bbq = std::mem::replace(&mut info.bbq, Churrasco::new());
                        let to_remove: Vec<u64> = self
                            .stream_clients
                            .iter()
                            .filter(|(_, (_, p))| *p == pid)
                            .map(|(&sid, _)| sid)
                            .collect();
                        for sid in &to_remove {
                            if let Some((cfd, _)) = self.stream_clients.remove(sid) {
                                let lines = drain_bbq(&bbq);
                                if !lines.is_empty() {
                                    let resp = LogsResponse { lines };
                                    let payload = serde_json::to_vec(&resp).unwrap();
                                    let len = payload.len() as u32;
                                    let header = len.to_le_bytes();
                                    let mut buf = Vec::with_capacity(4 + payload.len());
                                    buf.extend_from_slice(&header);
                                    buf.extend_from_slice(&payload);
                                    let write_id = self.next_write_id;
                                    self.next_write_id += 1;
                                    let user_data = STREAM_W_BASE + write_id;
                                    let sw = StreamWrite {
                                        fd: cfd,
                                        stream_id: *sid,
                                        buf,
                                        offset: 0,
                                    };
                                    self.pending_writes.insert(user_data, sw);
                                    {
                                        let mut sq = self.ring.submission();
                                        let sw = self.pending_writes.get_mut(&user_data).unwrap();
                                        uring::push_write(&mut sq, cfd, &sw.buf, user_data);
                                    }
                                }
                                sys::close(cfd);
                            }
                        }
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

    fn next_stream_id(&mut self) -> u64 {
        let id = self.next_stream_id_counter;
        self.next_stream_id_counter += 1;
        id
    }
}

impl Client {
    fn send_response<T: Serialize>(&mut self, resp: &T) {
        let payload = serde_json::to_vec(resp).unwrap();
        let len = payload.len() as u32;
        let header = len.to_le_bytes();
        let mut buf = Vec::with_capacity(4 + payload.len());
        buf.extend_from_slice(&header);
        buf.extend_from_slice(&payload);
        self.state = ClientState::WritingResponse { buf, offset: 0 };
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
