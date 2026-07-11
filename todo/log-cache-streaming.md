# Log Cache & Client Streaming

## Goals

- `logs` command returns buffered lines without draining
- Subscribe via pipe: client receives backlog + live output over an fd
- Always write to cache; write to pipe as well when subscribed
- Single contiguous `Vec<u8>` ring buffer — no per-line allocations
- All I/O via io_uring — no blocking syscalls in the event loop

---

## LogCache — Single-Buffer Ring

```
LogCache {
    buf: Vec<u8>,     // fixed capacity after init
    cap: usize,
    start: usize,     // byte offset of oldest byte
    end: usize,       // byte offset for next write
    bytes: usize,     // total bytes stored (disambiguates empty vs full)
}
```

**Internal format**: raw bytes delimited by `b'\n'`. No length prefix.

### Push

```rust
fn push(line: &[u8]) {
    let need = line.len() + 1; // +1 for trailing \n

    loop {
        let avail = self.cap - self.bytes;
        if avail >= need { break; }

        // Scan from start for first \n — drop that line.
        let mut i = self.start;
        loop {
            if self.buf[i] == b'\n' {
                let line_bytes = (i - self.start + 1) % self.cap;
                self.start = (self.start + line_bytes) % self.cap;
                self.bytes -= line_bytes;
                break;
            }
            i = (i + 1) % self.cap;
        }
    }

    for &b in line.iter().chain(std::iter::once(&b'\n')) {
        self.buf[self.end] = b;
        self.end = (self.end + 1) % self.cap;
    }
    self.bytes += need;
}
```

### Snapshot (for subscribe backlog)

```rust
fn snapshot(&self) -> Vec<u8> {
    // Iterates all cached lines + \n, copies into a contiguous Vec.
    // The caller submits this as one async write to the pipe.
}
```

### collect_lines (non-destructive, for Logs response)

```rust
fn collect_lines(&self) -> Vec<String> {
    // Same iteration as snapshot, but builds Strings.
}
```

Both share a common `scan` helper that yields `(&[u8], Option<&[u8]>)` per line.

---

## AsyncPipeWriter — io_uring-backed live writes

```
AsyncPipeWriter {
    fd: RawFd,                 // write end of pipe to client
    send_buf: Vec<u8>,         // fixed capacity, line + \n copied here
    in_flight: bool,
}
```

Uses `opcode::Write` (not SendMsg) — simpler for pipe output. The `send_buf` is copied once per live line, then submitted as an SQE. `in_flight` prevents reuse before CQE.

```rust
fn push_write(&mut self, sq: &mut SubmissionQueue, line: &[u8], id: u64) {
    if self.in_flight { return; }
    self.send_buf.clear();
    self.send_buf.extend_from_slice(line);
    self.send_buf.push(b'\n');
    uring::push_write(sq, self.fd, &self.send_buf, id);
    self.in_flight = true;
}

fn complete(&mut self, ret: i32) {
    self.in_flight = false;
    if ret < 0 {
        unsafe { libc::close(self.fd) };
        self.fd = -1;
    }
}
```

---

## SubscribeResponse — one-shot fd-pass buffer

```
SubscribeResponse {
    pipe_writer: RawFd,       // to close on error / drop
    pipe_reader: RawFd,       // to pass via SCM_RIGHTS
    backlog_buf: Vec<u8>,     // snapshot of cached lines, submitted as async write
    cmsg_buf: Vec<u8>,        // cmsghdr with SCM_RIGHTS
    iov: libc::iovec,
    msghdr: Box<libc::msghdr>,
}
```

### Subscribe flow (all async, no blocking)

```
handle_subscribe in daemon:

  1. pipe2(O_CLOEXEC) → (reader, writer)
  2. gateawy.cache.snapshot() → backlog_buf
  3. Build SubscribeResponse {
        pipe_writer: writer,
        pipe_reader: reader,
        backlog_buf,
        cmsg_buf: vec![0u8; 64],   // for SCM_RIGHTS
        iov,
        msghdr,
     }
  4. Submit backlog write:
       push_write(sq, writer, backlog_buf, BACKLOG_WRITE | pid)
  5. Don't submit fd-pass yet — wait for backlog CQE.
  6. Store SubscribeResponse in Daemon under a "pending fd pass" slot.
  7. Return (no response to client yet).
```

```
backlog write CQE handler (BACKLOG_WRITE | pid):

  1. If error → close pipe_writer + pipe_reader, send ErrorResponse via datagram, discard.
  2. Set up AsyncPipeWriter { fd: pipe_writer, send_buf, in_flight: false }
     in `container_info.gateway`.
  3. Build the fd-pass sendmsg:
       CMSG: SCM_RIGHTS with pipe_reader
       data: empty (fd arrival is the signal)
  4. Submit push_sendmsg(sq, datagram_fd, &msghdr, SUBSCRIBE_FD | pid)
  5. SubscribeResponse stays alive until fd-pass CQE.
```

```
fd-pass CQE handler (SUBSCRIBE_FD | pid):

  1. SubscribeResponse is no longer needed — free it.
  2. Subscription fully established.
```

---

## user_data encoding

```
0               datagram recvmsg
1               signalfd read
2+              output pipe reads (container stdout/stderr)

BACKLOG_WRITE = 1 << 63
PIPE_WRITE    = 1 << 62
SUBSCRIBE_FD  = 1 << 61

(BACKLOG_WRITE | pid)    backlog write to client pipe
(PIPE_WRITE    | pid)    live line write to client pipe
(SUBSCRIBE_FD  | pid)    SCM_RIGHTS fd-pass to client
```

Dispatch:

```rust
match user_data {
    0 => self.handle_datagram_cqe(ret),
    1 => self.handle_signal(ret),
    id if id & SUBSCRIBE_FD != 0 => self.complete_subscribe(id & !SUBSCRIBE_FD, ret),
    id if id & BACKLOG_WRITE != 0 => self.complete_backlog_write(id & !BACKLOG_WRITE, ret),
    id if id & PIPE_WRITE != 0 => self.complete_pipe_write(id & !PIPE_WRITE, ret),
    id => self.handle_output(id, ret),
}
```

---

## LogGateway

```
LogGateway {
    cache: LogCache,
    pipe: Option<AsyncPipeWriter>,
}
```

```rust
impl LogGateway {
    fn write(&mut self, sq: &mut SubmissionQueue, line: &[u8], pipe_id: u64) {
        self.cache.push(line);
        if let Some(pipe) = &mut self.pipe {
            if !pipe.in_flight {
                pipe.push_write(sq, line, pipe_id);
            }
        }
    }

    fn complete_write(&mut self, ret: i32) {
        if let Some(pipe) = &mut self.pipe {
            pipe.complete(ret);
            if pipe.fd < 0 {
                self.pipe = None;
            }
        }
    }
}
```

---

## ContainerInfo changes

```
struct ContainerInfo {
    pid: pid_t,
    command: String,
    overlay_dir: Option<PathBuf>,
    save: bool,
    start_time: SystemTime,
-   bbq: Churrasco<LOG_CAPACITY>,
+   gateway: LogGateway,
}
```

## Daemon changes

New field:

```
subscribe_pend: HashMap<pid_t, Box<SubscribeResponse>>,
```

Keyed by pid, holds the in-flight fd-pass buffer until the backlog write completes (then fd-pass is submitted).

```
- log_graveyard: HashMap<pid_t, Churrasco<LOG_CAPACITY>>,
+ log_graveyard: HashMap<pid_t, LogCache>,
```

---

## Wire protocol

### Request::Logs

```rust
pub enum Request {
    Logs {
        pid: i32,
        #[serde(default)]
        stream: bool,
    },
}
```

### Response

`stream: false` — non-destructive dump:

```
LogsResponse { lines: Vec<String> }
```

`stream: true` — daemon passes the pipe reader fd via `SCM_RIGHTS`. No JSON payload — the fd itself is the response. Client reads fd: first backlog lines, then live lines.

Error (unknown PID):

```
ErrorResponse { ok: false, error: "..." }   // plain datagram
```

---

## Event flow

### subscribe (stream: true)

```
client                              daemon
  │                                   │
  │  Logs{pid, stream:true}           │
  │──────────────────────────────────▶│
  │                                   │
  │  pipe2(O_CLOEXEC)                 │
  │  cache.snapshot() → backlog_buf   │← memcpy, no syscall
  │                                   │
  │  push_write(pipe_writer,          │← async: backlog to pipe
  │    backlog_buf, BACKLOG|pid)      │
  │                                   │
  │  (backlog write CQE)              │
  │    gatewy.pipe = AsyncPipeWriter  │← live streaming active
  │    push_sendmsg(datagram_fd,      │← async: fd-pass to client
  │      SCM_RIGHTS reader, SUB|pid)  │
  │                                   │
  │  (fd-pass CQE)                    │
  │    free SubscribeResponse         │← fully established
  │                                   │
  │ receives fd ◀─────────────────────│
  │                                   │
  │  for each new log line:           │
  │    cache.push(line)               │← always
  │    if pipe && !in_flight          │
  │      push_write(pipe, line)       │← async pipe write
  │                                   │
  │ reads pipe fd ◀───────────────────│
```

Lines that arrive between subscribe and backlog write completion go to cache. If no live pipe yet (AsyncPipeWriter not set up), they just sit in cache. After backlog CQE, if any lines arrived during that window, they're in the cache but won't be sent to the pipe until the next new line triggers a write. This is acceptable — those lines are still retrievable via `Logs { stream: false }`, and the next live line flush will (if we want) also flush pending cache additions.

### Logs (non-destructive)

```
client                              daemon
  │                                   │
  │  Logs{pid, stream:false}          │
  │──────────────────────────────────▶│
  │  cache.collect_lines()            │← non-destructive
  │  LogsResponse { lines }           │
  │◀──────────────────────────────────│
```

### Pipe failure (live writes)

```
CQE for push_write returns EPIPE
  → pipe.complete(epipe) → fd = -1, pipe = None
  → future writes go to cache only
```

---

## handle_output rewrite

Current:

```
if let Some(info) = self.containers.get_mut(&output.pid) {
    // BBQ push
} else if let Some(bbq) = self.log_graveyard.get_mut(&output.pid) {
    // BBQ push
}
```

New:

```
if let Some(info) = self.containers.get_mut(&output.pid) {
    info.gateway.write(sq, line, PIPE_WRITE | pid as u64)
} else if let Some(cache) = self.log_graveyard.get_mut(&output.pid) {
    cache.push(line)
}
```

---

## reap_children changes

Current:

```
let bbq = std::mem::replace(&mut info.bbq, Churrasco::new());
self.log_graveyard.insert(pid, bbq);
```

New:

```
if let Some(pipe) = &mut info.gateway.pipe {
    let _ = unsafe { libc::close(pipe.fd) };
}
self.log_graveyard.insert(pid, std::mem::replace(
    &mut info.gateway.cache, LogCache::new(CACHE_CAPACITY)));
```

---

## Removals

- `bbqueue` dependency from `Cargo.toml`
- `drain_bbq` function
- `Churrasco` import
- `LOG_CAPACITY` constant

---

## Files changed

| File | Changes |
|------|---------|
| `Cargo.toml` | Remove `bbqueue` |
| `src/daemon.rs` | `LogCache`, `AsyncPipeWriter`, `SubscribeResponse`, `LogGateway` structs + impls; modify `ContainerInfo`, `Daemon`, `handle_output`, `handle_logs`, `reap_children`, `Request::Logs`; add `Request::Logs { stream: bool }`, subscribe handler, `subscribe_pend`, backlog/fd-pass/pipe CQE handlers; remove `drain_bbq`, `Churrasco`, `bbqueue` |
| `src/uring.rs` | Remove `#![allow(dead_code)]` from `push_sendmsg` (now used) |
