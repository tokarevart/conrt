# Run Attach — daemon-managed interactive/live containers

## Problem

`conrt run` without `--detach` forks the container directly as a child of the
CLI process via `clone3` — the daemon never sees it. This means:

- Container not tracked by daemon
- `kill`, `logs`, `subscribe` don't work
- No log capture
- CLI becomes the reaper; if it dies, the container orphans

## Goal

Non-detach `run` goes through the daemon with **full feature parity**:
PTY, interactive stdin, terminal raw mode, SIGWINCH resize, exit code
propagation. The CLI experience is identical to the current inline path.

## Non-goals

- Change the detach path — stays as-is (datagram + subscribe)
- Change the subscribe path — stays as-is
- Expose PTY master fd to the client — PTY stays on the daemon

---

## New: Unix stream listener

The daemon opens a **second listening socket** alongside the existing datagram
socket, at `<socket-path>.stream`.

### Daemon state additions

```
Daemon {
    ...
    attach_listener_fd: RawFd,           // stream listener
    attach_sessions: HashMap<u64, AttachSession>,
    next_session_id: u64,
}
```

### AttachSession

```rust
struct AttachSession {
    stream_fd: RawFd,          // connected client stream
    ptm_fd: RawFd,             // PTY master, -1 if !tty && !interactive
    log_read_fd: RawFd,        // stdout/stderr pipe read, used if no PTY
    child_pid: pid_t,
    child_exited: bool,
    stream_rbuf: Vec<u8>,      // buffer for reading from stream
    stream_wbuf: Vec<u8>,      // buffer for writing to stream
    pty_rbuf: Vec<u8>,         // buffer for reading from PTY / log pipe
    pty_wbuf: Vec<u8>,         // buffer for writing to PTY master
}
```

If `--tty` is set: `ptm_fd` is the PTY master. The child uses the PTY slave
for stdin/stdout/stderr.  If `--tty` is false but `--interactive` is true, a
PTY is still used for the child (stdin needs it).  If neither: a regular pipe
captures stdout/stderr (same as detach), and stdin is `/dev/null`.

---

## Wire protocol (over the Unix stream)

Simple length-delimited framing: `[type:1][len:u16 LE][payload:len]`

| Byte | Type | Direction | Payload |
|------|------|-----------|---------|
| `0x00` | RunRequest | CLI→Daemon | JSON `Request::Run` |
| `0x01` | RunResponse | Daemon→CLI | JSON `RunResponse { ok, pid?, error? }` |
| `0x02` | ExitCode | Daemon→CLI | JSON `{ exit_code: N }` (then daemon closes) |
| `0x10` | Data | bidirectional | raw bytes (stdin or PTY output) |
| `0x11` | StdinEof | CLI→Daemon | empty payload (stdin closed) |
| `0x20` | WinSize | CLI→Daemon | JSON `{ rows: u16, cols: u16 }` |

### Flow

```
CLI (stream)                         daemon
  │                                    │
  │──── 0x00 RunRequest ──────────────▶│  accept, read frame
  │                                    │  parse Run, clone3, setup PTY/pipe
  │◀─── 0x01 RunResponse ─────────────│  PID sent back
  │                                    │
  │  ╔══ bidirectional relay ══════╗   │
  │  ║   stdin → 0x10 frames      ║   │  stream_read → PTY_write
  │  ║   0x10 frames → stdout     ║   │  PTY_read → stream_write
  │  ║   SIGWINCH → 0x20 frames   ║   │  ioctl(TIOCSWINSZ)
  │  ╚════════════════════════════╝   │
  │                                    │
  │  (child exits → wait4)            │
  │◀─── 0x02 ExitCode ────────────────│
  │  (daemon closes stream)           │
```

---

## CQE dispatch — new user_data flags

```
ACCEPT        = 1 << 60
STREAM_READ   = 1 << 59
STREAM_WRITE  = 1 << 58
PTY_READ      = 1 << 57
PTY_WRITE     = 1 << 56
```

Dispatch:
- `ACCEPT | listener_id` → accept handler
- `STREAM_READ | session_id` → `read_stream_frame(session_id, ret)`
- `STREAM_WRITE | session_id` → `completed_stream_write(session_id, ret)`
- `PTY_READ | session_id` → `read_pty_output(session_id, ret)`
- `PTY_WRITE | session_id` → `completed_pty_write(session_id, ret)`

---

## Event loop details

### Accept

```rust
fn handle_accept(&mut self, ret: i32) {
    // ret = connected fd
    let session_id = self.next_session_id;
    self.next_session_id += 1;
    self.attach_sessions.insert(session_id, AttachSession {
        stream_fd: ret,
        ptm_fd: -1,
        log_read_fd: -1,
        child_pid: 0,
        child_exited: false,
        stream_rbuf: vec![0u8; 4],        // read type + len first
        stream_wbuf: Vec::with_capacity(4096),
        pty_rbuf: vec![0u8; 4096],
        pty_wbuf: vec![0u8; 4096],
    });
    // Submit first read: read the 3-byte header (type + len)
    push_read(sq, stream_fd, &mut stream_rbuf, STREAM_READ | session_id);
}
```

### Frame reading (two-phase: header → payload)

1. Read 3 bytes (`type:u8`, `len:u16 LE`)
2. If `len > 0`: read `len` bytes into payload buffer
3. Dispatch by type:
   - `0x00` → parse JSON as `Request::Run`, call `handle_run_attach(session_id, args)`
   - `0x10` → submit `STREAM_WRITE | session_id` to PTY master
   - `0x11` → close stdin side of PTY (send EOF)
   - `0x20` → `ioctl(ptm_fd, TIOCSWINSZ, &ws)`

### handle_run_attach

Same as `handle_run` but:
- Creates PTY if `tty` or `interactive` (instead of just a pipe)
- Child: uses PTY slave fd for stdio instead of pipe
- Parent: stores `AttachSession` with `ptm_fd`, kicks off `PTY_READ` and `STREAM_READ` cycles

### Data relay (io_uring ping-pong)

Two independent async loops per session:

```
STREAM_READ ret > 0  →  copy to pty_wbuf → STREAM_WRITE to PTY
PTY_READ ret > 0     →  copy to stream_wbuf → PTY_WRITE to stream
```

Each side resubmits its read after the write CQE completes (flow control).

When one side hits EOF (ret ≤ 0), it shuts down that direction but keeps the
other direction alive until the pipe drains.

### Child exit (SIGCHLD)

When `reap_children` picks up an attached pid:
1. If the child hasn't already exited: mark `child_exited = true`
2. If there's a PTY: close `ptm_fd` (gives EOF on the output side)
3. Wait for pending writes to drain
4. Encode and send `0x02` exit frame
5. Close stream fd, remove session

---

## CLI changes

`run_container()` in `main.rs` is replaced with `run_attach()`:

```
fn run_attach(args: RunArgs) -> ExitCode {
    let stream = connect_to_daemon_stream(socket_path)?;

    // 1. Send RunRequest frame
    send_frame(stream, 0x00, json_request)?;

    // 2. Read RunResponse frame
    let resp = read_frame(stream);
    // expect 0x01, parse JSON, get pid

    // 3. If --tty: enter raw mode, install SIGWINCH handler
    if tty {
        enter_raw_mode()?;
        signal_hook(SIGWINCH, || send_winsz(stream));
    }

    // 4. Reader thread: stream → stdout
    let reader = thread::spawn(move || {
        loop {
            match read_frame(stream) {
                (0x10, data) => stdout.write(data),
                (0x02, exit_json) => break parse_exit(exit_json),
                _ => break,
            }
        }
    });

    // 5. Main thread: stdin → stream
    for line in stdin.bytes() {
        send_frame(stream, 0x10, &[line]);
    }
    send_frame(stream, 0x11, &[]);  // stdin EOF

    // 6. Wait for reader to get exit code, then exit with it
    let code = reader.join();
    process::exit(code);
}
```

Window resize is sent as:
```
send_frame(stream, 0x20, json_winsz);
```

---

## Files changed

| File | Changes |
|------|---------|
| `src/main.rs` | Replace `run_container()` with `run_attach()`; add stream connect, frame read/write, raw mode, SIGWINCH |
| `src/daemon.rs` | Add `attach_listener_fd`, `attach_sessions`, `next_session_id`; add `AttachSession` struct; add accept handler; add stream/PTY frame read/write CQE handlers; modify `handle_run` to support PTY attach variant; modify `reap_children` to emit exit frames for attached pids |
| `src/uring.rs` | May need `push_accept`, `push_connect` if not already present |

---

## Open questions

- Does `io-uring` crate expose `Accept` opcode? If not, `libc::accept` + `push_read` can work.
- Should `SIGWINCH` be delivered via a side channel (separate datagram socket) or inline on the stream? Inline with `0x20` frame is simplest.
- Flow control: if writes to the stream back up (client not reading), should we stop reading from the PTY? The ping-pong design naturally enforces this by resubmitting `PTY_READ` only after `STREAM_WRITE` completes.
