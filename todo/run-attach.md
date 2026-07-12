# Run Attach — daemon-managed interactive/live containers

## Status: **IMPLEMENTED** (Jul 2026)

See `src/daemon.rs` (`AttachSession`, `handle_run_attach`,
`handle_accept`, `handle_stream_read`, `handle_stream_write`,
`handle_pty_read`, `handle_pty_write`, `dispatch_frame`,
`send_attach_frame`, `close_attach_session`, `reap_children` exit-code
path) and `src/main.rs` (`run_attach`, `build_frame`, `send_frame`,
`read_frame`, `sigwinch_handler`).

Non-detach `run` always goes through the daemon — `<socket>.stream`
must exist. The old inline `run_container`, `wait_for_child`, and
`relay_pty`/`relay_pty_output` have been removed.

---

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
    stream_fd: RawFd,
    ptm_fd: RawFd,             // PTY master, -1 if !tty && !interactive
    log_read_fd: RawFd,        // stdout/stderr pipe read end (non-PTY)
    child_pid: pid_t,
    child_exited: bool,
    reading_header: bool,       // two-phase frame read
    frame_buf: Vec<u8>,
    frame_type: u8,
    frame_len: u16,
    output_rbuf: Vec<u8>,      // buffer for PTY/pipe reads
    stream_wbuf: Vec<u8>,      // buffer for stream writes (data + exit frames)
    pty_write_pending: bool,   // PTY_WRITE in-flight
    stream_write_pending: bool, // STREAM_WRITE in-flight
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
  │  ║   stdin → 0x10 frames       ║   │  stream_read → PTY_write
  │  ║   0x10 frames → stdout      ║   │  PTY_read → stream_write
  │  ║   SIGWINCH → 0x20 frames    ║   │  ioctl(TIOCSWINSZ)
  │  ╚═════════════════════════════╝   │
  │                                    │
  │  (child exits → wait4)             │
  │◀─── 0x02 ExitCode ────────────────│
  │  (daemon closes stream)            │
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

  `push_accept` on `attach_listener_fd` with `ACCEPT` user_data.
  On completion the connected fd is inserted as a new `AttachSession`
  with all fields initialised, and `submit_stream_read_header` is called
  to start reading the first frame.

  The accept is then resubmitted for the next client.

### Frame reading (two-phase: header → payload)

`handle_stream_read` implements a state machine via `reading_header`:
1. `reading_header = true` → read 3 bytes (`type:u8`, `len:u16 LE`) into `frame_buf`
2. `reading_header = false` → read `frame_len` bytes of payload into `frame_buf`
3. On payload complete → `dispatch_frame`:
   - `0x00` → parse JSON as `Request::Run`, call `handle_run_attach`,
     which does NOT resubmit the stream read (rejected if sent again)
   - `0x10` → `push_write` to PTY master (`pty_write_pending = true`),
     stream read header resubmitted eagerly
   - `0x11` → close PTY master (EOF to child), stream read resubmitted
   - `0x20` → `ioctl(ptm_fd, TIOCSWINSZ, &ws)`, stream read resubmitted
   - other → error, session closed

### handle_run_attach

Standalone handler (not shared with `handle_run`):
- If `tty` or `interactive`: open PTY (`open_pty`), child uses slave,
  parent stores master as `ptm_fd`
- Otherwise: create `pipe2(O_CLOEXEC)`, child dup2 write end to
  `STDOUT_FILENO`/`STDERR_FILENO`, /dev/null for stdin, parent stores
  read end as `log_read_fd`
- `clone3_container` in child namespace; parent sets up user ns maps,
  signals child to proceed, sends `0x01 RunResponse`, then submits
  both `submit_output_read` and `submit_stream_read_header`

### Data relay (io_uring asymmetric flow)

Two directions with different flow-control strategies:

**Output (PTY/pipe → stream):** ping-pong backpressure.
```
PTY_READ > 0  →  send 0x10 frame via STREAM_WRITE
                  ↓ (NOT resubmitted here)
STREAM_WRITE completes  →  resubmit PTY_READ
```
This naturally stops reading PTY output when the client isn't reading.

**Input (stream → PTY):** eager (resubmit stream read immediately).
```
STREAM_READ frame = 0x10  →  submit PTY_WRITE to PTY master
                              ↓
                              resubmit STREAM_READ header
                              (no backpressure — small frames only)
```

When output hits EOF (ret ≤ 0): close `ptm_fd`/`log_read_fd`, and if
`child_exited` is already true, close the session.

### Child exit (SIGCHLD)

When `reap_children` picks up an attached pid:
1. Mark `child_exited = true` (do NOT close output fds — let the
   PTY_READ handler drain remaining data and close on natural EOF)
2. If no `stream_write_pending`: send `0x02 ExitCode` frame immediately
3. If `stream_write_pending`: defer — store payload in `stream_wbuf`,
   it will be flushed when `handle_stream_write` completes
4. When both `child_exited` and output fds are closed (by the PTY_READ
   EOF handler) and no PTY_WRITE is in-flight: `close_attach_session`

---

## CLI changes

`run_attach()` in `main.rs` replaces the inline `run_container` path
for non-detach `run`:

1. Connect to `<socket-path>.stream` via `UnixStream`
2. Send `0x00 RunRequest` frame (JSON `Request::Run`)
3. Read `0x01 RunResponse` (check `ok` field)
4. If `--tty` and stdin is a TTY: set raw mode, send initial `0x20`
   WinSize frame, install `SIGWINCH` handler (atomic bool flag)
5. If `--interactive` (but not `--tty`) and stdin is a TTY: disable
   echo only
6. Spawn reader thread: reads frames from stream, writes `0x10` Data
   to stdout, exits on `0x02` ExitCode
7. Main thread loops reading stdin, sending `0x10` Data frames;
   checks `WINCH_PENDING` atomic before each read; on stdin EOF
   sends `0x11` StdinEof
8. Restore terminal, join reader thread, exit with reader's code

---

## Files changed

| File | Changes |
|------|---------|
| `src/main.rs` | Add `run_attach()`, frame helpers (`build_frame`, `send_frame`, `read_frame`), SIGWINCH handler (atomic bool); `RunArgs` gains `tty`/`interactive` fields; remove `run_container`, `wait_for_child`, test `rootfs_nonexistent_returns_failure` |
| `src/daemon.rs` | `attach_listener_fd`, `attach_sessions`, `next_session_id`, `AttachSession` struct; user_data flags (`ACCEPT`, `STREAM_READ`, `STREAM_WRITE`, `PTY_READ`, `PTY_WRITE`, `SESSION_MASK`); accept + CQE dispatch for stream/PTY ops; frame dispatch (0x00–0x20); `handle_run_attach` with PTY/pipe/clone3; `send_attach_frame`, `submit_output_read`; relay handlers (`handle_pty_read/write`, `handle_stream_read/write`, `close_attach_session`); `reap_children` emits `0x02` for attached pids with deferred send |
| `src/pty.rs` | Remove `relay_pty` and `relay_pty_output` (superseded by daemon-side relay) |
| `tests/container.rs` | `Daemon` helper (per-test daemon in temp dir, SIGKILL on drop); `run_conrt` injects `--socket-path` after subcommand; `container_stdout` filters tracing lines; net_pid tests use `--detach` for PID |
