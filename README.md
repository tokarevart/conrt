# conrt — A Minimal Container Runtime

conrt is a from-scratch, Docker-like container runtime built in Rust. It's a course project
for learning systems programming: Linux kernel interfaces (namespaces, mounts,
OverlayFS), process and memory management in Rust, and a few classic data structures.

## Architecture

conrt uses a **single daemon** process model:

```
┌──────────────────────────────────────────────────────────┐
│                    Daemon (host ns)                      │
│                                                          │
│  io_uring-based event loop                               │
│                                                          │
│  CQE sources:                                            │
│  ├── datagram listener (accept → handle client)          │
│  ├── stream listener (accept → AttachSession)            │
│  ├── client datagram fds (request / response)            │
│  ├── attach stream fds (frame protocol: 0x00–0x20)      │
│  ├── container PTY/pipe fds (read output)                │
│  ├── subscribe stream fds (write logs)                   │
│  └── signalfd (SIGCHLD → waitpid → reap + cleanup)      │
│                                                          │
│  loop: io_uring_submit_and_wait() → dispatch CQEs        │
│                                                          │
│  No threads. Single-threaded event loop.                 │
└──────────────────────────────────────────────────────────┘
```

Key points:
- **One daemon** manages N containers, not a parent process per container
- Single-threaded `io_uring`-based event loop
- Daemon handles all host-side teardown (overlay cleanup, etc.)
- Container stdout/stderr is buffered in a bbqueue ring buffer and can be streamed live to `conrt logs` clients
- Foreground `conrt run` (without `--detach`) now goes through the daemon via Unix stream attach (frame protocol)

## RingBuffer Data Structure

A concurrent, fixed-capacity ring buffer for container log storage.

**Memory layout** — single flat `Box<[u8]>`:

```
buf: [ cap=4 | 6 | "hello\n" | 4 | "food\n" | 10 | "abcdefgh\n" | ... ]

      pos 0   4   10          16  20        26  30             40    ...
```

- Each entry: `u32 LE length` prefix followed by `length` bytes of data, contiguous
- Head/tail are `AtomicUsize` byte positions that wrap circularly
- Writer at tail: write length, then data bytes (handles wrap-around)
- Reader at head: read length, advance past data
- On full: advance head (drop oldest entries) until space exists
- Writer accumulates raw PTY bytes and splits on `\n` before pushing

## Container stdout/stderr Flow

```
container process ──write()──► PTY slave
                                    │
                                    ▼
                              PTY master FD (daemon)
                                    │
                         io_uring IORING_OP_READ
                                    │
                         line-buffer until \n
                                    │
                         ┌──────────┼───────────┐
                         ▼          ▼          ▼
                    RingBuffer   caller stdout  (future: file)
                    (conrt logs)  (--attach)
```

## Phases

### Phase 1 — Container Runtime ✅

- `clone3` with `CLONE_NEWUSER | CLONE_NEWPID | CLONE_NEWNS | CLONE_NEWUTS | CLONE_NEWIPC`
- UID/GID maps via `/proc/<pid>/{uid,gid}_map`, child blocks on sync pipe until maps are written
- `chroot` into rootfs (bind-mount onto itself first — `pivot_root` requires init-ns `CAP_SYS_ADMIN`)
- Mount `/proc`, `/dev`
- PTY allocation (`-t` flag): `openpty` + `setsid` + `TIOCSCTTY` + `dup2` to 0/1/2, `poll` I/O relay
- `conrt run <cmd>` foreground (goes through daemon via stream-attach)

### Phase 2 — Daemon & OverlayFS ✅

- Unix-socket JSON daemon, `poll`/`signalfd` event loop → later `io_uring`
- `--detach` hands off to daemon, `conrt list`, `conrt kill`
- OverlayFS: lowerdir=`<rootfs>`, per-container upperdir+workdir, `--save`/`--rm`

### Phase 3 — Logging & Streaming ✅

- Fixed-capacity ring buffer (`Box<[u8]>`, length-prefixed entries, atomic head/tail)
- `io_uring` daemon event loop (CQEs for PTY reads, client I/O, signalfd)
- `conrt logs <pid>`: reads stored output; live streaming via bbqueue to concurrent clients

### Phase 4 — Network ✅

- `CLONE_NEWNET` with `lo` brought up via `SIOCSIFFLAGS` ioctl

## Dependencies

- Rust edition 2024 (nightly)
- `libc` — raw C FFI (syscalls, wait macros, hostname, mount, chroot, ...)
- `clap` — CLI argument parsing
- `anyhow` + `thiserror` — error propagation
- `tracing` + `tracing-subscriber` — structured logging
- `io-uring` — raw io_uring bindings for the daemon event loop

## Usage

```bash
# Daemon must be running for any command (run, list, kill, logs)
cargo run -- daemon &

# Non-detach run goes through daemon
cargo run -- run --rootfs /tmp/alpine /bin/sh
cargo run -- run --rootfs /tmp/alpine --tty /bin/sh  # interactive PTY

# Detach (daemon spawns and forgets)
cargo run -- run --detach --rootfs /tmp/alpine /bin/sleep 60
cargo run list
cargo run kill <pid>
cargo run logs <pid>
```

No root required — user namespaces handle privilege escalation.

## Status

### Phase 1 — Container Runtime ✅

- `clone3` with `USER | PID | NS | UTS | IPC` namespaces
- User namespace maps after clone, sync pipe blocking
- `chroot` into rootfs, mount `/proc` and `/dev`
- PTY allocation with raw-mode I/O relay (`-t` flag)
- `conrt run <cmd>` foreground

### Phase 2 — Daemon & OverlayFS ✅

- Unix-socket JSON daemon with `io_uring` event loop
- Container state tracking, `--detach`, `list`, `kill`
- OverlayFS: writable upperdir, `--save`/`--rm`

### Phase 3 — Logging & Streaming ✅

- Ring buffer for container log storage
- `conrt logs <pid>` reads stored output; live streaming via bbqueue
- `io_uring` handles all daemon I/O (PTY reads, client requests, signalfd)

### Phase 4 — Network ✅

- `CLONE_NEWNET` with `lo` up

### Phase 5 — Daemon-managed run-attach ✅

- Stream listener at `<socket>.stream` for non-detach `run`
- Frame protocol: `0x00 RunRequest`, `0x01 RunResponse`, `0x10 Data`, `0x11 StdinEof`, `0x20 WinSize`, `0x02 ExitCode`
- Daemon-side PTY/pipe I/O relay via io_uring ping-pong (backpressure on output)
- Client: raw terminal, SIGWINCH handler, reader thread, stdin relay
- Exit-code propagation with deferred send on pending writes
