# conrt — A Minimal Container Runtime

conrt is a from-scratch, Docker-like container runtime built in Rust. It's a course project
for learning systems programming: Linux kernel interfaces (namespaces, cgroups, mounts,
seccomp), process and memory management in Rust, and a few classic data structures.

## Architecture

conrt uses a **single daemon** process model:

```
┌─────────────────────────────────────────────────────────┐
│                    Daemon (host ns)                      │
│                                                          │
│  Event loop (epoll or tokio)                             │
│  ├── clone() → child₁ ───┬── PID 1 in ns₁               │
│  ├── clone() → child₂ ───┬── PID 1 in ns₂               │
│  ├── clone() → child₃ ───┬── PID 1 in ns₃               │
│  │                                                        │
│  ├── PTY reader thread per container                      │
│  │    └── reads bytes from PTY master FD                 │
│  │         ├── line-buffer until \n                       │
│  │         ├── push(Vec<u8>) to RingBuffer                │
│  │         └── write to attached caller's stdout          │
│  │                                                        │
│  ├── Unix socket listener for conrt logs <id>            │
│  │    └── reads from RingBuffer, sends to client         │
│  │                                                        │
│  └── SIGCHLD handler → reap exited children, clean up    │
└─────────────────────────────────────────────────────────┘
```

Key points:
- **One daemon** manages N containers, not a parent process per container
- Daemon `waitpid()`s on all children through an event loop
- Daemon handles all host-side teardown (cgroup deletion, veth cleanup, overlay unmount)
- PTY interception gives `conrt logs <id>` access to historical output
- No container-level reaper in v1 (user command is PID 1; `--init` can be added later)

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
                          daemon reader thread
                                    │
                         line-buffer until \n
                                    │
                         ┌──────────┼──────────┐
                         ▼          ▼          ▼
                    RingBuffer   caller stdout  (future: file)
                    (conrt logs)  (--attach)
```

## Phases

### Phase 0 — Project Scaffolding

- Rust project with `clap` for CLI, `nix` for syscalls, `tracing` for logging,
  `anyhow` for errors
- Daemon subcommand: `conrt daemon`
- Client subcommands: `conrt run [OPTIONS] <COMMAND>...`, `conrt logs <id>`,
  `conrt list`, `conrt kill <id>`
- Communication between client and daemon via Unix socket

### Phase 1 — Process & Filesystem Isolation

- `clone(CLONE_NEWPID | CLONE_NEWNS | CLONE_NEWUTS | CLONE_NEWIPC)`
- `pivot_root` into prepared rootfs (bind-mount rootfs dir onto itself first to
  satisfy same-filesystem requirement)
- Unmount `/.old_root` after pivot (`umount2(MNT_DETACH)`)
- Mount `/proc`, `/sys`, `/dev`
- PTY allocation (`nix::pty::openpty`) for interactive `-t` containers
- Daemon ensures child reaps correctly (SIGCHLD in event loop)

### Phase 2 — Cgroups v2

- `--cpu <percent>` → write quota/period to `cpu.max`
- `--memory <bytes>` → write to `memory.max`
- Create `/sys/fs/cgroup/conrt-<id>/`, write child PID to `cgroup.procs`
- Cleanup: remove cgroup dir on container exit

### Phase 3 — Network Namespace & veth

- Add `CLONE_NEWNET` at clone time
- Daemon creates veth pair via `ip link` (or netlink crate later)
- Moves one end into child's netns, attaches host end to bridge
- Child brings up `lo`, renames veth → `eth0`, assigns IP, sets default gateway
- Host NAT via iptables MASQUERADE
- Writes `/etc/resolv.conf` inside container

### Phase 4 — OverlayFS

- `--rootfs <path>` flag for the unpacked base image (no OCI/manifest parsing)
- Per-container upperdir + workdir created before mount
- `mount -t overlay ... -o lowerdir=<rootfs>,upperdir=<upper>,workdir=<work>`
  mounted before `pivot_root`
- `--rm` (default): wipe upperdir on exit; `--save`: preserve it

### Phase 5 — Security (Capabilities + Seccomp)

- Drop dangerous capabilities (`CAP_SYS_ADMIN`, `CAP_SYS_BOOT`, `CAP_NET_ADMIN`,
  etc.) via `prctl(PR_CAPBSET_DROP, ...)` before exec
- `--cap-add` / `--cap-drop` flags
- Seccomp via `libseccomp` Rust FFI crate: block `reboot`, `swapon`, `kexec_load`
  with an allow-default deny-list model

## Dependencies

- Rust edition 2024
- `nix` — syscall wrappers (clone, mount, pivot_root, sethostname, ...)
- `clap` — CLI argument parsing
- `anyhow` + `thiserror` — error propagation
- `tracing` + `tracing-subscriber` — structured logging
- `tokio` or `mio` — async I/O for event loop (deferred to implementation)

## Building

```bash
cargo build --release
sudo ./target/release/conrt daemon &
sudo ./target/release/conrt run --rootfs /tmp/alpine /bin/sh
sudo ./target/release/conrt logs <container-id>
```

Requires `root` for namespaces, cgroups, network setup, and mount operations.

## Status

Scaffolding in progress.
