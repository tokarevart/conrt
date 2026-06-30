# conrt — A Minimal Container Runtime

conrt is a from-scratch, Docker-like container runtime built in Rust. It's a course project
for learning systems programming: Linux kernel interfaces (namespaces, cgroups, mounts,
seccomp), process and memory management in Rust, and a few classic data structures.

## Architecture

conrt uses a **single daemon** process model:

```
┌──────────────────────────────────────────────────────────┐
│                    Daemon (host ns)                      │
│                                                          │
│  Single io_uring ring (no epoll, no tokio)               │
│                                                          │
│  SQEs:                                                   │
│  ├── IORING_OP_WAITID → child₁                           │
│  ├── IORING_OP_WAITID → child₂                           │
│  ├── IORING_OP_READ    → PTY master for container 1      │
│  ├── IORING_OP_READ    → PTY master for container 2      │
│  ├── IORING_OP_ACCEPT  → Unix socket                     │
│  ├── IORING_OP_READ    → connected client socket         │
│  ├── IORING_OP_WRITE   → connected client socket         │
│  └── ...                                                 │
│                                                          │
│  loop: io_uring_wait_cqe() → match op → submit new SQEs  │
│                                                          │
│  No separate threads. All I/O through one ring.          │
└──────────────────────────────────────────────────────────┘
```

Key points:
- **One daemon** manages N containers, not a parent process per container
- All I/O goes through a single `io_uring` ring — PTY reads, socket accept/read/write, child reaping (`IORING_OP_WAITID`). No separate threads.
- Daemon handles all host-side teardown (cgroup deletion, veth cleanup, overlay unmount)
- PTY interception gives `conrt logs <id>` access to historical output
- No container-level reaper in v1 (user command is PID 1; `--init` can be added later)
- `io-uring` crate used directly — no wrapper abstractions on top

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

### Phase 0 — Project Scaffolding

- Rust project with `clap` for CLI, `nix` for syscalls, `tracing` for logging,
  `anyhow` for errors
- Daemon subcommand: `conrt daemon`
- Client subcommands: `conrt run [OPTIONS] <COMMAND>...`, `conrt logs <id>`,
  `conrt list`, `conrt kill <id>`
- Communication between client and daemon via Unix socket

### Phase 1 — Process & Filesystem Isolation

- `clone3` with `CLONE_NEWUSER | CLONE_NEWPID | CLONE_NEWNS | CLONE_NEWUTS | CLONE_NEWIPC`
- Parent writes UID/GID maps (`/proc/<pid>/uid_map`, `/proc/<pid>/gid_map`)
  after clone so the child becomes UID 0 with full capabilities
- Pipe-based synchronization: child blocks until parent finishes writing maps
- `chroot` into prepared rootfs (bind-mount rootfs dir onto itself first)
  — uses `chroot` instead of `pivot_root` because unprivileged user namespaces
  cannot unmount the old root (requires init-namespace `CAP_SYS_ADMIN`)
- Mount `/proc`, `/sys`, `/dev`
- PTY allocation (`openpty`) for interactive `-t` containers
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

- Rust edition 2024 (nightly)
- `libc` — raw C FFI (syscalls, wait macros, hostname, mount, chroot, ...)
- `clap` — CLI argument parsing
- `anyhow` + `thiserror` — error propagation
- `tracing` + `tracing-subscriber` — structured logging
- `io-uring` — raw io_uring bindings for the daemon event loop (planned)

## Building

```bash
cargo build --release
sudo ./target/release/conrt daemon &
sudo ./target/release/conrt run --rootfs /tmp/alpine /bin/sh
sudo ./target/release/conrt logs <container-id>
```

No root required — user namespaces handle privilege escalation. Cgroups and network
setup may require additional capabilities.

## Status

### Phase 0 — Project Scaffolding ✅

- Rust project with `clap` for CLI, `libc` for syscalls, `tracing` for logging,
  `anyhow` for errors
- Daemon subcommand: `conrt daemon` (stub)
- Client subcommands: `conrt run [OPTIONS] <COMMAND>...`, `conrt logs <id>`,
  `conrt list`, `conrt kill <id>` (logs/list/kill are stubs)
- Communication between client and daemon via Unix socket (planned)

### Phase 1 — Process & Filesystem Isolation ✅

- `clone3` with `CLONE_NEWUSER | CLONE_NEWPID | CLONE_NEWNS | CLONE_NEWUTS | CLONE_NEWIPC`
- Parent writes UID/GID maps after clone so the child becomes UID 0 with full capabilities
- Pipe-based synchronization: child blocks until parent finishes writing maps
- `chroot` into prepared rootfs (bind-mount rootfs dir onto itself first)
- Mount `/proc`, `/dev`, best-effort `/sys`
- PTY allocation (planned)
- Daemon child reaping (planned)

### Phase 2 — Cgroups v2 (not started)

### Phase 3 — Network Namespace & veth (not started)

### Phase 4 — OverlayFS (not started)

### Phase 5 — Security (Capabilities + Seccomp) (not started)
