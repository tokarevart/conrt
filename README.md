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

### Phase 2 — Cgroups v2 (skipped)

Resource limits via cgroups v2 are not viable in rootless mode unless the kernel
delegates `cpu` and `memory` controllers. On this system, `user.slice/` only has
`pids` in `subtree_control` and the user's processes live in `/init.scope`
(outside the delegated subtree). Both `CLONE_INTO_CGROUP` and post-clone
`cgroup.procs` writes fail with EACCES/ENOENT. Only `pids.max` works without
root intervention.

### Phase 3 — Network Namespace

- `CLONE_NEWNET` gives the container an isolated network stack
- Child brings up `lo` via `SIOCSIFFLAGS` ioctl (works without `ip(8)` in rootfs)
- Veth pair + NAT (bridge, iptables) require `CAP_NET_ADMIN` in the init netns
  — not available rootlessly. External connectivity would need `slirp4netns`.

### Phase 4 — OverlayFS ✅

- When `--rootfs <path>` is given, an overlay mount is created with the rootfs
  as lowerdir and a per-container upperdir + workdir
- Overlay is mounted inside the child's mount namespace (auto-cleaned on exit)
- `--rm` (default): wipe upperdir on exit via `remove_dir_all`
- `--save`: preserve upperdir for debugging / inspection
- Works rootlessly (OverlayFS is supported in user namespaces on kernel 5.11+)

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
- PTY allocation (`-t` flag): `openpty` + `setsid` + `TIOCSCTTY` + `dup2` to 0/1/2,
  then `poll`-based I/O relay between host terminal and PTY master, with raw mode
  for interactive use
- Daemon child reaping (planned)

### Phase 2 — Cgroups v2 (skipped)

### Phase 3 — Network Namespace (lo only; veth requires CAP_NET_ADMIN)

### Phase 4 — OverlayFS ✅

- Overlay mount with lowerdir=`<rootfs>`, per-container upperdir + workdir
- `--save` flag to preserve the upperdir after container exit
- Works rootlessly on kernel 5.11+

### Phase 5 — Security (Capabilities + Seccomp) (not started)
