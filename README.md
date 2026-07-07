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
│  poll-based event loop (io_uring planned)                │
│                                                          │
│  poll fds:                                               │
│  ├── listener socket (accept → handle client)            │
│  └── signalfd (SIGCHLD → waitpid → reap + cleanup)      │
│                                                          │
│  loop: poll() → match revents                            │
│                                                          │
│  No threads. Single-threaded event loop.                 │
└──────────────────────────────────────────────────────────┘
```

Key points:
- **One daemon** manages N containers, not a parent process per container
- Single-threaded `poll`-based event loop; `io_uring` planned as an optimization
- Daemon handles all host-side teardown (overlay cleanup, etc.)
- Detached containers get `PR_SET_PDEATHSIG(SIGKILL)` — daemon crash kills all children
- Foreground `conrt run` (without `--detach`) runs standalone, no daemon required

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

## Usage

```bash
cargo run -- daemon &
cargo run -- run --rootfs /tmp/alpine /bin/sh
```

No root required — user namespaces handle privilege escalation.

## Status

### Phase 0 — Daemon ✅

- JSON-over-Unix-socket daemon with `poll` event loop
- `signalfd` for SIGCHLD → reap + overlay cleanup
- Container state tracking (`HashMap<pid, ContainerInfo>`)
- `conrt run --detach` hands off containers to the daemon
- `conrt list` / `conrt kill <pid>` connect to daemon via `~/.conrt/conrt.sock`
- `conrt run` (without `--detach`) stays standalone foreground
- PID used as container ID (no UUID)

### Phase 1 — Process & Filesystem Isolation ✅

- `clone3` with `CLONE_NEWUSER | CLONE_NEWPID | CLONE_NEWNS | CLONE_NEWUTS | CLONE_NEWIPC`
- Parent writes UID/GID maps after clone so the child becomes UID 0 with full capabilities
- Pipe-based synchronization: child blocks until parent finishes writing maps
- `chroot` into prepared rootfs (bind-mount rootfs dir onto itself first)
- Mount `/proc`, `/dev`, best-effort `/sys`
- PTY allocation (`-t` flag): `openpty` + `setsid` + `TIOCSCTTY` + `dup2` to 0/1/2,
  then `poll`-based I/O relay between host terminal and PTY master, with raw mode
  for interactive use
- Daemon child reaping via `signalfd` + `waitpid(-1, WNOHANG)` loop ✅
- Detached containers get `PR_SET_PDEATHSIG(SIGKILL)` — daemon crash kills children

### Phase 2 — Cgroups v2 (skipped)

### Phase 3 — Network Namespace (lo only; veth requires CAP_NET_ADMIN)

### Phase 4 — OverlayFS ✅

- Overlay mount with lowerdir=`<rootfs>`, per-container upperdir + workdir
- `--save` flag to preserve the upperdir after container exit
- Works rootlessly on kernel 5.11+

### Phase 5 — Security (Capabilities + Seccomp) (skipped — rootless user ns provides equivalent restrictions)
