# Built-in init process

Make the container PID 1 a built-in init by default so containers die
when the daemon dies.  Allow users to override with their own init or
skip it entirely.

## Motivation

`PR_SET_PDEATHSIG` cannot work across PID namespaces — the parent
must be in the same namespace for the signal to be delivered, but the
daemon lives in a different namespace from the child.  A custom init
as PID 1 that monitors a **death pipe** solves this: the daemon keeps
one end of the pipe open, and when it exits the kernel closes that end.
The init loop detects `POLLHUP` and kills the entire PID namespace.

## Modes

| Mode | Flag | PID 1 | Daemon-crash behavior | Death pipe |
|---|---|---|---|---|
| Default | *(none)* | Built-in init | Kills namespace | Yes |
| Custom init | `--init <path>` | User's init (tini, s6, …) | Survives | No |
| No init | `--no-init` | User command directly | Survives | No |

`--init` and `--no-init` are mutually exclusive.

## Wire protocol

Add two optional fields to the `Run` request:

- `no_init: bool`
- `init_path: Option<String>`

Server validates: `no_init && init_path.is_some()` is an error.

## Process tree (default init)

The clone3 child becomes PID 1 in the new namespace.  It forks a
grandchild that execs the user command, then enters an init loop.

```
daemon ──clone3──→ PID 1 (init loop)
                     │
                     ├──fork──→ PID 2 (user command)
                     │
                     └── poll(death_pipe, signalfd)
                          │
                          ├── POLLHUP        → kill(-1, SIGKILL); _exit()
                          ├── signalfd ready → forward signal to PID 2
                          └── SIGCHLD        → waitpid, exit with status
```

## Death pipe

A pipe created before `clone3`.  The daemon keeps the write end open
for the container's lifetime.  The init loop monitors the read end.

- Paired with `O_CLOEXEC` so it doesn't leak into the user command
- Daemon side: close read end, keep write end open (kernel closes it
  on daemon exit regardless of how the daemon exits)
- PID 1 side: close write end (already closed by daemon before clone3)
- Grandchild side: close both ends (doesn't need it)

### Kill flow

1. Daemon exits (SIGTERM, SIGKILL, crash — anything)
2. Kernel closes all daemon fds → death pipe write end closed
3. Init loop `poll()` returns `POLLHUP` on the read end
4. Init loop calls `kill(-1, SIGKILL)` — kills everything in the
   namespace, including the grandchild
5. Init loop calls `_exit()` (necessary because `kill(-1, SIGKILL)`
   does **not** kill PID 1 — the kernel protects it)

### What works

| Daemon exit reason | Death pipe breaks? | Container dies? |
|---|---|---|
| `SIGTERM` (orderly shutdown) | Yes | Yes |
| `SIGKILL` (kill -9) | Yes | Yes |
| Crash (segfault, panic) | Yes | Yes |

## Signal forwarding (default init)

The init loop blocks catchable signals via `sigprocmask` + `signalfd`,
and forwards them to the grandchild:

| Signal to PID 1 | Action |
|---|---|
| `SIGTERM` | Forwarded to grandchild |
| `SIGINT` | Forwarded to grandchild |
| `SIGHUP` | Forwarded to grandchild |
| `SIGKILL` | Cannot be blocked — kernel kills PID 1, namespace destroyed |

## Custom init (`--init <path>`)

Exec the custom init as PID 1, passing the user command after `--`:

```
clone3 → PID 1
          │
          └── exec(path, ["--", cmd, arg1, ...])
```

No death pipe — the user opted into their own init and accepts that
containers survive daemon death.

## No init (`--no-init`)

Exec the user command directly as PID 1:

```
clone3 → PID 1
          │
          └── exec(cmd, args)
```

Simplest path.  No reaping, no signal forwarding, no death detection.

## Init loop pseudocode (default mode)

```
setup:
  close(death_pipe.write)
  block_sigset = {SIGTERM, SIGINT, SIGHUP}
  sigprocmask(SIG_BLOCK, block_sigset)
  sigfd = signalfd(-1, block_sigset, SFD_CLOEXEC)

  gpid = fork()
  if gpid == 0:
    close(death_pipe.read)
    exec(user_command)         // PID 2

  loop:
    poll([death_pipe.read | POLLIN, sigfd | POLLIN], -1)

    if death_pipe.revents & (POLLHUP | POLLERR):
      kill(-1, SIGKILL)        // kills grandchild
      _exit(137)               // SIGKILL doesn't hurt PID 1

    if sigfd.revents & POLLIN:
      siginfo = read(sigfd)
      kill(gpid, siginfo.signo)

    if waitpid(gpid, WNOHANG) == gpid:
      _exit(child_status)
```

## Foreground path

Foreground `conrt run` (without `--detach`) goes through the daemon
via the stream-attach protocol. Init flags in the `RunRequest` apply
the same way as in the detach path — the daemon handles init setup
after `clone3`.
