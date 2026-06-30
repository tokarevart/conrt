mod cstring;
mod interprocess;
mod sys;

use std::ffi::c_int;
use std::io;
use std::process::ExitCode;
use std::str::FromStr;

use clap::Parser;
use cstring::CString;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(name = "conrt")]
enum Cli {
    /// Run a container
    Run {
        /// Path to the root filesystem (optional; uses host rootfs if omitted)
        #[arg(long)]
        rootfs: Option<String>,

        /// CPU limit as a percentage (1-100)
        #[arg(long)]
        cpu: Option<u8>,

        /// Memory limit (e.g. 128M, 1G)
        #[arg(long)]
        memory: Option<String>,

        /// Allocate a PTY for interactive use
        #[arg(short)]
        t: bool,

        /// Command to run inside the container
        command: Vec<CString>,
    },
    /// Start the daemon
    Daemon,
    /// Show container logs
    Logs {
        /// Container ID
        id: String,
    },
    /// List running containers
    List,
    /// Kill a container
    Kill {
        /// Container ID
        id: String,
    },
}

fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::builder()
                .with_default_directive(LevelFilter::INFO.into())
                .from_env()
                .unwrap(),
        )
        .init();

    let cli = Cli::parse();

    match cli {
        Cli::Daemon => run_daemon(),
        Cli::Run {
            rootfs,
            cpu,
            memory,
            t,
            command,
        } => run_container(RunArgs {
            rootfs,
            cpu,
            memory,
            tty: t,
            command,
        }),
        Cli::Logs { id } => show_logs(id),
        Cli::List => list_containers(),
        Cli::Kill { id } => kill_container(id),
    }
}

fn run_daemon() -> ExitCode {
    tracing::info!("starting daemon");
    todo!("daemon event loop")
}

struct RunArgs {
    rootfs: Option<String>,
    #[allow(dead_code)]
    cpu: Option<u8>,
    #[allow(dead_code)]
    memory: Option<String>,
    #[allow(dead_code)]
    tty: bool,
    command: Vec<CString>,
}

fn run_container(args: RunArgs) -> ExitCode {
    let rootfs = match args.rootfs {
        Some(p) => match std::fs::canonicalize(&p) {
            Ok(path) => Some(path.display().to_string()),
            Err(e) => {
                tracing::error!(%e, rootfs = %p, "invalid rootfs path");
                return ExitCode::FAILURE;
            }
        },
        None => None,
    };

    let signal = match interprocess::OneshotSignal::new() {
        Ok(s) => s,
        Err(e) => {
            tracing::error!(%e, "sync pipe creation failed");
            return ExitCode::FAILURE;
        }
    };

    match clone3_container() {
        Err(e) => {
            tracing::error!(%e, "clone3 failed");
            ExitCode::FAILURE
        }
        Ok(None) => {
            if let Err(e) = signal.wait() {
                tracing::error!(%e, "sync wait failed");
                std::process::exit(1);
            }

            if let Err(e) = sys::sethostname("conrt") {
                tracing::error!(%e, "sethostname failed");
            }

            if let Some(ref rootfs) = rootfs
                && let Err(e) = setup_container_root(rootfs)
            {
                tracing::error!(%e, "container root setup failed");
                std::process::exit(1);
            }

            let argv = sys::Argv::new(args.command);
            let errno = execvp(argv.as_slice());
            tracing::error!(%errno, "execvp failed");
            std::process::exit(1)
        }
        Ok(Some(pid)) => {
            let maps_result = setup_userns_maps(pid);
            signal.signal();

            if let Err(e) = maps_result {
                tracing::error!(%e, "container aborted");
                return ExitCode::FAILURE;
            }

            tracing::info!(child = pid, "container started");
            match wait_for_child(pid) {
                Ok(code) => code,
                Err(e) => {
                    tracing::error!(%e, "wait failed");
                    ExitCode::FAILURE
                }
            }
        }
    }
}

fn clone3_container() -> io::Result<Option<libc::pid_t>> {
    const CLONE_FLAGS: c_int = libc::CLONE_NEWPID
        | libc::CLONE_NEWNS
        | libc::CLONE_NEWUTS
        | libc::CLONE_NEWIPC
        | libc::CLONE_NEWUSER;

    let args = libc::clone_args {
        flags: CLONE_FLAGS as u64,
        pidfd: 0,
        child_tid: 0,
        parent_tid: 0,
        exit_signal: libc::SIGCHLD as u64,
        stack: 0,
        stack_size: 0,
        tls: 0,
        set_tid: 0,
        set_tid_size: 0,
        cgroup: 0,
    };

    Ok(match unsafe { sys::clone3(&args) }? {
        0 => None,
        x => Some(x as libc::pid_t),
    })
}

/// Set up the container root filesystem.
///
/// Uses `chroot` instead of `pivot_root` because an unprivileged user namespace
/// cannot unmount the old root (created by the init namespace), which
/// `pivot_root` + `umount2` requires.
///
/// 1. Remount the mount tree as private (prevent mount leaks to host).
/// 2. Bind-mount the rootfs onto itself.
/// 3. `chdir` into rootfs.
/// 4. `chroot(".")` — change root to the bound rootfs.
/// 5. `chdir("/")`.
/// 6. Mount `/proc`, `/sys`, `/dev`.
fn setup_container_root(rootfs: &str) -> io::Result<()> {
    let rootfs_c = CString::from_str(rootfs).unwrap();
    let root_c = CString::from_str("/").unwrap();
    let proc_c = CString::from_str("proc").unwrap();
    let proc_dir_c = CString::from_str("/proc").unwrap();
    let tmpfs_c = CString::from_str("tmpfs").unwrap();
    let dev_c = CString::from_str("/dev").unwrap();

    // 1. Remount entire tree as private
    sys::mount(
        None,
        root_c.borrow(),
        None,
        libc::MS_REC | libc::MS_PRIVATE,
        None,
    )?;

    // 2. Bind-mount rootfs onto itself (so it's a mount point)
    sys::mount(
        rootfs_c.borrow().into(),
        rootfs_c.borrow(),
        None,
        libc::MS_BIND | libc::MS_REC,
        None,
    )?;

    // 3. chdir into rootfs
    sys::chdir(rootfs_c.borrow())?;

    // 4. chroot to current directory (".")
    sys::chroot(rootfs_c.borrow())?;

    // 5. chdir to new root
    sys::chdir(root_c.borrow())?;

    // 6. Mount proc
    sys::mount(
        proc_c.borrow().into(),
        proc_dir_c.borrow(),
        proc_c.borrow().into(),
        0,
        None,
    )?;

    // 7. Mount dev (tmpfs)
    sys::mount(None, dev_c.borrow(), tmpfs_c.borrow().into(), 0, None)?;

    Ok(())
}

/// Replace the current process with the given command.
fn execvp(argv: &sys::ArgvSlice) -> io::Error {
    sys::execvp(argv)
}

/// Wait for a child process and return its exit code.
fn wait_for_child(pid: libc::pid_t) -> io::Result<ExitCode> {
    let mut status: i32 = 0;
    let _ = sys::wait4(pid, &mut status, 0, None)?;
    tracing::info!(%status, "container exited");
    if libc::WIFEXITED(status) {
        Ok(ExitCode::from(libc::WEXITSTATUS(status) as u8))
    } else if libc::WIFSIGNALED(status) {
        Ok(ExitCode::from(128 + libc::WTERMSIG(status) as u8))
    } else {
        Ok(ExitCode::FAILURE)
    }
}

fn setup_userns_maps(pid: libc::pid_t) -> io::Result<()> {
    let uid = unsafe { libc::getuid() };
    let gid = unsafe { libc::getgid() };

    let setgroups_path = format!("/proc/{}/setgroups", pid);
    if std::fs::write(&setgroups_path, "deny\n").is_err() {
        // setgroups file may not exist on older kernels; ignore
    }

    let uid_map_path = format!("/proc/{}/uid_map", pid);
    std::fs::write(&uid_map_path, format!("0 {} 1\n", uid))?;

    let gid_map_path = format!("/proc/{}/gid_map", pid);
    std::fs::write(&gid_map_path, format!("0 {} 1\n", gid))?;

    Ok(())
}

fn show_logs(_id: String) -> ExitCode {
    todo!("container logs")
}

fn list_containers() -> ExitCode {
    todo!("list containers")
}

fn kill_container(_id: String) -> ExitCode {
    todo!("kill container")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rootfs_nonexistent_returns_failure() {
        let code = run_container(RunArgs {
            rootfs: Some("/definitely/not/a/real/path".into()),
            cpu: None,
            memory: None,
            tty: false,
            command: vec![CString::from_str("/bin/true").unwrap()],
        });
        assert_eq!(code, ExitCode::FAILURE);
    }
}
