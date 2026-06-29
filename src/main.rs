mod cstring;
mod interprocess;
mod sys;

use std::ffi::c_int;
use std::io;
use std::mem;
use std::process::ExitCode;

use clap::Parser;
use clap::Subcommand;
use cstring::CString;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(name = "conrt")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
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

    match cli.command {
        Command::Daemon => run_daemon(),
        Command::Run {
            rootfs: _rootfs,
            cpu,
            memory,
            t,
            command,
        } => run_container(cpu, memory, t, command),
        Command::Logs { id } => show_logs(id),
        Command::List => list_containers(),
        Command::Kill { id } => kill_container(id),
    }
}

fn run_daemon() -> ExitCode {
    tracing::info!("starting daemon");
    todo!("daemon event loop")
}

fn run_container(
    _cpu: Option<u8>,
    _memory: Option<String>,
    _tty: bool,
    command: Vec<CString>,
) -> ExitCode {
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

            const HOSTNAME: &[u8] = b"conrt";
            if let Err(e) =
                unsafe { sys::sethostname(HOSTNAME.as_ptr() as *const (), HOSTNAME.len()) }
            {
                tracing::error!(%e, "sethostname failed");
            }

            let errno = execvp(command);
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

    let ret = unsafe {
        sys::clone3(
            &args as *const libc::clone_args,
            mem::size_of::<libc::clone_args>(),
        )
    }?;

    Ok(if ret == 0 {
        None
    } else {
        Some(ret as libc::pid_t)
    })
}

/// Replace the current process with the given command.
fn execvp(command: Vec<CString>) -> io::Error {
    let mut argv = CString::into_ptr_vec(command);
    argv.push(std::ptr::null());
    sys::execvp(argv.as_ptr())
}

/// Wait for a child process and return its exit code.
fn wait_for_child(pid: libc::pid_t) -> io::Result<ExitCode> {
    let mut status: i32 = 0;
    let _ = unsafe { sys::wait4(pid, &mut status, 0) }?;
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
