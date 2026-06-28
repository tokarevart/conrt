use std::ffi::CString;
use std::mem;
use std::process::ExitCode;

use clap::Parser;
use clap::Subcommand;
use nix::sched::CloneFlags;
use nix::unistd::Pid;
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
        command: Vec<String>,
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
    command: Vec<String>,
) -> ExitCode {
    let flags = CloneFlags::CLONE_NEWPID
        | CloneFlags::CLONE_NEWNS
        | CloneFlags::CLONE_NEWUTS
        | CloneFlags::CLONE_NEWIPC;

    match clone3(flags, libc::SIGCHLD) {
        Err(e) => {
            tracing::error!(%e, "clone3 failed");
            ExitCode::FAILURE
        }
        Ok(None) => {
            // Child process — in new namespaces as PID 1
            if let Err(e) = nix::unistd::sethostname("conrt") {
                tracing::error!(%e, "sethostname failed");
            }
            execvp_or_exit(&command);
        }
        Ok(Some(pid)) => {
            // Parent — monitor the child
            tracing::info!(child = %pid, "container started");
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

/// Raw clone3 syscall wrapper (Linux 5.3+).
///
/// Returns `Ok(None)` in the child, `Ok(Some(pid))` in the parent.
/// Avoids glibc's clone wrapper entirely — no stack allocation needed.
fn clone3(flags: CloneFlags, exit_signal: i32) -> nix::Result<Option<Pid>> {
    let args = libc::clone_args {
        flags: flags.bits() as u64,
        pidfd: 0,
        child_tid: 0,
        parent_tid: 0,
        exit_signal: exit_signal as u64,
        stack: 0,
        stack_size: 0,
        tls: 0,
        set_tid: 0,
        set_tid_size: 0,
        cgroup: 0,
    };

    let ret = unsafe {
        libc::syscall(
            libc::SYS_clone3,
            &args as *const libc::clone_args as i64,
            mem::size_of::<libc::clone_args>() as i64,
        )
    };

    if ret < 0 {
        return Err(nix::errno::Errno::last());
    }

    if ret == 0 {
        Ok(None)
    } else {
        Ok(Some(Pid::from_raw(ret as i32)))
    }
}

/// Replace the current process with the given command.
fn execvp_or_exit(command: &[String]) -> ! {
    if !command.is_empty() {
        let c_cmd = CString::new(command[0].as_bytes()).unwrap();
        let mut cargs: Vec<*const libc::c_char> = command
            .iter()
            .map(|a| CString::new(a.as_bytes()).unwrap().into_raw() as *const libc::c_char)
            .collect();
        cargs.push(std::ptr::null());
        unsafe {
            libc::execvp(c_cmd.as_ptr(), cargs.as_ptr());
        }
    }
    std::process::exit(1);
}

/// Wait for a child process and return its exit code.
fn wait_for_child(pid: Pid) -> nix::Result<ExitCode> {
    let status = nix::sys::wait::waitpid(pid, None)?;
    tracing::info!(?status, "container exited");
    match status {
        nix::sys::wait::WaitStatus::Exited(_, code) => Ok(ExitCode::from(code as u8)),
        nix::sys::wait::WaitStatus::Signaled(_, sig, _) => {
            Ok(ExitCode::from(128 + sig as i32 as u8))
        }
        _ => Ok(ExitCode::FAILURE),
    }
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
