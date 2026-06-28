mod cstring;

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
    match clone3_container() {
        Err(e) => {
            tracing::error!(%e, "clone3 failed");
            ExitCode::FAILURE
        }
        Ok(None) => {
            // Child process — in new namespaces as PID 1
            if unsafe { libc::sethostname(c"conrt".as_ptr() as _, 5) } < 0 {
                let e = io::Error::last_os_error();
                tracing::error!(%e, "sethostname failed");
            }

            let errno = execvp(command);
            tracing::error!(%errno, "execvp failed");
            std::process::exit(1)
        }
        Ok(Some(pid)) => {
            // Parent — monitor the child
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
    const CLONE_FLAGS: c_int =
        libc::CLONE_NEWPID | libc::CLONE_NEWNS | libc::CLONE_NEWUTS | libc::CLONE_NEWIPC;

    clone3(CLONE_FLAGS as _, libc::SIGCHLD)
}

/// Raw clone3 syscall wrapper (Linux 5.3+).
///
/// Returns `Ok(None)` in the child, `Ok(Some(pid))` in the parent.
/// Avoids glibc's clone wrapper entirely — no stack allocation needed.
fn clone3(flags: u64, exit_signal: i32) -> io::Result<Option<libc::pid_t>> {
    let args = libc::clone_args {
        flags,
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
        return Err(io::Error::last_os_error());
    }

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
    unsafe { libc::execvp(argv[0], argv.as_ptr()) };
    io::Error::last_os_error()
}

/// Wait for a child process and return its exit code.
fn wait_for_child(pid: libc::pid_t) -> io::Result<ExitCode> {
    let mut status: i32 = 0;
    let ret = unsafe { libc::waitpid(pid, &mut status, 0) };
    if ret < 0 {
        return Err(io::Error::last_os_error());
    }
    tracing::info!(%status, "container exited");
    if libc::WIFEXITED(status) {
        Ok(ExitCode::from(libc::WEXITSTATUS(status) as u8))
    } else if libc::WIFSIGNALED(status) {
        Ok(ExitCode::from(128 + libc::WTERMSIG(status) as u8))
    } else {
        Ok(ExitCode::FAILURE)
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
