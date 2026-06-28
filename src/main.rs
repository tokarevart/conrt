use anyhow::Result;
use clap::Parser;
use clap::Subcommand;
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
        /// Path to the root filesystem
        #[arg(long)]
        rootfs: String,

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

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::builder()
                .with_default_directive(LevelFilter::INFO.into())
                .from_env()
                .unwrap(), // .add_directive("some-crate=warn".parse().unwrap()),
        )
        .init();

    let cli = Cli::parse();

    match cli.command {
        Command::Daemon => run_daemon(),
        Command::Run {
            rootfs,
            cpu,
            memory,
            t,
            command,
        } => run_container(rootfs, cpu, memory, t, command),
        Command::Logs { id } => show_logs(id),
        Command::List => list_containers(),
        Command::Kill { id } => kill_container(id),
    }
}

fn run_daemon() -> Result<()> {
    tracing::info!("starting daemon");
    todo!("daemon event loop")
}

fn run_container(
    _rootfs: String,
    _cpu: Option<u8>,
    _memory: Option<String>,
    _tty: bool,
    _command: Vec<String>,
) -> Result<()> {
    tracing::info!("running container");
    todo!("container creation")
}

fn show_logs(_id: String) -> Result<()> {
    todo!("container logs")
}

fn list_containers() -> Result<()> {
    todo!("list containers")
}

fn kill_container(_id: String) -> Result<()> {
    todo!("kill container")
}
