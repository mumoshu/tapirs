mod docker;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "tapiadm", about = "TAPIR cluster administration")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Manage a Docker Compose-based TAPIR cluster.
    Docker {
        #[command(subcommand)]
        action: DockerAction,
    },
}

#[derive(Subcommand)]
enum DockerAction {
    /// Build, start, and bootstrap a TAPIR cluster.
    Up,
    /// Stop and remove all cluster resources.
    Down,
    /// Add a resource to the cluster.
    Add {
        #[command(subcommand)]
        resource: AddResource,
    },
    /// Remove a resource from the cluster.
    Remove {
        #[command(subcommand)]
        resource: RemoveResource,
    },
    /// List resources in the cluster.
    Get {
        #[command(subcommand)]
        resource: GetResource,
    },
}

#[derive(Subcommand)]
enum AddResource {
    /// Spin up a new tapi node container on the Docker network.
    Node {
        /// Optional name for the node (default: auto-generated tapir-node-N).
        #[arg(long)]
        name: Option<String>,
    },
}

#[derive(Subcommand)]
enum RemoveResource {
    /// Stop and remove a node container.
    Node {
        /// Name of the node to remove.
        name: String,
    },
}

#[derive(Subcommand)]
enum GetResource {
    /// List all nodes with names, container IPs, and host admin ports.
    Nodes,
    /// List all replicas across all nodes with shard, node, and listen address.
    Replicas,
}

fn main() {
    let cli = Cli::parse();
    let result = match cli.command {
        Command::Docker { action } => match action {
            DockerAction::Up => docker::up(),
            DockerAction::Down => docker::down(),
            DockerAction::Add {
                resource: AddResource::Node { name },
            } => docker::add_node(name),
            DockerAction::Remove {
                resource: RemoveResource::Node { name },
            } => docker::remove_node(&name),
            DockerAction::Get {
                resource: GetResource::Nodes,
            } => docker::get_nodes(),
            DockerAction::Get {
                resource: GetResource::Replicas,
            } => docker::get_replicas(),
        },
    };

    if let Err(e) = result {
        eprintln!("Error: {e}");
        std::process::exit(1);
    }
}
