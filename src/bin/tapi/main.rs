mod admin_client;
mod admin_server;
mod client;
mod config;
mod discovery;
mod node;
mod repl;
mod shard_manager_server;

use clap::{Parser, Subcommand};
use config::{ClientConfig, NodeConfig};

#[derive(Parser)]
#[command(name = "tapi", about = "TAPIR distributed transactional KV store")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Start a node hosting multiple replicas.
    Node {
        #[arg(long)]
        config: Option<String>,
        #[arg(long)]
        admin_listen_addr: Option<String>,
        #[arg(long)]
        persist_dir: Option<String>,
    },
    /// Admin operations on a running node.
    Admin {
        #[command(subcommand)]
        action: AdminAction,
    },
    /// Interactive transactional REPL client.
    Client {
        #[arg(long)]
        config: Option<String>,
    },
    /// Run the cluster discovery service.
    Discovery {
        /// Address to listen on.
        #[arg(long, default_value = "127.0.0.1:8080")]
        listen_addr: String,
    },
    /// Run a standalone shard manager server.
    ShardManager {
        /// Address to listen on.
        #[arg(long, default_value = "127.0.0.1:9001")]
        listen_addr: String,
        /// Discovery service URL.
        #[arg(long)]
        discovery_url: String,
    },
}

#[derive(Subcommand)]
enum AdminAction {
    /// Show node status.
    Status {
        #[arg(long, default_value = "127.0.0.1:9000")]
        admin_listen_addr: String,
    },
    /// Add a replica for a shard.
    AddReplica {
        #[arg(long, default_value = "127.0.0.1:9000")]
        admin_listen_addr: String,
        #[arg(long)]
        shard: u32,
    },
    /// Remove a replica for a shard.
    RemoveReplica {
        #[arg(long, default_value = "127.0.0.1:9000")]
        admin_listen_addr: String,
        #[arg(long)]
        shard: u32,
    },
    /// Trigger a view change for a shard.
    ViewChange {
        #[arg(long, default_value = "127.0.0.1:9000")]
        admin_listen_addr: String,
        #[arg(long)]
        shard: u32,
    },
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();

    match cli.command {
        Command::Node {
            config: config_path,
            admin_listen_addr,
            persist_dir,
        } => {
            let mut cfg = config_path
                .as_deref()
                .map(NodeConfig::from_file)
                .unwrap_or_default();
            if let Some(addr) = admin_listen_addr {
                cfg.admin_listen_addr = Some(addr);
            }
            if let Some(dir) = persist_dir {
                cfg.persist_dir = Some(dir);
            }
            node::run(cfg).await;
        }
        Command::Admin { action } => {
            admin_client::run(action).await;
        }
        Command::Client { config: config_path } => {
            let cfg = config_path
                .as_deref()
                .map(ClientConfig::from_file)
                .unwrap_or_default();
            client::run(cfg).await;
        }
        Command::Discovery { listen_addr } => {
            discovery::run(listen_addr).await;
        }
        Command::ShardManager {
            listen_addr,
            discovery_url,
        } => {
            shard_manager_server::run(listen_addr, discovery_url).await;
        }
    }
}
