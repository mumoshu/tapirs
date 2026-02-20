mod admin_client;
mod admin_server;
mod client;
mod config;
mod discovery;
mod node;
mod repl;
mod shard_manager_server;

#[cfg(test)]
mod integration_test;

use clap::{Parser, Subcommand, ValueEnum};
use config::{ClientConfig, NodeConfig};

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum StorageBackend {
    /// In-memory MVCC store (default). Fast, bounded by RAM.
    Memory,
    /// Disk-backed MVCC store (WiscKey LSM+VLog). Not yet available.
    Disk,
}

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
        #[arg(long)]
        discovery_url: Option<String>,
        #[arg(long)]
        shard_manager_url: Option<String>,
    },
    /// Admin operations on a running node.
    Admin {
        #[command(subcommand)]
        action: AdminAction,
    },
    /// Transactional REPL client (interactive or scripted).
    ///
    /// Without flags, starts an interactive REPL. Use -e to run inline
    /// commands or -s to run a script file for non-interactive use.
    Client {
        /// Path to client configuration file (TOML).
        #[arg(long)]
        config: Option<String>,

        /// Discovery service URL (overrides config file).
        #[arg(long)]
        discovery_url: Option<String>,

        /// Path to a JSON file describing shard topology.
        ///
        /// Static mode (explicit addresses):
        ///   {"shards":[{"number":0,"membership":["addr:port",...],"key_range_end":"n"}]}
        ///
        /// DNS mode (headless service, resolved at startup):
        ///   {"shards":[{"number":0,"headless_service":"svc.ns:port"}]}
        #[arg(long)]
        discovery_json: Option<String>,

        /// Execute commands inline (semicolons separate commands).
        /// Can be specified multiple times.
        ///
        /// Examples:
        ///   -e "begin; put foo bar; commit"
        ///   -e "begin" -e "put foo bar" -e "commit"
        #[arg(short = 'e', long = "execute")]
        execute: Vec<String>,

        /// Read commands from a script file instead of stdin.
        ///
        /// The file should contain one command per line, same syntax
        /// as the interactive REPL.
        #[arg(short = 's', long = "script")]
        script: Option<String>,
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
        #[arg(long)]
        listen_addr: String,
        /// MVCC storage backend. "memory" (default) keeps all data in RAM.
        /// "disk" uses a WiscKey LSM+VLog store (not yet available).
        #[arg(long, value_enum, default_value = "memory")]
        storage: StorageBackend,
    },
    /// Remove a replica for a shard.
    RemoveReplica {
        #[arg(long, default_value = "127.0.0.1:9000")]
        admin_listen_addr: String,
        #[arg(long)]
        shard: u32,
    },
    /// Leave a shard (protocol-level membership removal via shard-manager).
    Leave {
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
    /// Take a full cluster backup.
    Backup {
        /// Comma-separated admin server addresses (e.g. "127.0.0.1:9000,127.0.0.1:9001").
        #[arg(long)]
        admin_addrs: String,
        /// Output directory for backup files.
        #[arg(long)]
        output: String,
    },
    /// Restore a cluster from backup.
    Restore {
        /// Directory containing backup files (cluster.json + shard_N.json).
        #[arg(long)]
        backup_dir: String,
        /// Comma-separated admin server addresses for target nodes.
        #[arg(long)]
        admin_addrs: String,
        /// Base port for restored shard replicas (each node allocates ports sequentially from here).
        #[arg(long)]
        base_port: u16,
        /// Optional discovery URL to register restored shards.
        #[arg(long)]
        discovery_url: Option<String>,
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
            discovery_url,
            shard_manager_url,
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
            if let Some(url) = discovery_url {
                cfg.discovery_url = Some(url);
            }
            if let Some(url) = shard_manager_url {
                cfg.shard_manager_url = Some(url);
            }
            node::run(cfg).await;
        }
        Command::Admin { action } => {
            admin_client::run(action).await;
        }
        Command::Client {
            config: config_path,
            discovery_url,
            discovery_json,
            execute,
            script,
        } => {
            let mut cfg = config_path
                .as_deref()
                .map(ClientConfig::from_file)
                .unwrap_or_default();
            if let Some(url) = discovery_url {
                cfg.discovery_url = Some(url);
            }

            let input_source = if !execute.is_empty() {
                repl::InputSource::Commands(execute)
            } else if let Some(path) = script {
                repl::InputSource::File(std::path::PathBuf::from(path))
            } else {
                repl::InputSource::Stdin
            };

            let exit_code = client::run(cfg, discovery_json, input_source).await;
            std::process::exit(exit_code);
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
