mod admin_client;
mod admin_server;
mod client;
mod config;
mod discovery_backend;
mod metrics_server;
mod node;
mod repl;
mod shard_manager_server;

#[cfg(test)]
mod helpers;
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
        /// Address for the Prometheus metrics endpoint (/metrics).
        #[arg(long)]
        metrics_listen_addr: Option<String>,
        #[arg(long)]
        persist_dir: Option<String>,
        /// Path to a JSON file describing shard topology for discovery.
        ///
        /// Same format as `tapi client --discovery-json`. Used as
        /// CachingShardDirectory's remote backend via DiscoveryBackend::Json.
        #[arg(long)]
        discovery_json: Option<String>,
        /// TAPIR discovery cluster endpoint.
        ///
        /// Static: comma-separated replica addresses (e.g. "10.0.0.1:6000,10.0.0.2:6000").
        /// DNS: "srv://hostname:port" for periodic re-resolution.
        /// Uses eventual consistent reads (unlogged scan) for shard discovery.
        #[arg(long)]
        discovery_tapir_endpoint: Option<String>,
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

        /// Path to a JSON file describing shard topology.
        ///
        /// Static mode (explicit addresses):
        ///   {"shards":[{"number":0,"membership":["addr:port",...],"key_range_end":"n"}]}
        ///
        /// DNS mode (headless service, resolved at startup):
        ///   {"shards":[{"number":0,"headless_service":"svc.ns:port"}]}
        #[arg(long)]
        discovery_json: Option<String>,

        /// TAPIR discovery cluster endpoint.
        ///
        /// Static: comma-separated replica addresses (e.g. "10.0.0.1:6000,10.0.0.2:6000").
        /// DNS: "srv://hostname:port" for periodic re-resolution.
        /// Reads shard topology via unlogged scan (eventual consistency).
        #[arg(long)]
        discovery_tapir_endpoint: Option<String>,

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
    /// Run a standalone shard manager server.
    ShardManager {
        /// Address to listen on.
        #[arg(long, default_value = "127.0.0.1:9001")]
        listen_addr: String,
        /// TAPIR discovery cluster endpoint.
        ///
        /// Static: comma-separated replica addresses (e.g. "10.0.0.1:6000,10.0.0.2:6000").
        /// DNS: "srv://hostname:port" for periodic re-resolution.
        /// Uses linearizable reads (RO transactions) for shard authority.
        #[arg(long)]
        discovery_tapir_endpoint: Option<String>,
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
    ///
    /// Two modes:
    ///
    /// 1. Without --membership (shard-manager mode): The node registers with
    ///    the shard-manager via POST /v1/join. The shard-manager looks up the
    ///    shard's current membership from TAPIR discovery and coordinates
    ///    adding this replica to the existing group. Use this when adding a
    ///    replica to an existing shard that is already registered in discovery
    ///    (e.g. adding a 4th replica to an existing 3-replica shard).
    ///
    /// 2. With --membership (static mode): Creates the replica with the exact
    ///    membership list provided, bypassing the shard-manager entirely. Use
    ///    this for initial shard bootstrap when the shard doesn't exist in
    ///    discovery yet (e.g. creating shard 2 replicas before a shard split).
    ///    All replicas in the group should be created with the same membership
    ///    list so they form a quorum.
    //
    // TODO: Once the shard-manager becomes the authority for strictly
    // consistent shard membership state, the shard-manager mode should be
    // able to handle initial bootstrap too — the shard-manager would create
    // the shard entry in discovery on the first /v1/join for an unknown
    // shard, then subsequent joins would add to the existing group. This
    // would eliminate the need for --membership entirely, simplifying the
    // operator workflow to always use add-replica without flags.
    //
    // Current state: create_replica() (no --membership) calls
    // shard_manager_join() which POSTs /v1/join. The /v1/join handler queries
    // discovery for the shard's membership — if the shard isn't found, it
    // returns 400 "shard N not found in discovery".
    //
    // Alternative approach: `tapictl create shard --shard N --replicas ...`
    // can register a shard with the shard-manager (POST /v1/register), which
    // publishes to discovery. After that, add-replica without --membership
    // would work. But register_shard requires replicas to already exist
    // (chicken-and-egg), so the first replica still needs --membership for
    // static bootstrap. The fix is to have /v1/join auto-register the shard
    // with a single-member membership on the first call, then use the
    // existing join path for subsequent replicas.
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
        /// Static membership addresses (comma-separated). When provided,
        /// creates the replica with explicit membership (no shard-manager).
        /// Required for initial shard bootstrap (shard not yet in discovery).
        /// Example: --membership 10.0.0.1:6000,10.0.0.2:6000,10.0.0.3:6000
        #[arg(long, value_delimiter = ',')]
        membership: Vec<String>,
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
    /// Wait for a node's admin server to become reachable.
    WaitReady {
        #[arg(long, default_value = "127.0.0.1:9000")]
        admin_listen_addr: String,
        /// Timeout in seconds.
        #[arg(long, default_value = "60")]
        timeout: u64,
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
            metrics_listen_addr,
            persist_dir,
            discovery_json,
            discovery_tapir_endpoint,
            shard_manager_url,
        } => {
            let mut cfg = config_path
                .as_deref()
                .map(NodeConfig::from_file)
                .unwrap_or_default();
            if let Some(addr) = admin_listen_addr {
                cfg.admin_listen_addr = Some(addr);
            }
            if let Some(addr) = metrics_listen_addr {
                cfg.metrics_listen_addr = Some(addr);
            }
            if let Some(dir) = persist_dir {
                cfg.persist_dir = Some(dir);
            }
            if let Some(url) = shard_manager_url {
                cfg.shard_manager_url = Some(url);
            }
            node::run(cfg, discovery_json, discovery_tapir_endpoint).await;
        }
        Command::Admin { action } => {
            admin_client::run(action).await;
        }
        Command::Client {
            config: config_path,
            discovery_json,
            discovery_tapir_endpoint,
            execute,
            script,
        } => {
            let cfg = config_path
                .as_deref()
                .map(ClientConfig::from_file)
                .unwrap_or_default();

            let input_source = if !execute.is_empty() {
                repl::InputSource::Commands(execute)
            } else if let Some(path) = script {
                repl::InputSource::File(std::path::PathBuf::from(path))
            } else {
                repl::InputSource::Stdin
            };

            let exit_code =
                client::run(cfg, discovery_json, discovery_tapir_endpoint, input_source).await;
            std::process::exit(exit_code);
        }
        Command::ShardManager {
            listen_addr,
            discovery_tapir_endpoint,
        } => {
            shard_manager_server::run(listen_addr, discovery_tapir_endpoint).await;
        }
    }
}
