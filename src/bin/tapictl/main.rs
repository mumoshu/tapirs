mod shard_manager_client;

use clap::{Parser, Subcommand};
use shard_manager_client::HttpShardManagerClient;

#[derive(Parser)]
#[command(name = "tapictl", about = "TAPIR cluster control CLI")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Split a shard at a key boundary.
    Split {
        #[command(subcommand)]
        resource: SplitResource,
    },
    /// Merge two adjacent shards.
    Merge {
        #[command(subcommand)]
        resource: MergeResource,
    },
    /// Compact a shard onto fresh replicas.
    Compact {
        #[command(subcommand)]
        resource: CompactResource,
    },
    /// Create a cluster resource.
    Create {
        #[command(subcommand)]
        resource: CreateResource,
    },
    /// Get cluster information.
    Get {
        #[command(subcommand)]
        resource: GetResource,
    },
    /// Operations via direct node access (no ShardManager required).
    ///
    /// These commands communicate directly with node admin APIs to discover
    /// shard membership and manage replicas. No running ShardManager server
    /// is needed. Use for standalone clusters (e.g. the discovery store)
    /// where no ShardManager exists.
    Solo {
        #[command(subcommand)]
        command: SoloCommand,
    },
}

#[derive(Subcommand)]
enum SplitResource {
    /// Split a shard into two at the given key.
    Shard {
        /// Shard-manager URL.
        #[arg(long, default_value = "http://127.0.0.1:9001")]
        shard_manager_url: String,
        /// Source shard number to split.
        #[arg(long)]
        source: u32,
        /// Key at which to split the shard.
        #[arg(long)]
        split_key: String,
        /// Shard number for the new (right) shard.
        #[arg(long)]
        new_shard: u32,
        /// Comma-separated replica addresses for the new shard.
        #[arg(long)]
        new_replicas: String,
    },
}

#[derive(Subcommand)]
enum MergeResource {
    /// Merge an absorbed shard into a surviving shard.
    Shard {
        /// Shard-manager URL.
        #[arg(long, default_value = "http://127.0.0.1:9001")]
        shard_manager_url: String,
        /// Shard number to absorb (will be removed).
        #[arg(long)]
        absorbed: u32,
        /// Shard number that survives (absorbs data).
        #[arg(long)]
        surviving: u32,
    },
}

#[derive(Subcommand)]
enum SoloCommand {
    /// Clone a shard from one cluster to another using CDC.
    ///
    /// Queries source nodes' admin API to discover shard membership,
    /// creates destination replicas via admin API, then runs a 3-phase
    /// CDC copy (bulk copy, catch-up, freeze+drain+transfer read protection).
    /// Used for blue-green compaction of standalone clusters.
    Clone {
        /// Comma-separated admin API addresses of source nodes (host:port).
        #[arg(long)]
        source_nodes_admin_addrs: String,
        /// Shard number on source nodes.
        #[arg(long)]
        source_shard: u32,
        /// Comma-separated admin API addresses of destination nodes (host:port).
        #[arg(long)]
        dest_nodes_admin_addrs: String,
        /// Shard number to create on destination nodes.
        #[arg(long)]
        dest_shard: u32,
        /// Base TAPIR protocol port for destination replicas.
        #[arg(long)]
        dest_base_port: u16,
    },
}

#[derive(Subcommand)]
enum CompactResource {
    /// Compact a shard onto fresh replicas.
    Shard {
        /// Shard-manager URL.
        #[arg(long, default_value = "http://127.0.0.1:9001")]
        shard_manager_url: String,
        /// Source shard to compact from.
        #[arg(long)]
        source: u32,
        /// Shard number for the compacted shard.
        #[arg(long)]
        new_shard: u32,
        /// Comma-separated replica addresses for the new shard.
        #[arg(long)]
        new_replicas: String,
    },
}

#[derive(Subcommand)]
enum CreateResource {
    /// Register a shard with the shard-manager.
    ///
    /// Tells the shard-manager about a shard's key range so it can manage
    /// routing. The shard's replicas must already exist (created via
    /// `tapi admin add-replica`).
    Shard {
        /// Shard-manager URL.
        #[arg(long, default_value = "http://127.0.0.1:9001")]
        shard_manager_url: String,
        /// Shard number to register.
        #[arg(long)]
        shard: u32,
        /// Start of the key range (inclusive). Omit for unbounded start.
        #[arg(long)]
        key_range_start: Option<String>,
        /// End of the key range (exclusive). Omit for unbounded end.
        #[arg(long)]
        key_range_end: Option<String>,
        /// Comma-separated replica addresses. When provided, the server
        /// uses these directly instead of querying the discovery cluster.
        #[arg(long)]
        replicas: Option<String>,
    },
}

#[derive(Subcommand)]
enum GetResource {
    /// Show cluster topology from the TAPIR discovery cluster.
    ///
    /// Queries the discovery cluster for all registered shards and
    /// their replica membership.
    Topology {
        /// TAPIR discovery cluster endpoint.
        ///
        /// Static: comma-separated replica addresses (e.g. "10.0.0.1:6000,10.0.0.2:6000").
        /// DNS: "srv://hostname:port" for DNS resolution.
        #[arg(long)]
        discovery_tapir_endpoint: String,
    },
}

async fn get_topology(endpoint: &str) -> Result<(), String> {
    use rand::{thread_rng, Rng as _};
    use tapirs::discovery::{tapir, RemoteShardDirectory};
    use tapirs::{TcpAddress, TcpTransport, TapirReplica};

    let ephemeral_addr = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
        let a = l.local_addr().unwrap();
        drop(l);
        TcpAddress(a)
    };
    let disc_dir = std::sync::Arc::new(tapirs::discovery::InMemoryShardDirectory::new());
    let persist_dir = format!("/tmp/tapictl_topology_{}", std::process::id());
    let persist_dir_path = persist_dir.clone();
    let disc_transport: TcpTransport<TapirReplica<String, String>> =
        TcpTransport::with_directory(ephemeral_addr, persist_dir, disc_dir);

    let rng = tapirs::Rng::from_seed(thread_rng().r#gen());
    let dir = tapir::parse_tapir_endpoint::<TcpAddress, _>(
        endpoint,
        tapir::ReadMode::Eventual,
        disc_transport,
        rng,
    )
    .await
    .map_err(|e| format!("failed to connect to discovery: {e}"))?;

    let entries =
        <_ as RemoteShardDirectory<TcpAddress, String>>::all(&dir)
            .await
            .map_err(|e| format!("failed to fetch topology: {e}"))?;

    if entries.is_empty() {
        println!("No shards registered in discovery.");
    } else {
        println!("{:<8} {:<8} {}", "SHARD", "VIEW", "REPLICAS");
        for (shard, membership, view) in entries {
            let addrs = tapirs::discovery::membership_to_strings(&membership);
            println!("{:<8} {:<8} {}", shard.0, view, addrs.join(", "));
        }
    }

    // Clean up temporary persist directory.
    let _ = std::fs::remove_dir_all(&persist_dir_path);

    Ok(())
}

fn main() {
    let cli = Cli::parse();
    let result: Result<(), String> = match cli.command {
        Command::Split {
            resource:
                SplitResource::Shard {
                    shard_manager_url,
                    source,
                    split_key,
                    new_shard,
                    new_replicas,
                },
        } => {
            let client = HttpShardManagerClient::new(&shard_manager_url);
            let replicas: Vec<String> =
                new_replicas.split(',').map(|s| s.trim().to_string()).collect();
            client
                .split(source, &split_key, new_shard, &replicas)
                .map(|()| {
                    println!(
                        "Split shard {source} at key \"{split_key}\" -> new shard {new_shard}"
                    )
                })
        }
        Command::Merge {
            resource:
                MergeResource::Shard {
                    shard_manager_url,
                    absorbed,
                    surviving,
                },
        } => {
            let client = HttpShardManagerClient::new(&shard_manager_url);
            client
                .merge(absorbed, surviving)
                .map(|()| println!("Merged shard {absorbed} into shard {surviving}"))
        }
        Command::Compact {
            resource:
                CompactResource::Shard {
                    shard_manager_url,
                    source,
                    new_shard,
                    new_replicas,
                },
        } => {
            let client = HttpShardManagerClient::new(&shard_manager_url);
            let replicas: Vec<String> =
                new_replicas.split(',').map(|s| s.trim().to_string()).collect();
            client
                .compact(source, new_shard, &replicas)
                .map(|()| println!("Compacted shard {source} -> new shard {new_shard}"))
        }
        Command::Create {
            resource:
                CreateResource::Shard {
                    shard_manager_url,
                    shard,
                    key_range_start,
                    key_range_end,
                    replicas,
                },
        } => {
            let client = HttpShardManagerClient::new(&shard_manager_url);
            let replica_addrs: Option<Vec<String>> = replicas.map(|r| {
                r.split(',').map(|s| s.trim().to_string()).collect()
            });
            client
                .register(
                    shard,
                    key_range_start.as_deref(),
                    key_range_end.as_deref(),
                    replica_addrs.as_deref(),
                )
                .map(|()| println!("Registered shard {shard}"))
        }
        Command::Get {
            resource:
                GetResource::Topology {
                    discovery_tapir_endpoint,
                },
        } => {
            let rt = tokio::runtime::Runtime::new().expect("create tokio runtime");
            rt.block_on(async {
                get_topology(&discovery_tapir_endpoint).await
            })
        }
        Command::Solo {
            command:
                SoloCommand::Clone {
                    source_nodes_admin_addrs,
                    source_shard,
                    dest_nodes_admin_addrs,
                    dest_shard,
                    dest_base_port,
                },
        } => {
            let source_addrs: Vec<String> = source_nodes_admin_addrs
                .split(',')
                .map(|s| s.trim().to_string())
                .collect();
            let dest_addrs: Vec<String> = dest_nodes_admin_addrs
                .split(',')
                .map(|s| s.trim().to_string())
                .collect();
            let rt = tokio::runtime::Runtime::new().expect("create tokio runtime");
            rt.block_on(async {
                use rand::{thread_rng, Rng as _};
                let rng = tapirs::Rng::from_seed(thread_rng().r#gen());
                let mut mgr = tapirs::SoloClusterManager::new(rng);
                mgr.set_progress_callback(|phase| {
                    eprintln!("[solo clone] {phase}");
                });
                mgr.clone_shard_direct(
                    &source_addrs,
                    tapirs::ShardNumber(source_shard),
                    &dest_addrs,
                    tapirs::ShardNumber(dest_shard),
                    dest_base_port,
                )
                .await
                .map_err(|e| format!("{e:?}"))
                .map(|()| {
                    println!(
                        "Cloned shard {} -> shard {} on destination nodes",
                        source_shard, dest_shard
                    )
                })
            })
        }
    };

    if let Err(e) = result {
        eprintln!("Error: {e}");
        std::process::exit(1);
    }
}
