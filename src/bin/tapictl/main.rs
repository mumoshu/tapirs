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

fn main() {
    let cli = Cli::parse();
    let result = match cli.command {
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
    };

    if let Err(e) = result {
        eprintln!("Error: {e}");
        std::process::exit(1);
    }
}
