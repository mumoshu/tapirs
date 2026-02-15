use crate::config::{NodeConfig, ReplicaConfig};
use crate::discovery::HttpDiscoveryClient;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tapirs::discovery::{DiscoveryShardDirectory, InMemoryShardDirectory};
use tapirs::{
    IrMembership, IrReplica, ShardNumber, TapirReplica, TcpAddress, TcpTransport,
};

type TapirIrReplica = IrReplica<TapirReplica<String, String>, TcpTransport<TapirReplica<String, String>>>;

pub struct ReplicaHandle {
    pub replica: Arc<TapirIrReplica>,
    pub listen_addr: std::net::SocketAddr,
}

pub struct Node {
    pub replicas: Mutex<HashMap<ShardNumber, ReplicaHandle>>,
    persist_dir: String,
    directory: Arc<InMemoryShardDirectory<TcpAddress>>,
    // Holds the DiscoveryShardDirectory alive so its background sync task
    // continues running. When None, no discovery sync is active.
    _discovery_dir: Option<Arc<DiscoveryShardDirectory<TcpAddress, HttpDiscoveryClient>>>,
}

impl Node {
    fn new(persist_dir: String) -> Self {
        Self {
            replicas: Mutex::new(HashMap::new()),
            persist_dir,
            directory: Arc::new(InMemoryShardDirectory::new()),
            _discovery_dir: None,
        }
    }

    fn with_discovery(persist_dir: String, discovery_url: &str) -> Self {
        let directory = Arc::new(InMemoryShardDirectory::new());
        let client = Arc::new(HttpDiscoveryClient::new(discovery_url));
        let discovery_dir = DiscoveryShardDirectory::new(
            Arc::clone(&directory),
            client,
            std::time::Duration::from_secs(10),
        );
        Self {
            replicas: Mutex::new(HashMap::new()),
            persist_dir,
            directory,
            _discovery_dir: Some(discovery_dir),
        }
    }

    pub async fn add_replica(&self, cfg: &ReplicaConfig) {
        let shard = ShardNumber(cfg.shard);
        let listen_addr: std::net::SocketAddr = cfg
            .listen_addr
            .parse()
            .unwrap_or_else(|e| panic!("invalid listen_addr '{}': {e}", cfg.listen_addr));

        let membership_addrs: Vec<TcpAddress> = cfg
            .membership
            .iter()
            .map(|a| {
                TcpAddress(
                    a.parse()
                        .unwrap_or_else(|e| panic!("invalid membership addr '{a}': {e}")),
                )
            })
            .collect();
        let membership = IrMembership::new(membership_addrs);

        let persist_dir = format!("{}/shard_{}", self.persist_dir, cfg.shard);
        let address = TcpAddress(listen_addr);
        let transport =
            TcpTransport::with_directory(address, persist_dir, Arc::clone(&self.directory));

        // Populate shard directory so TapirTransport::shard_addresses works.
        transport.set_shard_addresses(shard, membership.clone());

        // If discovery is configured, register this shard for push.
        if let Some(ref dir) = self._discovery_dir {
            dir.add_own_shard(shard);
        }

        // Start listener BEFORE creating replica (IrReplica::new starts tick tasks).
        transport
            .listen(listen_addr)
            .await
            .unwrap_or_else(|e| panic!("failed to listen on {listen_addr}: {e}"));

        let transport_for_replica = transport.clone();
        let replica = Arc::new_cyclic(|weak: &std::sync::Weak<TapirIrReplica>| {
            let weak = weak.clone();
            transport_for_replica.set_receive_callback(move |from, message| {
                weak.upgrade()?.receive(from, message)
            });
            let upcalls = TapirReplica::new(shard, false);
            IrReplica::new(membership, upcalls, transport_for_replica.clone(), Some(TapirReplica::tick))
        });

        tracing::info!(?shard, %listen_addr, "replica started");

        self.replicas.lock().unwrap().insert(
            shard,
            ReplicaHandle {
                replica,
                listen_addr,
            },
        );
    }

    pub fn remove_replica(&self, shard: ShardNumber) -> bool {
        let removed = self.replicas.lock().unwrap().remove(&shard);
        if removed.is_some() {
            // Remove from discovery push list. Discovery deregistration
            // happens on the next background sync cycle.
            if let Some(ref dir) = self._discovery_dir {
                dir.remove_own_shard(shard);
            }
            tracing::info!(?shard, "replica removed");
            true
        } else {
            false
        }
    }

    pub fn force_view_change(&self, shard: ShardNumber) -> bool {
        let replicas = self.replicas.lock().unwrap();
        if let Some(handle) = replicas.get(&shard) {
            handle.replica.force_view_change();
            tracing::info!(?shard, "view change triggered");
            true
        } else {
            false
        }
    }

    pub fn shard_list(&self) -> Vec<(ShardNumber, std::net::SocketAddr)> {
        self.replicas
            .lock()
            .unwrap()
            .iter()
            .map(|(shard, handle)| (*shard, handle.listen_addr))
            .collect()
    }
}

pub async fn run(cfg: NodeConfig) {
    let persist_dir = cfg
        .persist_dir
        .unwrap_or_else(|| "/tmp/tapi".to_string());
    let admin_listen_addr = cfg
        .admin_listen_addr
        .unwrap_or_else(|| "127.0.0.1:9000".to_string());

    let node = if let Some(ref discovery_url) = cfg.discovery_url {
        Arc::new(Node::with_discovery(persist_dir, discovery_url))
    } else {
        Arc::new(Node::new(persist_dir))
    };

    for replica_cfg in &cfg.replicas {
        node.add_replica(replica_cfg).await;
    }

    let admin_addr: std::net::SocketAddr = admin_listen_addr
        .parse()
        .unwrap_or_else(|e| panic!("invalid admin_listen_addr '{admin_listen_addr}': {e}"));

    crate::admin_server::start(admin_addr, Arc::clone(&node)).await;

    tracing::info!(%admin_listen_addr, "node ready, press Ctrl-C to stop");

    tokio::signal::ctrl_c()
        .await
        .expect("failed to listen for Ctrl-C");
    tracing::info!("shutting down");
}
