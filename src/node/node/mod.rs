mod add_replica;
mod add_replica_join;
mod leave_shard;
mod remove_replica;
mod shard_manager_http;

use crate::discovery::backend::DiscoveryBackend;
use crate::discovery::{CachingShardDirectory, InMemoryShardDirectory};
use crate::{
    IrReplica, IrReplicaMetrics, ShardNumber, TapirReplica, TcpAddress, TcpTransport,
};
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

pub type TapirIrReplica = IrReplica<TapirReplica<String, String>, TcpTransport<TapirReplica<String, String>>>;

pub struct ReplicaHandle {
    pub replica: Arc<TapirIrReplica>,
    pub listen_addr: SocketAddr,
}

pub struct Node {
    pub replicas: Mutex<BTreeMap<ShardNumber, ReplicaHandle>>,
    pub(crate) persist_dir: String,
    pub(crate) directory: Arc<InMemoryShardDirectory<TcpAddress>>,
    /// Holds the CachingShardDirectory alive so its background sync task
    /// continues running. When None, no discovery sync is active.
    /// Also used to register/unregister own_shards for PUSH filtering.
    pub(crate) discovery_dir: Option<Arc<CachingShardDirectory<TcpAddress, String, DiscoveryBackend>>>,
    pub shard_manager_url: Option<String>,
    /// Factory function for creating new RNG instances.
    /// Kept out of library to avoid thread_rng() in library code.
    pub new_rng: fn() -> crate::Rng,
    #[cfg(feature = "tls")]
    pub tls_config: Option<crate::tls::TlsConfig>,
}

impl Node {
    pub fn new(persist_dir: String, new_rng: fn() -> crate::Rng) -> Self {
        Self {
            replicas: Mutex::new(BTreeMap::new()),
            persist_dir,
            directory: Arc::new(InMemoryShardDirectory::new()),
            discovery_dir: None,
            shard_manager_url: None,
            new_rng,
            #[cfg(feature = "tls")]
            tls_config: None,
        }
    }

    pub fn with_discovery_backend(
        persist_dir: String,
        backend: DiscoveryBackend,
        new_rng: fn() -> crate::Rng,
    ) -> Self {
        let directory = Arc::new(InMemoryShardDirectory::new());
        let discovery_dir = CachingShardDirectory::new(
            Arc::clone(&directory),
            Arc::new(backend),
            std::time::Duration::from_secs(10),
        );
        Self {
            replicas: Mutex::new(BTreeMap::new()),
            persist_dir,
            directory,
            discovery_dir: Some(discovery_dir),
            shard_manager_url: None,
            new_rng,
            #[cfg(feature = "tls")]
            tls_config: None,
        }
    }

    pub fn with_discovery_backend_and_shard_manager(
        persist_dir: String,
        backend: DiscoveryBackend,
        shard_manager_url: &str,
        new_rng: fn() -> crate::Rng,
    ) -> Self {
        let mut node = Self::with_discovery_backend(persist_dir, backend, new_rng);
        node.shard_manager_url = Some(shard_manager_url.to_string());
        node
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

    /// Returns the current view number of the replica hosting `shard` on this node,
    /// or None if this node has no replica for that shard.
    pub fn shard_view_number(&self, shard: ShardNumber) -> Option<u64> {
        let replicas = self.replicas.lock().unwrap();
        replicas.get(&shard).map(|h| h.replica.view_number())
    }

    pub fn shard_list(&self) -> Vec<(ShardNumber, SocketAddr)> {
        self.replicas
            .lock()
            .unwrap()
            .iter()
            .map(|(shard, handle)| (*shard, handle.listen_addr))
            .collect()
    }

    /// Collect metrics from all replicas on this node for Prometheus exposition.
    pub fn collect_metrics(&self) -> Vec<(ShardNumber, IrReplicaMetrics)> {
        self.replicas
            .lock()
            .unwrap()
            .iter()
            .filter_map(|(shard, handle)| {
                handle.replica.collect_metrics().map(|m| (*shard, m))
            })
            .collect()
    }
}

impl super::nodemetrics_server::MetricsCollector for Node {
    fn collect_metrics(&self) -> Vec<(ShardNumber, IrReplicaMetrics)> {
        Node::collect_metrics(self)
    }
}
