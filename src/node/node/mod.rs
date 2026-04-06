mod add_replica;
mod add_replica_join;
mod writable_clone;
mod leave_shard;
mod remove_replica;
mod shard_manager_http;

use crate::discovery::backend::DiscoveryBackend;
use crate::discovery::{CachingShardDirectory, InMemoryShardDirectory};
use crate::{
    IrReplicaMetrics, ShardNumber, TcpAddress,
};
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

pub type TapirIrReplica = crate::store_defaults::ProductionIrReplica;
pub type S3BackedTapirIrReplica = crate::store_defaults::S3BackedIrReplica;

/// A replica can be either a production replica (DefaultDiskIo) or an
/// S3-backed replica (BufferedIo with S3 cache). Both support the same
/// IrReplica methods but are different generic instantiations.
pub enum AnyReplica {
    Production(Arc<TapirIrReplica>),
    S3Backed(Arc<S3BackedTapirIrReplica>),
}

impl AnyReplica {
    pub fn force_view_change(&self) {
        match self {
            Self::Production(r) => r.force_view_change(),
            Self::S3Backed(r) => r.force_view_change(),
        }
    }

    pub fn view_number(&self) -> u64 {
        match self {
            Self::Production(r) => r.view_number(),
            Self::S3Backed(r) => r.view_number(),
        }
    }

    pub fn collect_metrics(&self) -> Option<IrReplicaMetrics> {
        match self {
            Self::Production(r) => r.collect_metrics(),
            Self::S3Backed(r) => r.collect_metrics(),
        }
    }
}

pub struct ReplicaHandle {
    pub replica: AnyReplica,
    pub listen_addr: SocketAddr,
}

pub struct Node {
    pub(crate) replicas: Mutex<BTreeMap<ShardNumber, ReplicaHandle>>,
    pub(crate) persist_dir: String,
    pub(crate) directory: Arc<InMemoryShardDirectory<TcpAddress>>,
    /// Holds the CachingShardDirectory alive so its background sync task
    /// continues running. When None, no discovery sync is active.
    /// Also used to register/unregister own_shards for PUSH filtering.
    pub(crate) discovery_dir: Option<Arc<CachingShardDirectory<TcpAddress, String, DiscoveryBackend>>>,
    pub(crate) shard_manager_url: Option<String>,
    /// Factory function for creating new RNG instances.
    /// Kept out of library to avoid thread_rng() in library code.
    pub(crate) new_rng: fn() -> crate::Rng,
    #[cfg(feature = "tls")]
    pub(crate) tls_config: Option<crate::tls::TlsConfig>,
    pub(crate) s3_config: Option<crate::remote_store::config::S3StorageConfig>,
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
            s3_config: None,
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
            s3_config: None,
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

    pub fn set_shard_manager_url(&mut self, url: String) {
        self.shard_manager_url = Some(url);
    }

    #[cfg(feature = "tls")]
    pub fn set_tls_config(&mut self, config: Option<crate::tls::TlsConfig>) {
        self.tls_config = config;
    }

    pub fn set_s3_config(&mut self, config: Option<crate::remote_store::config::S3StorageConfig>) {
        self.s3_config = config;
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
