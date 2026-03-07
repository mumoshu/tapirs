use crate::{
    discovery::{tapir::TapirRemoteShardDirectory, InMemoryShardDirectory},
    tapir::{IO, CO, CR},
    ChannelRegistry, ChannelTransport, IrMembership, IrReplica, IrVersionedRecord, ShardNumber,
    TapirReplica,
};
use std::sync::Arc;

pub struct TestDiscoveryCluster {
    pub registry: ChannelRegistry<TapirReplica<String, String>>,
    pub directory: Arc<InMemoryShardDirectory<usize>>,
    num_replicas: usize,
    pub replicas: Vec<
        Arc<
            IrReplica<
                TapirReplica<String, String>,
                ChannelTransport<TapirReplica<String, String>>,
                IrVersionedRecord<IO<String, String>, CO<String, String>, CR>,
            >,
        >,
    >,
}

impl TestDiscoveryCluster {
    /// Gracefully shut down the discovery cluster.
    ///
    /// Drops all replica Arc references and yields to drain pending
    /// fire-and-forget tasks (spawned by `do_send`). Call this before the
    /// test function returns to prevent spawned tasks from running during
    /// tokio runtime shutdown, which causes heap corruption when tasks
    /// serialize replica state concurrently with runtime teardown.
    pub async fn shutdown(mut self) {
        // Clear strong references to replicas.  Callbacks in spawned
        // do_send tasks hold Weak references — subsequent
        // Weak::upgrade() will return None, causing those tasks to
        // exit without calling receive()/persist().
        self.replicas.clear();
        // Yield several times to let pending spawned tasks run and
        // observe the None upgrade.
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
    }

    /// Create a TapirRemoteShardDirectory client.
    pub fn create_remote(
        &self,
        rng: &mut crate::Rng,
    ) -> Arc<
        TapirRemoteShardDirectory<usize, ChannelTransport<TapirReplica<String, String>>>,
    > {
        let channel = self
            .registry
            .channel(move |_, _| None, Arc::clone(&self.directory));
        let membership = IrMembership::new((0..self.num_replicas).collect());
        Arc::new(TapirRemoteShardDirectory::new(
            rng.fork(),
            membership,
            channel,
        ))
    }
}

pub fn build_single_node_discovery(rng: &mut crate::Rng) -> TestDiscoveryCluster {
    build_test_discovery(rng, 3)
}

pub fn build_test_discovery(rng: &mut crate::Rng, num_replicas: usize) -> TestDiscoveryCluster {
    let registry = ChannelRegistry::default();
    let directory = Arc::new(InMemoryShardDirectory::new());

    let replicas = super::cluster::build_shard::<String, String>(
        rng,
        ShardNumber(0),
        false,
        num_replicas,
        &registry,
        &directory,
    );

    TestDiscoveryCluster {
        registry,
        directory,
        num_replicas,
        replicas,
    }
}
