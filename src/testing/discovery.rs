use crate::{
    discovery::{tapir::TapirRemoteShardDirectory, InMemoryShardDirectory},
    ChannelRegistry, ChannelTransport, IrMembership, IrReplica, ShardNumber, TapirReplica,
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
            >,
        >,
    >,
}

impl TestDiscoveryCluster {
    /// Create a TapirRemoteShardDirectory client with eventual consistent reads.
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
        Arc::new(TapirRemoteShardDirectory::with_eventual_consistent_read(
            rng.fork(),
            membership,
            channel,
        ))
    }

    /// Create a TapirRemoteShardDirectory client with strong consistent reads.
    pub fn create_remote_strong(
        &self,
        rng: &mut crate::Rng,
    ) -> Arc<
        TapirRemoteShardDirectory<usize, ChannelTransport<TapirReplica<String, String>>>,
    > {
        let channel = self
            .registry
            .channel(move |_, _| None, Arc::clone(&self.directory));
        let membership = IrMembership::new((0..self.num_replicas).collect());
        Arc::new(TapirRemoteShardDirectory::with_strong_consistent_read(
            rng.fork(),
            membership,
            channel,
        ))
    }
}

pub fn build_single_node_discovery(rng: &mut crate::Rng) -> TestDiscoveryCluster {
    build_test_discovery(rng, 1)
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
