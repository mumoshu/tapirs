use crate::mvcc::disk::{DiskStore, memory_io::MemoryIo};
use crate::tapir::{Key, Timestamp, Value};
use crate::{
    discovery::{InMemoryShardDirectory, ShardDirectory as _},
    ChannelRegistry, ChannelTransport, IrMembership, IrReplica, ShardNumber, TapirClient,
    TapirReplica,
};
use std::sync::Arc;

pub fn build_shard<K: Key, V: Value>(
    rng: &mut crate::Rng,
    shard: ShardNumber,
    linearizable: bool,
    num_replicas: usize,
    registry: &ChannelRegistry<TapirReplica<K, V>>,
    directory: &Arc<InMemoryShardDirectory<usize>>,
) -> Vec<Arc<IrReplica<TapirReplica<K, V>, ChannelTransport<TapirReplica<K, V>>>>> {
    let initial_address = registry.len();
    let membership = IrMembership::new(
        (0..num_replicas)
            .map(|n| n + initial_address)
            .collect::<Vec<_>>(),
    );

    let replicas = (0..num_replicas)
        .map(|_| {
            let replica_rng = rng.fork();
            let dir = Arc::clone(directory);
            let m = membership.clone();
            Arc::new_cyclic(
                |weak: &std::sync::Weak<
                    IrReplica<TapirReplica<K, V>, ChannelTransport<TapirReplica<K, V>>>,
                >| {
                    let weak = weak.clone();
                    let channel = registry.channel(
                        move |from, message| weak.upgrade()?.receive(from, message),
                        Arc::clone(&dir),
                    );
                    channel.set_shard(shard);
                    let backend = DiskStore::<K, V, Timestamp, MemoryIo>::open(
                        MemoryIo::temp_path(),
                    ).unwrap();
                    let upcalls = TapirReplica::new_with_backend(shard, linearizable, backend);
                    IrReplica::new(
                        replica_rng,
                        m.clone(),
                        upcalls,
                        channel,
                        Some(TapirReplica::tick),
                    )
                },
            )
        })
        .collect::<Vec<_>>();

    directory.put(shard, membership, 0);
    replicas
}

pub fn build_clients<K: Key, V: Value>(
    rng: &mut crate::Rng,
    num_clients: usize,
    registry: &ChannelRegistry<TapirReplica<K, V>>,
    directory: &Arc<InMemoryShardDirectory<usize>>,
) -> Vec<Arc<TapirClient<K, V, ChannelTransport<TapirReplica<K, V>>>>> {
    (0..num_clients)
        .map(|_| {
            let channel = registry.channel(move |_, _| unreachable!(), Arc::clone(directory));
            Arc::new(TapirClient::new(rng.fork(), channel))
        })
        .collect()
}
