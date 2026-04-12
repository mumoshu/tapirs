use crate::storage::io::memory_io::MemoryIo;
use crate::storage::io::disk_io::OpenFlags;
use crate::tapir::{Key, Value};
use crate::storage::combined::CombinedStoreInner;
use crate::storage::combined::record_handle::CombinedRecordHandle;
use crate::{
    discovery::{InMemoryShardDirectory, ShardDirectory as _},
    ChannelRegistry, ChannelTransport, IrMembership, IrReplica, ShardNumber,
    TapirClient, TapirReplica,
};
use std::sync::Arc;

pub type TestIrRecordStore<K, V> = CombinedRecordHandle<K, V, MemoryIo>;
pub type TestIrReplica<K, V> = IrReplica<TapirReplica<K, V>, ChannelTransport<TapirReplica<K, V>>, TestIrRecordStore<K, V>>;

pub fn build_shard<K: Key, V: Value>(
    rng: &mut crate::Rng,
    shard: ShardNumber,
    linearizable: bool,
    num_replicas: usize,
    registry: &ChannelRegistry<TapirReplica<K, V>>,
    directory: &Arc<InMemoryShardDirectory<usize>>,
) -> Vec<Arc<TestIrReplica<K, V>>> {
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
                |weak: &std::sync::Weak<TestIrReplica<K, V>>| {
                    let weak = weak.clone();
                    let channel = registry.channel(
                        move |from, message| weak.upgrade()?.receive(from, message),
                        Arc::clone(&dir),
                    );
                    channel.set_shard(shard);
                    let io_flags = OpenFlags {
                        create: true,
                        direct: false,
                    };
                    let inner = CombinedStoreInner::<K, V, MemoryIo>::open(
                        &MemoryIo::temp_path(),
                        io_flags,
                        shard,
                        linearizable,
                    ).unwrap();
                    let record_handle = inner.into_record_handle();
                    let tapir_handle = record_handle.tapir_handle();
                    let upcalls = TapirReplica::new_with_store(tapir_handle);
                    IrReplica::new(
                        replica_rng,
                        m.clone(),
                        upcalls,
                        channel,
                        Some(TapirReplica::tick),
                        record_handle,
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
