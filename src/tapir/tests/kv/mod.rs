mod add_replica;
mod cdc;
mod clone_shard;
mod compact;
mod coordinator_recovery;
mod fuzz;
mod increment;
mod read_only;
mod resharding;
mod rwr;
mod throughput;

use super::fuzz_event_log::{FuzzEvent, FuzzEventLog};
use super::invariant_checker::{InvariantChecker, TxnOutcome, TxnRecord};
use crate::{
    discovery::{
        CachingShardDirectory, InMemoryRemoteDirectory, InMemoryShardDirectory,
        RemoteShardDirectory as _, ShardDirectory as _,
    },
    mvcc::disk::{DiskStore, memory_io::MemoryIo},
    tapir::dynamic_router::{DynamicRouter, ShardDirectory, ShardEntry},
    tapir::key_range::KeyRange,
    tapir::Sharded,
    tapir::Timestamp,
    transport::{FaultyChannelTransport, LatencyConfig, NetworkFaultConfig},
    ChannelRegistry, ChannelTransport, IrMembership, IrReplica, RoutingClient, ShardNumber,
    TapirClient, TapirReplica, TapirTimestamp, Transport as _,
};
use futures::future::join_all;
use rand::{rngs::StdRng, seq::SliceRandom, Rng, SeedableRng};
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex, RwLock,
    },
    time::Duration,
};
use tokio::time::timeout;

type K = i64;
type V = i64;
type Transport = ChannelTransport<TapirReplica<K, V>>;
type FaultyTransport = FaultyChannelTransport<TapirReplica<K, V>>;

fn test_rng(seed: u64) -> crate::Rng {
    crate::Rng::from_seed(seed)
}

fn init_tracing() {
    let _ = tracing::subscriber::set_global_default(
        tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .finish(),
    );
}

fn build_shard(
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

    fn create_replica(
        rng: &mut crate::Rng,
        registry: &ChannelRegistry<TapirReplica<K, V>>,
        directory: &Arc<InMemoryShardDirectory<usize>>,
        shard: ShardNumber,
        membership: &IrMembership<usize>,
        linearizable: bool,
    ) -> Arc<IrReplica<TapirReplica<K, V>, ChannelTransport<TapirReplica<K, V>>>> {
        let backend = DiskStore::<K, V, Timestamp, MemoryIo>::open(
            MemoryIo::temp_path(),
        ).unwrap();
        Arc::new_cyclic(
            |weak: &std::sync::Weak<
                IrReplica<TapirReplica<K, V>, ChannelTransport<TapirReplica<K, V>>>,
            >| {
                let weak = weak.clone();
                let channel =
                    registry.channel(move |from, message| weak.upgrade()?.receive(from, message), Arc::clone(directory));
                channel.set_shard(shard);
                let upcalls = TapirReplica::new_with_backend(shard, linearizable, backend);
                IrReplica::new(
                    rng.fork(),
                    membership.clone(),
                    upcalls,
                    channel,
                    Some(TapirReplica::tick),
                )
            },
        )
    }

    let replicas = (0..num_replicas)
        .map(|_| create_replica(rng, &registry, directory, shard, &membership, linearizable))
        .collect::<Vec<_>>();

    directory.put(shard, membership.clone(), 0);

    replicas
}

fn build_clients(
    rng: &mut crate::Rng,
    num_clients: usize,
    registry: &ChannelRegistry<TapirReplica<K, V>>,
    directory: &Arc<InMemoryShardDirectory<usize>>,
) -> Vec<Arc<TapirClient<K, V, ChannelTransport<TapirReplica<K, V>>>>> {
    fn create_client(
        rng: &mut crate::Rng,
        registry: &ChannelRegistry<TapirReplica<K, V>>,
        directory: &Arc<InMemoryShardDirectory<usize>>,
    ) -> Arc<TapirClient<K, V, ChannelTransport<TapirReplica<K, V>>>> {
        let channel = registry.channel(move |_, _| unreachable!(), Arc::clone(directory));
        Arc::new(TapirClient::new(rng.fork(), channel))
    }

    let clients = (0..num_clients)
        .map(|_| create_client(rng, &registry, directory))
        .collect::<Vec<_>>();

    clients
}

fn build_kv(
    linearizable: bool,
    num_replicas: usize,
    num_clients: usize,
) -> (
    Vec<Arc<IrReplica<TapirReplica<K, V>, ChannelTransport<TapirReplica<K, V>>>>>,
    Vec<Arc<TapirClient<K, V, ChannelTransport<TapirReplica<K, V>>>>>,
) {
    let (mut shards, clients) = build_sharded_kv(linearizable, 1, num_replicas, num_clients);
    (shards.remove(0), clients)
}

fn build_sharded_kv(
    linearizable: bool,
    num_shards: usize,
    num_replicas: usize,
    num_clients: usize,
) -> (
    Vec<Vec<Arc<IrReplica<TapirReplica<K, V>, ChannelTransport<TapirReplica<K, V>>>>>>,
    Vec<Arc<TapirClient<K, V, ChannelTransport<TapirReplica<K, V>>>>>,
) {
    init_tracing();

    println!("---------------------------");
    println!(" linearizable={linearizable} num_shards={num_shards} num_replicas={num_replicas}");
    println!("---------------------------");

    let registry = ChannelRegistry::default();
    let dir = Arc::new(InMemoryShardDirectory::new());
    let mut rng = test_rng(42);

    let mut shards = Vec::new();
    for shard in 0..num_shards {
        let replicas = build_shard(
            &mut rng,
            ShardNumber(shard as u32),
            linearizable,
            num_replicas,
            &registry,
            &dir,
        );
        shards.push(replicas);
    }

    let clients = build_clients(&mut rng, num_clients, &registry, &dir);

    (shards, clients)
}
