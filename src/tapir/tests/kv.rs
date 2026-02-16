use super::invariant_checker::{InvariantChecker, TxnOutcome, TxnRecord};
use crate::{
    discovery::{
        DiscoveryShardDirectory, InMemoryDiscovery, InMemoryShardDirectory,
        ShardDirectory as _,
    },
    tapir::dynamic_router::{DynamicRouter, ShardDirectory, ShardEntry},
    tapir::key_range::KeyRange,
    tapir::Sharded,
    transport::{FaultyChannelTransport, LatencyConfig, NetworkFaultConfig},
    ChannelRegistry, ChannelTransport, IrMembership, IrReplica, RoutingClient, ShardNumber,
    TapirClient, TapirReplica, TapirTimestamp, Transport as _,
};
use futures::future::join_all;
use rand::{rngs::StdRng, thread_rng, Rng, SeedableRng};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, RwLock,
    },
    time::Duration,
};
use tokio::time::timeout;

fn init_tracing() {
    let _ = tracing::subscriber::set_global_default(
        tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .finish(),
    );
}

type K = i64;
type V = i64;
type Transport = ChannelTransport<TapirReplica<K, V>>;
type FaultyTransport = FaultyChannelTransport<TapirReplica<K, V>>;

fn build_shard(
    shard: ShardNumber,
    linearizable: bool,
    num_replicas: usize,
    registry: &ChannelRegistry<TapirReplica<K, V>>,
) -> Vec<Arc<IrReplica<TapirReplica<K, V>, ChannelTransport<TapirReplica<K, V>>>>> {
    let initial_address = registry.len();
    let membership = IrMembership::new(
        (0..num_replicas)
            .map(|n| n + initial_address)
            .collect::<Vec<_>>(),
    );

    fn create_replica(
        registry: &ChannelRegistry<TapirReplica<K, V>>,
        shard: ShardNumber,
        membership: &IrMembership<usize>,
        linearizable: bool,
    ) -> Arc<IrReplica<TapirReplica<K, V>, ChannelTransport<TapirReplica<K, V>>>> {
        Arc::new_cyclic(
            |weak: &std::sync::Weak<
                IrReplica<TapirReplica<K, V>, ChannelTransport<TapirReplica<K, V>>>,
            >| {
                let weak = weak.clone();
                let channel =
                    registry.channel(move |from, message| weak.upgrade()?.receive(from, message));
                channel.set_shard(shard);
                let upcalls = TapirReplica::new(shard, linearizable);
                IrReplica::new(
                    membership.clone(),
                    upcalls,
                    channel,
                    Some(TapirReplica::tick),
                )
            },
        )
    }

    let replicas =
        std::iter::repeat_with(|| create_replica(&registry, shard, &membership, linearizable))
            .take(num_replicas)
            .collect::<Vec<_>>();

    registry.put_shard_addresses(shard, membership.clone());

    replicas
}

fn build_clients(
    num_clients: usize,
    registry: &ChannelRegistry<TapirReplica<K, V>>,
) -> Vec<Arc<TapirClient<K, V, ChannelTransport<TapirReplica<K, V>>>>> {
    fn create_client(
        registry: &ChannelRegistry<TapirReplica<K, V>>,
    ) -> Arc<TapirClient<K, V, ChannelTransport<TapirReplica<K, V>>>> {
        let channel = registry.channel(move |_, _| unreachable!());
        Arc::new(TapirClient::new(channel))
    }

    let clients = std::iter::repeat_with(|| create_client(&registry))
        .take(num_clients)
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

    let mut shards = Vec::new();
    for shard in 0..num_shards {
        let replicas = build_shard(
            ShardNumber(shard as u32),
            linearizable,
            num_replicas,
            &registry,
        );
        shards.push(replicas);
    }

    let clients = build_clients(num_clients, &registry);

    (shards, clients)
}

#[tokio::test]
async fn fuzz_rwr_3() {
    fuzz_rwr(3).await;
}

#[tokio::test]
async fn fuzz_rwr_5() {
    fuzz_rwr(5).await;
}

#[tokio::test]
async fn fuzz_rwr_7() {
    fuzz_rwr(7).await;
}

async fn fuzz_rwr(replicas: usize) {
    for _ in 0..16 {
        for linearizable in [false, true] {
            timeout(
                Duration::from_secs((replicas as u64 + 5) * 10),
                rwr(linearizable, replicas),
            )
            .await
            .unwrap();
        }
    }
}

async fn rwr(linearizable: bool, num_replicas: usize) {
    let (_replicas, clients) = build_kv(linearizable, num_replicas, 2);

    let txn = clients[0].begin();
    assert_eq!(txn.get(0).await, None);
    txn.put(1, Some(2));
    let first = txn.commit().await.unwrap();

    Transport::sleep(Duration::from_millis(10)).await;

    if linearizable {
        let txn = clients[1].begin();
        let result = txn.get(1).await;
        if result.is_none() {
            // We read stale data so shouldn't be able to commit.
            assert_eq!(txn.commit().await, None, "prev = {first:?}");
        } else {
            // Up to date, should be able to commit.
            //assert!(txn.commit().await.is_some());
        }
    } else {
        let txn = clients[1].begin();
        let result = txn.get(1).await;
        if let Some(commit) = txn.commit().await {
            if result.is_none() {
                assert!(commit < first, "{commit:?} {first:?}");
            } else {
                assert_eq!(result, Some(2));
                assert!(commit > first);
            }
        }
    }
}

#[tokio::test]
async fn sharded() {
    let (_shards, clients) = build_sharded_kv(true, 5, 3, 2);

    let txn = clients[0].begin();
    assert_eq!(
        txn.get(Sharded {
            shard: ShardNumber(0),
            key: 0
        })
        .await,
        None
    );
    assert_eq!(
        txn.get(Sharded {
            shard: ShardNumber(1),
            key: 0
        })
        .await,
        None
    );
    txn.put(
        Sharded {
            shard: ShardNumber(2),
            key: 0,
        },
        Some(0),
    );
    assert!(txn.commit().await.is_some());
}

#[tokio::test]
async fn increment_sequential_3() {
    increment_sequential_timeout(3).await;
}

#[tokio::test]
async fn increment_sequential_7() {
    increment_sequential_timeout(7).await;
}

async fn increment_sequential_timeout(num_replicas: usize) {
    timeout(
        Duration::from_secs((num_replicas as u64 + 10) * 10),
        increment_sequential(num_replicas),
    )
    .await
    .unwrap();
}

async fn increment_sequential(num_replicas: usize) {
    let (_replicas, clients) = build_kv(true, num_replicas, 1);

    let mut committed = 0;
    for _ in 0..10 {
        println!("^^^^^^^^^^^^^^^^^^^ BEGINNING TXN");
        let txn = clients[0].begin();
        let old = txn.get(0).await.unwrap_or_default();
        txn.put(0, Some(old + 1));
        if txn.commit().await.is_some() {
            assert_eq!(committed, old);
            committed += 1;
        }

        Transport::sleep(Duration::from_millis(1000)).await;
    }

    eprintln!("committed = {committed}");
    assert!(committed > 0);
}

#[tokio::test]
async fn increment_parallel_3() {
    increment_parallel_timeout(3).await;
}

#[tokio::test]
async fn increment_parallel_7() {
    increment_parallel_timeout(7).await;
}

async fn increment_parallel_timeout(num_replicas: usize) {
    timeout(
        Duration::from_secs((num_replicas as u64 + 10) * 10),
        increment_parallel(num_replicas),
    )
    .await
    .unwrap();
}

async fn increment_parallel(num_replicas: usize) {
    let (_replicas, clients) = build_kv(true, num_replicas, 2);

    let add = || async {
        let txn = clients[0].begin();
        let old = txn.get(0).await.unwrap_or_default();
        txn.put(0, Some(old + 1));
        txn.commit().await.is_some()
    };

    let committed = join_all((0..5).map(|_| add()))
        .await
        .into_iter()
        .filter(|ok| *ok)
        .count() as i64;

    Transport::sleep(Duration::from_secs(3)).await;

    let txn = clients[1].begin();
    let result = txn.get(0).await.unwrap_or_default();
    eprintln!("INCREMENT TEST result={result} committed={committed}");
    println!("{} {}", txn.commit().await.is_some(), result == committed);
}

#[tokio::test]
async fn throughput_3_ser() {
    throughput(false, 3, 1000).await;
}

#[tokio::test]
async fn throughput_3_lin() {
    throughput(true, 3, 1000).await;
}

async fn throughput(linearizable: bool, num_replicas: usize, num_clients: usize) {
    let local = tokio::task::LocalSet::new();

    local.spawn_local(async move {
        tokio::time::sleep(Duration::from_secs(60)).await;
        panic!("timeout");
    });

    // Run the local task set.
    local
        .run_until(async move {
            let (_replicas, clients) = build_kv(linearizable, num_replicas, num_clients);

            let attempted = Arc::new(AtomicU64::new(0));
            let committed = Arc::new(AtomicU64::new(0));

            for client in clients {
                let attempted = Arc::clone(&attempted);
                let committed = Arc::clone(&committed);
                tokio::task::spawn_local(async move {
                    let attempted = Arc::clone(&attempted);
                    let committed = Arc::clone(&committed);
                    loop {
                        let i = thread_rng().gen_range(0..num_clients as i64 * 10); // thread_rng().gen::<i64>();
                        let txn = client.begin();
                        let old = txn.get(i).await.unwrap_or_default();
                        txn.put(i, Some(old + 1));
                        let c = txn.commit().await.is_some() as u64;
                        attempted.fetch_add(1, Ordering::Relaxed);
                        committed.fetch_add(c, Ordering::Relaxed);

                        tokio::time::sleep(Duration::from_millis(
                            thread_rng().gen_range(1..=num_clients as u64),
                        ))
                        .await;
                    }
                });
            }

            /*
            let guard = pprof::ProfilerGuardBuilder::default()
                .frequency(1000)
                .blocklist(&["libc", "libgcc", "pthread", "vdso"])
                .build();
            */

            for _ in 0..10 {
                tokio::time::sleep(Duration::from_millis(1000)).await;

                let a = attempted.swap(0, Ordering::Relaxed);
                let c = committed.swap(0, Ordering::Relaxed);

                println!("TPUT {a}, {c}");
            }

            /*
            if let Ok(guard) = guard {
                if let Ok(report) = guard.report().build() {
                    let file = std::fs::File::create("flamegraph.svg").unwrap();
                    let mut options = pprof::flamegraph::Options::default();
                    options.image_width = Some(2500);
                    report.flamegraph_with_options(file, &mut options).unwrap();
                }
            }
            */
        })
        .await;
}

#[ignore]
#[tokio::test]
async fn coordinator_recovery_3_loop() {
    loop {
        timeout_coordinator_recovery(3).await;
    }
}

#[tokio::test]
async fn coordinator_recovery_3() {
    timeout_coordinator_recovery(3).await;
}

#[tokio::test]
async fn coordinator_recovery_5() {
    timeout_coordinator_recovery(5).await;
}

#[ignore]
#[tokio::test]
async fn coordinator_recovery_7_loop() {
    loop {
        timeout_coordinator_recovery(7).await;
    }
}

#[tokio::test]
async fn coordinator_recovery_7() {
    timeout_coordinator_recovery(7).await;
}

async fn timeout_coordinator_recovery(num_replicas: usize) {
    timeout(
        Duration::from_secs((num_replicas as u64 + 10) * 20),
        coordinator_recovery(num_replicas),
    )
    .await
    .unwrap();
}

async fn coordinator_recovery(num_replicas: usize) {
    let (_replicas, clients) = build_kv(true, num_replicas, 3);

    'outer: for n in (0..50).step_by(2).chain((50..500).step_by(10)) {
        let conflicting = clients[2].begin();
        conflicting.get(n).await;
        tokio::spawn(conflicting.only_prepare());

        //let conflicting = clients[2].begin();
        //conflicting.put(n, Some(1));
        //tokio::spawn(conflicting.only_prepare());

        let txn = clients[0].begin();
        txn.put(n, Some(42));
        let result = Arc::new(Mutex::new(Option::<Option<TapirTimestamp>>::None));

        {
            let result = Arc::clone(&result);
            tokio::spawn(async move {
                let ts = txn.commit2(Some(Duration::from_millis(n as u64))).await;
                *result.lock().unwrap() = Some(ts);
            });
        }

        Transport::sleep(Duration::from_millis(thread_rng().gen_range(0..100))).await;

        for i in 0..128 {
            let txn = clients[1].begin();
            let read = txn.get(n).await;
            println!("{n} try {i} read {read:?}");

            if let Ok(Some(ts)) = timeout(Duration::from_secs(5), txn.commit()).await {
                let result = result.lock().unwrap();
                if let Some(result) = *result {
                    if let Some(result) = result {
                        assert_eq!(read.is_some(), ts > result);
                    } else {
                        assert!(read.is_none());
                    }
                }
                continue 'outer;
            }

            tokio::time::sleep(Duration::from_millis(200)).await;
        }

        panic!("never recovered");
    }
}

// --- Faulty transport helpers ---

fn build_shard_faulty(
    shard: ShardNumber,
    linearizable: bool,
    num_replicas: usize,
    registry: &ChannelRegistry<TapirReplica<K, V>>,
    config: &NetworkFaultConfig,
    seed: u64,
) -> Vec<Arc<IrReplica<TapirReplica<K, V>, FaultyTransport>>> {
    let initial_address = registry.len();
    let membership = IrMembership::new(
        (0..num_replicas)
            .map(|n| n + initial_address)
            .collect::<Vec<_>>(),
    );

    let replicas = (0..num_replicas)
        .map(|i| {
            let node_seed = seed.wrapping_add(initial_address as u64 + i as u64);
            let config = config.clone();
            let membership = membership.clone();

            Arc::new_cyclic(
                |weak: &std::sync::Weak<IrReplica<TapirReplica<K, V>, FaultyTransport>>| {
                    let weak = weak.clone();
                    let channel = registry
                        .channel(move |from, message| weak.upgrade()?.receive(from, message));
                    let transport = FaultyChannelTransport::new(channel, config, node_seed);
                    transport.set_shard(shard);
                    let upcalls = TapirReplica::new(shard, linearizable);
                    IrReplica::new(membership, upcalls, transport, Some(TapirReplica::tick))
                },
            )
        })
        .collect::<Vec<_>>();

    registry.put_shard_addresses(shard, membership);

    replicas
}

fn build_clients_faulty(
    num_clients: usize,
    registry: &ChannelRegistry<TapirReplica<K, V>>,
    config: &NetworkFaultConfig,
    seed: u64,
) -> (Vec<Arc<TapirClient<K, V, FaultyTransport>>>, Vec<FaultyTransport>) {
    let mut clients = Vec::new();
    let mut transports = Vec::new();
    for i in 0..num_clients {
        let client_seed = seed.wrapping_add(10000 + i as u64);
        let channel = registry.channel(move |_, _| unreachable!());
        let transport = FaultyChannelTransport::new(channel, config.clone(), client_seed);
        transports.push(transport.clone());
        clients.push(Arc::new(TapirClient::new(transport)));
    }
    (clients, transports)
}

fn build_sharded_kv_faulty(
    linearizable: bool,
    num_shards: usize,
    num_replicas: usize,
    num_clients: usize,
    config: &NetworkFaultConfig,
    seed: u64,
    address_directory: Arc<InMemoryShardDirectory<usize>>,
) -> (
    Vec<Vec<Arc<IrReplica<TapirReplica<K, V>, FaultyTransport>>>>,
    Vec<Arc<TapirClient<K, V, FaultyTransport>>>,
    Vec<FaultyTransport>,
) {
    init_tracing();

    eprintln!("---------------------------");
    eprintln!(
        " linearizable={linearizable} num_shards={num_shards} num_replicas={num_replicas} seed={seed}"
    );
    eprintln!("---------------------------");

    let registry = ChannelRegistry::with_directory(address_directory);

    let mut shards = Vec::new();
    for shard in 0..num_shards {
        let shard_seed = seed.wrapping_add(shard as u64 * 100);
        let replicas = build_shard_faulty(
            ShardNumber(shard as u32),
            linearizable,
            num_replicas,
            &registry,
            config,
            shard_seed,
        );
        shards.push(replicas);
    }

    let (clients, client_transports) =
        build_clients_faulty(num_clients, &registry, config, seed.wrapping_add(5000));

    (shards, clients, client_transports)
}

/// Build non-overlapping key ranges covering `[0, num_keys)` across shards.
///
/// Keys are distributed as evenly as possible. With num_shards=3, num_keys=5:
///   Shard 0: [0, 2) -> keys 0, 1
///   Shard 1: [2, 4) -> keys 2, 3
///   Shard 2: [4, 5) -> key 4
fn build_shard_entries(num_shards: u32, num_keys: i64) -> Vec<ShardEntry<i64>> {
    let keys_per_shard = num_keys / num_shards as i64;
    let remainder = num_keys % num_shards as i64;
    let mut entries = Vec::new();
    let mut cursor: i64 = 0;
    for s in 0..num_shards {
        let size = keys_per_shard + if (s as i64) < remainder { 1 } else { 0 };
        entries.push(ShardEntry {
            shard: ShardNumber(s),
            range: KeyRange {
                start: Some(cursor),
                end: Some(cursor + size),
            },
        });
        cursor += size;
    }
    entries
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn fuzz_tapir_transactions() {
    let seed: u64 = std::env::var("TAPI_TEST_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| thread_rng().r#gen());

    eprintln!("fuzz_tapir_transactions seed={seed}");

    let mut rng = StdRng::seed_from_u64(seed);

    let config = NetworkFaultConfig {
        drop_rate: 0.05,
        duplicate_rate: 0.02,
        reorder_buffer_size: 3,
        latency: LatencyConfig::Uniform {
            min: Duration::from_millis(1),
            max: Duration::from_millis(10),
        },
        partition_pairs: Default::default(),
        clock_skew_nanos: 0,
    };

    let num_shards = rng.gen_range(1..=3u32);
    let num_replicas = 3;
    let num_clients = 3;
    let num_keys: i64 = 5;
    let iterations_per_client = 20;

    eprintln!("fuzz_tapir_transactions: num_shards={num_shards} seed={seed}");

    // Create shared address directory and discovery for sync testing.
    let address_directory = Arc::new(InMemoryShardDirectory::new());
    let discovery = Arc::new(InMemoryDiscovery::new());
    let disc_dir = DiscoveryShardDirectory::<usize, _>::new(
        Arc::clone(&address_directory),
        Arc::clone(&discovery),
        Duration::from_millis(500),
    );
    disc_dir.add_own_shard(ShardNumber(0));

    let (shards, clients, client_transports) = build_sharded_kv_faulty(
        true,
        num_shards as usize,
        num_replicas,
        num_clients,
        &config,
        seed,
        Arc::clone(&address_directory),
    );

    // Build router for key-to-shard mapping.
    let shard_dir = ShardDirectory::new(build_shard_entries(num_shards, num_keys));
    let router = Arc::new(DynamicRouter::new(Arc::new(RwLock::new(shard_dir))));

    // Build RoutingClient instances for automatic key-to-shard routing.
    let routing_clients: Vec<_> = clients
        .iter()
        .map(|c| Arc::new(RoutingClient::new(Arc::clone(c), Arc::clone(&router))))
        .collect();

    // Track committed increments per key.
    let committed_counts: Arc<Mutex<HashMap<i64, i64>>> =
        Arc::new(Mutex::new(HashMap::new()));
    let total_committed = Arc::new(AtomicU64::new(0));
    let total_attempted = Arc::new(AtomicU64::new(0));
    let collector: Arc<Mutex<Vec<TxnRecord>>> = Arc::new(Mutex::new(Vec::new()));
    let next_txn_index = Arc::new(AtomicU64::new(0));

    // Generate per-client seeds up front (rng is not Send).
    let client_seeds: Vec<u64> = (0..num_clients).map(|_| rng.r#gen()).collect();

    // Spawn fault injection task.
    //
    // IR/TAPIR is LEADERLESS for normal operations — there is no designated
    // leader. View changes can be initiated by ANY replica (periodic tick
    // timeout) or ANY client (detecting view number inconsistency across
    // replicas). The replica designated by view.leader() only serves as the
    // view change COORDINATOR — it collects f+1 DoViewChange addenda and
    // runs sync/merge. It does NOT initiate view changes.
    //
    // Three fault types exercise different view change triggers:
    //   1. Replica-initiated: any replica bumps its view and broadcasts
    //      DoViewChange, simulating its periodic tick timeout.
    //   2. Client-initiated: any client sends DoViewChange with
    //      from_client=true to all replicas, nudging them to adopt a higher
    //      view. Client messages carry addendum=None and do NOT contribute
    //      to the f+1 quorum needed by the coordinator.
    //   3. Network partition: isolates a replica, forcing the remaining
    //      replicas to complete a view change (sync/merge) without it, then
    //      heals so the lagging replica catches up via StartView.
    let fault_seed = rng.r#gen::<u64>();
    let fault_shards = shards.clone();
    let fault_clients = clients.clone();
    let fault_client_transports = client_transports.clone();
    let fault_handle = tokio::spawn(async move {
        let mut rng = StdRng::seed_from_u64(fault_seed);
        let num_fault_rounds = rng.gen_range(2..=4u32);

        for round in 0..num_fault_rounds {
            // Wait before injecting faults (let some transactions complete).
            FaultyTransport::sleep(Duration::from_millis(rng.gen_range(50..=200))).await;

            let event: u8 = rng.gen_range(0..100);

            if event < 35 {
                // --- Replica-initiated view change ---
                let shard_idx = rng.gen_range(0..fault_shards.len());
                let replica_idx = rng.gen_range(0..fault_shards[shard_idx].len());
                eprintln!(
                    "fault[{round}]: replica-initiated view change on \
                     shard={shard_idx} replica={replica_idx} (seed={fault_seed})"
                );
                fault_shards[shard_idx][replica_idx].force_view_change();
            } else if event < 65 {
                // --- Client-initiated view change ---
                let client_idx = rng.gen_range(0..fault_clients.len());
                let shard_idx = rng.gen_range(0..fault_shards.len());
                eprintln!(
                    "fault[{round}]: client-initiated view change from \
                     client={client_idx} shard={shard_idx} (seed={fault_seed})"
                );
                fault_clients[client_idx]
                    .force_view_change(ShardNumber(shard_idx as u32));
            } else {
                // --- Network partition + heal ---
                // Partition must be applied on ALL transports for a full
                // network partition (each transport has independent fault state).
                let shard_idx = rng.gen_range(0..fault_shards.len());
                let replica_idx = rng.gen_range(0..fault_shards[shard_idx].len());
                let target_addr = fault_shards[shard_idx][replica_idx].address();
                eprintln!(
                    "fault[{round}]: partitioning replica addr={target_addr} \
                     shard={shard_idx} replica={replica_idx} (seed={fault_seed})"
                );

                for shard in &fault_shards {
                    for replica in shard {
                        replica.transport().partition_node(target_addr);
                    }
                }
                for ct in &fault_client_transports {
                    ct.partition_node(target_addr);
                }

                // Hold partition briefly. Replicas now reply with state=None
                // when ViewChanging (no hot loop in send()), and the remaining
                // 2 of 3 can still form a quorum for view change completion.
                let hold_ms = rng.gen_range(200..=1000u64);
                FaultyTransport::sleep(Duration::from_millis(hold_ms)).await;

                // Heal partition.
                eprintln!(
                    "fault[{round}]: healing replica addr={target_addr} \
                     after {hold_ms}ms (seed={fault_seed})"
                );
                for shard in &fault_shards {
                    for replica in shard {
                        replica.transport().heal_node(target_addr);
                    }
                }
                for ct in &fault_client_transports {
                    ct.heal_node(target_addr);
                }
            }

            // Let view change propagate (replicas exchange DoViewChange,
            // coordinator collects quorum, runs sync/merge, broadcasts
            // StartView).
            FaultyTransport::sleep(Duration::from_millis(rng.gen_range(100..=500))).await;
        }
    });

    // Spawn concurrent client workloads.
    let handles: Vec<_> = routing_clients
        .iter()
        .enumerate()
        .map(|(client_idx, routing_client)| {
            let routing_client = Arc::clone(routing_client);
            let committed_counts = Arc::clone(&committed_counts);
            let total_committed = Arc::clone(&total_committed);
            let total_attempted = Arc::clone(&total_attempted);
            let collector = Arc::clone(&collector);
            let next_txn_index = Arc::clone(&next_txn_index);
            let client_seed = client_seeds[client_idx];

            tokio::spawn(async move {
                let mut rng = StdRng::seed_from_u64(client_seed);

                for _ in 0..iterations_per_client {
                    let txn_index = next_txn_index.fetch_add(1, Ordering::Relaxed) as usize;
                    total_attempted.fetch_add(1, Ordering::Relaxed);

                    let wall_start = tokio::time::Instant::now();
                    let txn_type: u8 = rng.gen_range(0..100);
                    let txn = routing_client.begin();
                    // Track reads/writes for invariant checking.
                    let mut reads: Vec<(i64, Option<i64>)> = Vec::new();
                    let mut writes: Vec<(i64, i64)> = Vec::new();
                    // Track which keys we wrote +1 to.
                    let mut write_targets: Vec<i64> = Vec::new();

                    if txn_type < 80 {
                        // RMW transaction (80%): 1-3 distinct random keys with optional scan.
                        // Cross-shard happens naturally when keys map to different shards.
                        let n_keys = rng.gen_range(1..=3u8);
                        let mut used_keys = std::collections::HashSet::new();
                        for _ in 0..n_keys {
                            let key: i64 = rng.gen_range(0..num_keys);
                            if !used_keys.insert(key) {
                                continue; // skip duplicate key within same txn
                            }
                            let raw = txn.get(key).await;
                            reads.push((key, raw));
                            let old = raw.unwrap_or(0);
                            txn.put(key, Some(old + 1));
                            writes.push((key, old + 1));
                            write_targets.push(key);
                        }
                        // Optionally include a scan (50% chance) for phantom detection.
                        if rng.gen_range(0..2u8) == 0 {
                            let lo: i64 = rng.gen_range(0..num_keys);
                            let hi: i64 = rng.gen_range(lo..num_keys);
                            let _results = txn.scan(lo, hi).await;
                        }
                    } else {
                        // Read-only transaction (20%).
                        let n_reads = rng.gen_range(1..=2u8);
                        for _ in 0..n_reads {
                            let key: i64 = rng.gen_range(0..num_keys);
                            let val = txn.get(key).await;
                            reads.push((key, val));
                        }
                    }

                    match timeout(Duration::from_secs(10), txn.commit()).await {
                        Ok(Some(ts)) => {
                            let wall_end = tokio::time::Instant::now();
                            let mut counts = committed_counts.lock().unwrap();
                            for &k in &write_targets {
                                *counts.entry(k).or_default() += 1;
                            }
                            total_committed.fetch_add(1, Ordering::Relaxed);
                            collector.lock().unwrap().push(TxnRecord {
                                index: txn_index,
                                client_id: client_idx,
                                read_set: reads,
                                write_set: writes,
                                outcome: TxnOutcome::Committed(ts),
                                wall_start,
                                wall_end,
                            });
                        }
                        Ok(None) => {
                            let wall_end = tokio::time::Instant::now();
                            collector.lock().unwrap().push(TxnRecord {
                                index: txn_index,
                                client_id: client_idx,
                                read_set: reads,
                                write_set: writes,
                                outcome: TxnOutcome::Aborted,
                                wall_start,
                                wall_end,
                            });
                        }
                        Err(_) => {
                            let wall_end = tokio::time::Instant::now();
                            collector.lock().unwrap().push(TxnRecord {
                                index: txn_index,
                                client_id: client_idx,
                                read_set: reads,
                                write_set: writes,
                                outcome: TxnOutcome::TimedOut,
                                wall_start,
                                wall_end,
                            });
                        }
                    }

                    // Small inter-transaction delay.
                    FaultyTransport::sleep(Duration::from_millis(rng.gen_range(1..=20))).await;
                }
            })
        })
        .collect();

    // Wait for all workloads with overall timeout.
    let all_done = timeout(Duration::from_secs(60), async {
        let _ = fault_handle.await;
        for handle in handles {
            handle.await.unwrap();
        }
    })
    .await;

    if all_done.is_err() {
        eprintln!("fuzz_tapir_transactions: workload timed out (seed={seed})");
    }

    let attempted = total_attempted.load(Ordering::Relaxed);
    let committed = total_committed.load(Ordering::Relaxed);
    eprintln!(
        "fuzz_tapir_transactions: attempted={attempted} committed={committed} \
         shards={num_shards} seed={seed}"
    );
    assert!(committed > 0, "no transactions committed (seed={seed})");

    // Let replicas drain pending operations after all view changes settle.
    FaultyTransport::sleep(Duration::from_secs(5)).await;

    // Verify address directory is populated with all shards.
    for s in 0..num_shards {
        let shard = ShardNumber(s);
        assert!(
            address_directory.get(shard).is_some(),
            "address directory should contain shard {s} (seed={seed})"
        );
    }

    // Verify discovery received shard 0 via background sync push.
    tokio::task::yield_now().await;
    let disc_shard_0 = discovery.get_shard(0);
    assert!(
        disc_shard_0.is_some(),
        "discovery should contain shard 0 after sync (seed={seed})"
    );

    // Keep DiscoveryShardDirectory alive until after assertions.
    drop(disc_dir);

    // Run invariant checker: serializability, strict serializability,
    // cross-shard atomicity (via dependency graph + real-time ordering).
    let records = collector.lock().unwrap().clone();
    let checker = InvariantChecker::new(records, seed);
    checker.check_all();

    // Counter invariant: for each key, final value == committed increments.
    // Cross-validate checker's expected_counts against inline committed_counts.
    let checker_counts = checker.expected_counts();
    let inline_counts = committed_counts.lock().unwrap().clone();
    assert_eq!(
        checker_counts, inline_counts,
        "checker vs inline committed_counts mismatch (seed={seed})"
    );

    let verify_client = RoutingClient::new(Arc::clone(&clients[0]), Arc::clone(&router));
    for (key, expected) in &inline_counts {
        let txn = verify_client.begin();
        let actual = txn.get(*key).await.unwrap_or(0);

        match timeout(Duration::from_secs(10), txn.commit()).await {
            Ok(Some(_)) => {
                assert_eq!(
                    actual, *expected,
                    "counter invariant violated for key={key}: \
                     actual={actual} expected={expected} (seed={seed})"
                );
            }
            _ => {
                eprintln!(
                    "warning: verification read for key={key} \
                     did not commit (seed={seed})"
                );
            }
        }
    }

    eprintln!(
        "fuzz_tapir_transactions: seed={seed} shards={num_shards} \
         {committed}/{attempted} committed, invariants passed"
    );
}

#[tokio::test(start_paused = true)]
async fn test_add_replica_with_preload() {
    use crate::tapir::shard_manager::ShardManager;

    init_tracing();

    let shard = ShardNumber(0);
    let registry = ChannelRegistry::default();

    // Build 3-replica shard and 1 client.
    let replicas = build_shard(shard, true, 3, &registry);
    let clients = build_clients(1, &registry);
    let shard_dir = ShardDirectory::new(vec![ShardEntry {
        shard,
        range: KeyRange {
            start: None,
            end: None,
        },
    }]);
    let router = Arc::new(DynamicRouter::new(Arc::new(RwLock::new(shard_dir))));
    let routing_client = Arc::new(RoutingClient::new(Arc::clone(&clients[0]), Arc::clone(&router)));

    // Commit a transaction: put key=1 value=42.
    let txn = routing_client.begin();
    txn.put(1_i64, Some(42_i64));
    let commit_ts = txn.commit().await;
    assert!(commit_ts.is_some(), "initial commit should succeed");

    // Trigger a view change to populate leader_record on all replicas.
    replicas[0].force_view_change();
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Create 4th replica.
    let new_address = registry.len();
    let new_replica = Arc::new_cyclic(
        |weak: &std::sync::Weak<
            IrReplica<TapirReplica<K, V>, ChannelTransport<TapirReplica<K, V>>>,
        >| {
            let weak = weak.clone();
            let channel =
                registry.channel(move |from, message| weak.upgrade()?.receive(from, message));
            channel.set_shard(shard);
            let upcalls = TapirReplica::new(shard, true);
            // Start with membership=[self] only — the real membership comes via AddMember.
            IrReplica::new(
                IrMembership::new(vec![new_address]),
                upcalls,
                channel,
                Some(TapirReplica::tick),
            )
        },
    );

    // Set up ShardManager with the original 3-replica membership.
    let manager_channel = registry.channel(move |_, _| None);
    let original_membership =
        IrMembership::new((0..3).collect::<Vec<_>>());
    let address_directory = Arc::clone(registry.directory());
    let mut manager = ShardManager::new(manager_channel, address_directory);
    manager.register_shard(shard, original_membership, KeyRange {
        start: None,
        end: None,
    });

    // add_replica: fetch leader_record → bootstrap R4 → AddMember.
    let new_membership = IrMembership::new(vec![new_address]);
    manager.add_replica(shard, new_address, new_membership).await;

    // Wait for the view change (AddMember → N+3) to complete.
    tokio::time::sleep(Duration::from_secs(10)).await;

    // Verify: read key=1 through the 4-replica group.
    // The 4th replica should have the committed data from the bootstrap.
    let txn = routing_client.begin();
    let val = txn.get(1_i64).await;
    assert_eq!(val, Some(42), "key=1 should be readable after add_replica");
    assert!(txn.commit().await.is_some(), "read-only txn should commit");

    // Verify: new write succeeds through the 4-replica group.
    let txn = routing_client.begin();
    txn.put(2_i64, Some(99_i64));
    assert!(
        txn.commit().await.is_some(),
        "new write should succeed after add_replica"
    );

    // In IR, invoke_inconsistent only delivers the propose to f+1 replicas
    // (JoinUntil early-returns after the quorum). The remaining replicas get
    // the data during the next view change merge. Trigger a view change to
    // ensure all 4 replicas have the committed data before reading.
    replicas[0].force_view_change();
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Verify: read back the new write (any replica should have it after merge).
    let txn = routing_client.begin();
    let val = txn.get(2_i64).await;
    assert_eq!(val, Some(99), "key=2 should be readable after view change merge");
    assert!(txn.commit().await.is_some());

    // Keep replicas alive for the duration of the test.
    drop(new_replica);
    drop(replicas);
}

// ── Read-only transaction tests ──

#[tokio::test(start_paused = true)]
async fn read_only_basic() {
    let (_replicas, clients) = build_kv(true, 3, 2);

    // Write a value via read-write transaction.
    let txn = clients[0].begin();
    txn.put(
        Sharded {
            shard: ShardNumber(0),
            key: 42,
        },
        Some(100),
    );
    assert!(txn.commit().await.is_some());

    // Advance time so the read-only snapshot is strictly after the commit.
    //
    // The Timestamp uses (time, client_id) lexicographic ordering.
    // With start_paused = true, transport.time() returns the same value,
    // so timestamp ordering depends on client_id.
    //
    // If the writing client's ID > reading client's ID,
    // the commit timestamp > snapshot timestamp, and
    // the read-only transaction won't see the write.
    tokio::time::advance(Duration::from_millis(1)).await;

    // Read it back via read-only transaction.
    let ro = clients[1].begin_read_only();
    let val = ro
        .get(Sharded {
            shard: ShardNumber(0),
            key: 42,
        })
        .await;
    assert_eq!(val, Some(100));

    // Non-existent key returns None.
    let val = ro
        .get(Sharded {
            shard: ShardNumber(0),
            key: 999,
        })
        .await;
    assert_eq!(val, None);

    drop(_replicas);
}

#[tokio::test(start_paused = true)]
async fn read_only_consistent_snapshot() {
    let (_replicas, clients) = build_kv(true, 3, 2);

    // Write two keys via read-write transaction.
    let txn = clients[0].begin();
    txn.put(
        Sharded {
            shard: ShardNumber(0),
            key: 1,
        },
        Some(10),
    );
    txn.put(
        Sharded {
            shard: ShardNumber(0),
            key: 2,
        },
        Some(20),
    );
    assert!(txn.commit().await.is_some());

    // Advance time so the read-only snapshot is strictly after the commit.
    //
    // The Timestamp uses (time, client_id) lexicographic ordering.
    // With start_paused = true, transport.time() returns the same value,
    // so timestamp ordering depends on client_id.
    //
    // If the writing client's ID > reading client's ID,
    // the commit timestamp > snapshot timestamp, and
    // the read-only transaction won't see the write.
    tokio::time::advance(Duration::from_millis(1)).await;

    // Read-only transaction sees a consistent snapshot of both keys.
    let ro = clients[1].begin_read_only();
    let v1 = ro
        .get(Sharded {
            shard: ShardNumber(0),
            key: 1,
        })
        .await;
    let v2 = ro
        .get(Sharded {
            shard: ShardNumber(0),
            key: 2,
        })
        .await;
    assert_eq!(v1, Some(10));
    assert_eq!(v2, Some(20));

    // Reading the same key again within the transaction returns cached value.
    let v1_again = ro
        .get(Sharded {
            shard: ShardNumber(0),
            key: 1,
        })
        .await;
    assert_eq!(v1_again, Some(10));

    drop(_replicas);
}

#[tokio::test(start_paused = true)]
async fn read_only_multi_key_sharded() {
    let (_shards, clients) = build_sharded_kv(true, 3, 3, 2);

    // Write keys across different shards.
    let txn = clients[0].begin();
    txn.put(
        Sharded {
            shard: ShardNumber(0),
            key: 1,
        },
        Some(100),
    );
    txn.put(
        Sharded {
            shard: ShardNumber(1),
            key: 2,
        },
        Some(200),
    );
    txn.put(
        Sharded {
            shard: ShardNumber(2),
            key: 3,
        },
        Some(300),
    );
    assert!(txn.commit().await.is_some());

    // Advance time so the read-only snapshot is strictly after the commit.
    //
    // The Timestamp uses (time, client_id) lexicographic ordering.
    // With start_paused = true, transport.time() returns the same value,
    // so timestamp ordering depends on client_id.
    //
    // If the writing client's ID > reading client's ID,
    // the commit timestamp > snapshot timestamp, and
    // the read-only transaction won't see the write.
    tokio::time::advance(Duration::from_millis(1)).await;

    // Read-only transaction reads across all shards.
    let ro = clients[1].begin_read_only();
    let v1 = ro
        .get(Sharded {
            shard: ShardNumber(0),
            key: 1,
        })
        .await;
    let v2 = ro
        .get(Sharded {
            shard: ShardNumber(1),
            key: 2,
        })
        .await;
    let v3 = ro
        .get(Sharded {
            shard: ShardNumber(2),
            key: 3,
        })
        .await;
    assert_eq!(v1, Some(100));
    assert_eq!(v2, Some(200));
    assert_eq!(v3, Some(300));

    drop(_shards);
}

// ── Read-only scan tests ──

#[tokio::test(start_paused = true)]
async fn read_only_scan_basic() {
    let (_replicas, clients) = build_kv(true, 3, 2);

    // Write several keys.
    let txn = clients[0].begin();
    txn.put(
        Sharded { shard: ShardNumber(0), key: 10 },
        Some(100),
    );
    txn.put(
        Sharded { shard: ShardNumber(0), key: 20 },
        Some(200),
    );
    txn.put(
        Sharded { shard: ShardNumber(0), key: 30 },
        Some(300),
    );
    assert!(txn.commit().await.is_some());

    // Advance time so the read-only snapshot is strictly after the commit.
    tokio::time::advance(Duration::from_millis(1)).await;

    // Scan range [10, 30] via read-only transaction.
    let ro = clients[1].begin_read_only();
    let results = ro
        .scan(
            Sharded { shard: ShardNumber(0), key: 10 },
            Sharded { shard: ShardNumber(0), key: 30 },
        )
        .await;
    assert_eq!(results, vec![(10, 100), (20, 200), (30, 300)]);

    drop(_replicas);
}

#[tokio::test(start_paused = true)]
async fn read_only_scan_consistent_with_get() {
    let (_replicas, clients) = build_kv(true, 3, 2);

    // Write a key.
    let txn = clients[0].begin();
    txn.put(
        Sharded { shard: ShardNumber(0), key: 42 },
        Some(999),
    );
    assert!(txn.commit().await.is_some());

    tokio::time::advance(Duration::from_millis(1)).await;

    // Scan first, then get the same key — should return same value from cache.
    let ro = clients[1].begin_read_only();
    let scan_results = ro
        .scan(
            Sharded { shard: ShardNumber(0), key: 40 },
            Sharded { shard: ShardNumber(0), key: 50 },
        )
        .await;
    assert_eq!(scan_results, vec![(42, 999)]);

    let val = ro
        .get(Sharded { shard: ShardNumber(0), key: 42 })
        .await;
    assert_eq!(val, Some(999));

    drop(_replicas);
}

#[tokio::test(start_paused = true)]
async fn read_only_scan_empty_range() {
    let (_replicas, clients) = build_kv(true, 3, 2);

    // Write keys outside the scan range.
    let txn = clients[0].begin();
    txn.put(
        Sharded { shard: ShardNumber(0), key: 100 },
        Some(1),
    );
    assert!(txn.commit().await.is_some());

    tokio::time::advance(Duration::from_millis(1)).await;

    // Scan range [0, 50] — no keys in range.
    let ro = clients[1].begin_read_only();
    let results = ro
        .scan(
            Sharded { shard: ShardNumber(0), key: 0 },
            Sharded { shard: ShardNumber(0), key: 50 },
        )
        .await;
    assert!(results.is_empty());

    drop(_replicas);
}

#[tokio::test(start_paused = true)]
async fn read_only_scan_multi_shard() {
    use crate::tapir::dynamic_router::{DynamicRouter, ShardDirectory, ShardEntry};
    use crate::tapir::key_range::KeyRange;
    use std::sync::RwLock;

    let (_shards, clients) = build_sharded_kv(true, 2, 3, 2);

    // Write keys: shard 0 gets keys < 100, shard 1 gets keys >= 100.
    let txn = clients[0].begin();
    txn.put(Sharded { shard: ShardNumber(0), key: 10 }, Some(100));
    txn.put(Sharded { shard: ShardNumber(0), key: 20 }, Some(200));
    txn.put(Sharded { shard: ShardNumber(1), key: 110 }, Some(1100));
    txn.put(Sharded { shard: ShardNumber(1), key: 120 }, Some(1200));
    assert!(txn.commit().await.is_some());

    tokio::time::advance(Duration::from_millis(1)).await;

    // Build RoutingClient with shard directory.
    let shard_dir = ShardDirectory::new(vec![
        ShardEntry {
            shard: ShardNumber(0),
            range: KeyRange { start: None, end: Some(100) },
        },
        ShardEntry {
            shard: ShardNumber(1),
            range: KeyRange { start: Some(100), end: None },
        },
    ]);
    let router = Arc::new(DynamicRouter::new(Arc::new(RwLock::new(shard_dir))));
    let routing_client = RoutingClient::new(Arc::clone(&clients[1]), Arc::clone(&router));

    let ro = routing_client.begin_read_only();
    let results = ro.scan(10, 120).await;
    // Should contain keys from both shards.
    assert_eq!(
        results,
        vec![(10, 100), (20, 200), (110, 1100), (120, 1200)]
    );

    drop(_shards);
}

#[tokio::test(start_paused = true)]
async fn read_only_scan_blocks_phantom_write() {
    let (_replicas, clients) = build_kv(true, 3, 2);

    // Write key=10 so the range isn't completely empty.
    let txn = clients[0].begin();
    txn.put(Sharded { shard: ShardNumber(0), key: 10 }, Some(100));
    assert!(txn.commit().await.is_some());

    tokio::time::advance(Duration::from_millis(1)).await;

    // Read-only scan range [0, 50] — triggers QuorumScan which records range_read.
    let ro = clients[1].begin_read_only();
    let results = ro
        .scan(
            Sharded { shard: ShardNumber(0), key: 0 },
            Sharded { shard: ShardNumber(0), key: 50 },
        )
        .await;
    assert_eq!(results, vec![(10, 100)]);

    // Now try to write a NEW key=25 (phantom) via read-write transaction.
    // The QuorumScan should have recorded range_read at the scan's snapshot_ts.
    // This write at an earlier timestamp should be retried at a higher timestamp.
    // But the write at a LATER timestamp should succeed.
    tokio::time::advance(Duration::from_millis(1)).await;

    let txn = clients[0].begin();
    txn.put(Sharded { shard: ShardNumber(0), key: 25 }, Some(250));
    // This should succeed because the write's timestamp is after the scan's snapshot_ts.
    assert!(txn.commit().await.is_some());

    drop(_replicas);
}
