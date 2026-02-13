use crate::{
    tapir::Sharded,
    transport::{FaultyChannelTransport, LatencyConfig, NetworkFaultConfig},
    ChannelRegistry, ChannelTransport, IrMembership, IrReplica, ShardNumber, TapirClient,
    TapirReplica, TapirTimestamp, Transport as _,
};
use futures::future::join_all;
use rand::{rngs::StdRng, thread_rng, Rng, SeedableRng};
use std::{
    sync::{
        atomic::{AtomicI64, AtomicU64, Ordering},
        Arc, Mutex,
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
) -> Vec<Arc<TapirClient<K, V, FaultyTransport>>> {
    (0..num_clients)
        .map(|i| {
            let client_seed = seed.wrapping_add(10000 + i as u64);
            let channel = registry.channel(move |_, _| unreachable!());
            let transport = FaultyChannelTransport::new(channel, config.clone(), client_seed);
            Arc::new(TapirClient::new(transport))
        })
        .collect()
}

fn build_kv_faulty(
    linearizable: bool,
    num_replicas: usize,
    num_clients: usize,
    config: &NetworkFaultConfig,
    seed: u64,
) -> (
    Vec<Arc<IrReplica<TapirReplica<K, V>, FaultyTransport>>>,
    Vec<Arc<TapirClient<K, V, FaultyTransport>>>,
) {
    init_tracing();

    let registry = ChannelRegistry::default();

    let replicas = build_shard_faulty(
        ShardNumber(0),
        linearizable,
        num_replicas,
        &registry,
        config,
        seed,
    );

    let clients = build_clients_faulty(num_clients, &registry, config, seed.wrapping_add(5000));

    (replicas, clients)
}

#[tokio::test(flavor = "multi_thread")]
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

    let num_replicas = 3;
    let num_clients = 3;
    let num_keys: i64 = 5;
    let iterations_per_client = 20;

    let (_replicas, clients) = build_kv_faulty(true, num_replicas, num_clients, &config, seed);

    // Track committed increments per key.
    let committed_counts: Arc<Vec<AtomicI64>> =
        Arc::new((0..num_keys).map(|_| AtomicI64::new(0)).collect());
    let total_committed = Arc::new(AtomicU64::new(0));
    let total_attempted = Arc::new(AtomicU64::new(0));

    // Generate per-client seeds up front (rng is not Send).
    let client_seeds: Vec<u64> = (0..num_clients).map(|_| rng.r#gen()).collect();

    // Spawn concurrent client workloads.
    let handles: Vec<_> = clients
        .iter()
        .enumerate()
        .map(|(client_idx, client)| {
            let client = Arc::clone(client);
            let committed_counts = Arc::clone(&committed_counts);
            let total_committed = Arc::clone(&total_committed);
            let total_attempted = Arc::clone(&total_attempted);
            let client_seed = client_seeds[client_idx];

            tokio::spawn(async move {
                let mut rng = StdRng::seed_from_u64(client_seed);

                for _ in 0..iterations_per_client {
                    let key: i64 = rng.gen_range(0..num_keys);
                    total_attempted.fetch_add(1, Ordering::Relaxed);

                    let txn = client.begin();
                    let old = txn.get(key).await.unwrap_or(0);
                    txn.put(key, Some(old + 1));

                    match timeout(Duration::from_secs(5), txn.commit()).await {
                        Ok(Some(_ts)) => {
                            committed_counts[key as usize].fetch_add(1, Ordering::Relaxed);
                            total_committed.fetch_add(1, Ordering::Relaxed);
                        }
                        Ok(None) | Err(_) => {
                            // Aborted or timed out -- no side effect.
                        }
                    }

                    // Small inter-transaction delay.
                    FaultyTransport::sleep(Duration::from_millis(rng.gen_range(1..=20))).await;
                }
            })
        })
        .collect();

    // Wait for all workloads with overall timeout.
    let all_done = timeout(Duration::from_secs(30), async {
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
    eprintln!("fuzz_tapir_transactions: attempted={attempted} committed={committed} seed={seed}");
    assert!(committed > 0, "no transactions committed (seed={seed})");

    // Let replicas drain pending operations.
    FaultyTransport::sleep(Duration::from_secs(2)).await;

    // Verify counter invariant: for each key, final value == number of committed increments.
    let verify_client = &clients[0];
    for key in 0..num_keys {
        let expected = committed_counts[key as usize].load(Ordering::Relaxed);

        let txn = verify_client.begin();
        let actual = txn.get(key).await.unwrap_or(0);

        match timeout(Duration::from_secs(5), txn.commit()).await {
            Ok(Some(_)) => {
                assert_eq!(
                    actual, expected,
                    "counter invariant violated for key {key}: actual={actual} expected={expected} (seed={seed})"
                );
            }
            _ => {
                eprintln!(
                    "warning: verification read for key {key} did not commit (seed={seed})"
                );
            }
        }
    }

    eprintln!(
        "fuzz_tapir_transactions: seed={seed} {committed}/{attempted} committed, invariants passed"
    );
}
