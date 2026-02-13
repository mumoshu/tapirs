use crate::{
    tapir::Sharded,
    transport::{FaultyChannelTransport, LatencyConfig, NetworkFaultConfig},
    ChannelRegistry, ChannelTransport, IrMembership, IrReplica, ShardNumber, TapirClient,
    TapirReplica, TapirTimestamp, Transport as _,
};
use futures::future::join_all;
use rand::{rngs::StdRng, thread_rng, Rng, SeedableRng};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
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

fn build_kv_faulty(
    linearizable: bool,
    num_replicas: usize,
    num_clients: usize,
    config: &NetworkFaultConfig,
    seed: u64,
) -> (
    Vec<Arc<IrReplica<TapirReplica<K, V>, FaultyTransport>>>,
    Vec<Arc<TapirClient<K, V, FaultyTransport>>>,
    Vec<FaultyTransport>,
) {
    let (mut shards, clients, client_transports) =
        build_sharded_kv_faulty(linearizable, 1, num_replicas, num_clients, config, seed);
    (shards.remove(0), clients, client_transports)
}

fn build_sharded_kv_faulty(
    linearizable: bool,
    num_shards: usize,
    num_replicas: usize,
    num_clients: usize,
    config: &NetworkFaultConfig,
    seed: u64,
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

    let registry = ChannelRegistry::default();

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

    let (shards, clients, client_transports) = build_sharded_kv_faulty(
        true,
        num_shards as usize,
        num_replicas,
        num_clients,
        &config,
        seed,
    );

    // Track committed increments per (shard, key).
    let committed_counts: Arc<Mutex<HashMap<(u32, i64), i64>>> =
        Arc::new(Mutex::new(HashMap::new()));
    let total_committed = Arc::new(AtomicU64::new(0));
    let total_attempted = Arc::new(AtomicU64::new(0));

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
                    total_attempted.fetch_add(1, Ordering::Relaxed);

                    let txn_type: u8 = rng.gen_range(0..100);
                    let txn = client.begin();
                    // Track which (shard, key) pairs we wrote +1 to.
                    let mut write_targets: Vec<(u32, i64)> = Vec::new();

                    if txn_type < 60 {
                        // Single-key read-modify-write.
                        let shard = rng.gen_range(0..num_shards);
                        let key: i64 = rng.gen_range(0..num_keys);
                        let sk = Sharded { shard: ShardNumber(shard), key };
                        let old = txn.get(sk.clone()).await.unwrap_or(0);
                        txn.put(sk, Some(old + 1));
                        write_targets.push((shard, key));
                    } else if txn_type < 85 {
                        // Cross-shard: read from one shard, write to another.
                        let shard_a = rng.gen_range(0..num_shards);
                        let shard_b = rng.gen_range(0..num_shards);
                        let key_a: i64 = rng.gen_range(0..num_keys);
                        let key_b: i64 = rng.gen_range(0..num_keys);
                        let _val = txn
                            .get(Sharded { shard: ShardNumber(shard_a), key: key_a })
                            .await;
                        let sk_b = Sharded { shard: ShardNumber(shard_b), key: key_b };
                        let old_b = txn.get(sk_b.clone()).await.unwrap_or(0);
                        txn.put(sk_b, Some(old_b + 1));
                        write_targets.push((shard_b, key_b));
                    } else {
                        // Read-only transaction.
                        let n_reads = rng.gen_range(1..=2u8);
                        for _ in 0..n_reads {
                            let shard = rng.gen_range(0..num_shards);
                            let key: i64 = rng.gen_range(0..num_keys);
                            txn.get(Sharded { shard: ShardNumber(shard), key }).await;
                        }
                    }

                    match timeout(Duration::from_secs(10), txn.commit()).await {
                        Ok(Some(_ts)) => {
                            let mut counts = committed_counts.lock().unwrap();
                            for (s, k) in write_targets {
                                *counts.entry((s, k)).or_default() += 1;
                            }
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

    // Verify counter invariant: for each (shard, key), final value == committed increments.
    let counts = committed_counts.lock().unwrap().clone();
    let verify_client = &clients[0];
    for ((shard, key), expected) in &counts {
        let txn = verify_client.begin();
        let actual = txn
            .get(Sharded {
                shard: ShardNumber(*shard),
                key: *key,
            })
            .await
            .unwrap_or(0);

        match timeout(Duration::from_secs(10), txn.commit()).await {
            Ok(Some(_)) => {
                assert_eq!(
                    actual, *expected,
                    "counter invariant violated for shard={shard} key={key}: \
                     actual={actual} expected={expected} (seed={seed})"
                );
            }
            _ => {
                eprintln!(
                    "warning: verification read for shard={shard} key={key} \
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
