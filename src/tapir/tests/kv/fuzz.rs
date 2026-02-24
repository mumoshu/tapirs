use super::*;

// --- Faulty transport helpers ---

#[allow(clippy::too_many_arguments)]
fn build_shard_faulty(
    rng: &mut crate::Rng,
    shard: ShardNumber,
    linearizable: bool,
    num_replicas: usize,
    registry: &ChannelRegistry<TapirReplica<K, V>>,
    config: &NetworkFaultConfig,
    seed: u64,
    per_replica_locals: &[Arc<InMemoryShardDirectory<usize>>],
) -> Vec<Arc<IrReplica<TapirReplica<K, V>, FaultyTransport>>> {
    let initial_address = registry.len();
    let membership = IrMembership::new(
        (0..num_replicas)
            .map(|n| n + initial_address)
            .collect::<Vec<_>>(),
    );

    // Seed each replica's local directory with the shard membership.
    for local in per_replica_locals.iter().take(num_replicas) {
        local.put(shard, membership.clone(), 0);
    }

    (0..num_replicas)
        .map(|i| {
            let node_seed = seed.wrapping_add(initial_address as u64 + i as u64);
            let config = config.clone();
            let membership = membership.clone();
            let replica_rng = rng.fork();
            let dir = Arc::clone(&per_replica_locals[i]);

            Arc::new_cyclic(
                |weak: &std::sync::Weak<IrReplica<TapirReplica<K, V>, FaultyTransport>>| {
                    let weak = weak.clone();
                    let channel = registry
                        .channel(move |from, message| weak.upgrade()?.receive(from, message), dir);
                    let transport = FaultyChannelTransport::new(channel, config, node_seed);
                    transport.set_shard(shard);
                    let upcalls = TapirReplica::new_with_backend(shard, linearizable,
                        DiskStore::<K, V, Timestamp, MemoryIo>::open(
                            MemoryIo::temp_path(),
                        ).unwrap(),
                    );
                    IrReplica::new(replica_rng, membership, upcalls, transport, Some(TapirReplica::tick))
                },
            )
        })
        .collect::<Vec<_>>()
}

fn build_clients_faulty(
    rng: &mut crate::Rng,
    num_clients: usize,
    registry: &ChannelRegistry<TapirReplica<K, V>>,
    config: &NetworkFaultConfig,
    seed: u64,
    directories: &[Arc<InMemoryShardDirectory<usize>>],
) -> (Vec<Arc<TapirClient<K, V, FaultyTransport>>>, Vec<FaultyTransport>) {
    let mut clients = Vec::new();
    let mut transports = Vec::new();
    for (i, dir) in directories.iter().enumerate().take(num_clients) {
        let client_seed = seed.wrapping_add(10000 + i as u64);
        let channel = registry.channel(move |_, _| unreachable!(), Arc::clone(dir));
        let transport = FaultyChannelTransport::new(channel, config.clone(), client_seed);
        transports.push(transport.clone());
        clients.push(Arc::new(TapirClient::new(rng.fork(), transport)));
    }
    (clients, transports)
}

/// Returns (shards, clients, client_transports, registry, node_shard_sets).
/// `node_shard_sets[node_idx]` = set of ShardNumbers assigned to that node.
#[allow(clippy::too_many_arguments)]
fn build_sharded_kv_faulty(
    rng: &mut crate::Rng,
    linearizable: bool,
    replica_counts: &[usize],
    num_clients: usize,
    config: &NetworkFaultConfig,
    seed: u64,
    node_locals: &[Arc<InMemoryShardDirectory<usize>>],
    client_locals: &[Arc<InMemoryShardDirectory<usize>>],
) -> (
    Vec<Vec<Arc<IrReplica<TapirReplica<K, V>, FaultyTransport>>>>,
    Vec<Arc<TapirClient<K, V, FaultyTransport>>>,
    Vec<FaultyTransport>,
    ChannelRegistry<TapirReplica<K, V>>,
    Vec<std::collections::BTreeSet<ShardNumber>>,
) {
    let num_shards = replica_counts.len();
    let num_nodes = node_locals.len();

    init_tracing();

    eprintln!("---------------------------");
    eprintln!(
        " linearizable={linearizable} num_shards={num_shards} replica_counts={replica_counts:?} seed={seed}"
    );
    eprintln!("---------------------------");

    let registry = ChannelRegistry::default();

    // Use a local seeded RNG for node assignment shuffling (deterministic).
    let mut assign_rng = StdRng::seed_from_u64(seed.wrapping_add(99999));

    // Track which shards each node hosts (for own_shards registration).
    let mut node_shard_sets: Vec<std::collections::BTreeSet<ShardNumber>> =
        (0..num_nodes).map(|_| std::collections::BTreeSet::new()).collect();

    let mut shards = Vec::new();
    for (shard, &count) in replica_counts.iter().enumerate() {
        let shard_seed = seed.wrapping_add(shard as u64 * 100);

        // Shuffle node indices — take first `count` for no same-shard collision.
        let mut node_indices: Vec<usize> = (0..num_nodes).collect();
        node_indices.shuffle(&mut assign_rng);
        let per_replica_locals: Vec<_> = (0..count)
            .map(|r| {
                node_shard_sets[node_indices[r]].insert(ShardNumber(shard as u32));
                Arc::clone(&node_locals[node_indices[r]])
            })
            .collect();

        let replicas = build_shard_faulty(
            rng,
            ShardNumber(shard as u32),
            linearizable,
            count,
            &registry,
            config,
            shard_seed,
            &per_replica_locals,
        );
        shards.push(replicas);
    }

    let (clients, client_transports) =
        build_clients_faulty(rng, num_clients, &registry, config, seed.wrapping_add(5000), client_locals);

    (shards, clients, client_transports, registry, node_shard_sets)
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

// Deterministic simulation fuzz test: concurrent transactions, fault
// injection (view changes, partitions), and resharding (split/merge).
//
// By default this test uses a fixed seed (0) so that `cargo test` runs are
// deterministic.  Set TAPI_TEST_SEED to override, e.g. with a random value.
//
// To fuzz many iterations with randomly chosen seeds, use the make targets
// and scripts instead of running this test directly:
//
//   make fuzz          – run 20 random seeds sequentially
//   make fuzz100       – run 100 random seeds, 4 in parallel
//   ./scripts/fuzz-multi-seed.sh              (configurable via env vars)
//
// Other useful invocations:
//
// Run once with a specific seed:
//   TAPI_TEST_SEED=<seed> cargo test --lib fuzz_tapir_transactions -- --nocapture
//
// Run with structured event timeline (auto-dumps on failure):
//   FUZZ_VERBOSE=1 TAPI_TEST_SEED=<seed> cargo test --lib fuzz_tapir_transactions -- --nocapture
//
// Detect indeterminism (same seed, many runs):
//   ./scripts/detect-fuzz-indeterminism.sh
#[test]
fn fuzz_tapir_transactions() {
    if cfg!(debug_assertions) {
        eprintln!("\n\
            ╔══════════════════════════════════════════════════════════════════╗\n\
            ║  WARNING: fuzz_tapir_transactions running in DEBUG mode         ║\n\
            ║                                                                 ║\n\
            ║  This test creates 19 replicas with fault injection and         ║\n\
            ║  resharding. Debug builds are 5-10x slower, so the wall-clock   ║\n\
            ║  watchdog (default 30s, TAPI_WATCHDOG_SECS) may fire.          ║\n\
            ║                                                                 ║\n\
            ║  To run successfully:                                           ║\n\
            ║    cargo test --release fuzz_tapir_transactions -- --nocapture  ║\n\
            ║  or:                                                            ║\n\
            ║    make test   (uses --release)                                 ║\n\
            ╚══════════════════════════════════════════════════════════════════╝\n\
        ");
    }

    let seed: u64 = std::env::var("TAPI_TEST_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);

    // Build runtime with deterministic scheduler seed so the same TAPI_TEST_SEED
    // produces the same task interleaving across runs. Without rng_seed, tokio's
    // internal scheduler RNG is seeded from the system, making task wake/poll
    // order non-deterministic even with start_paused=true.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .rng_seed(tokio::runtime::RngSeed::from_bytes(&seed.to_le_bytes()))
        .build()
        .unwrap();

    rt.block_on(fuzz_tapir_transactions_inner(seed));
}

async fn fuzz_tapir_transactions_inner(seed: u64) {
    eprintln!("fuzz_tapir_transactions seed={seed}");
    let _ = std::fs::write("/tmp/tapi-fuzz-seed.txt", seed.to_string());

    let mut rng = StdRng::seed_from_u64(seed);
    let mut lib_rng = test_rng(seed);

    let event_log = FuzzEventLog::new();

    // Wall-clock watchdog: catches hot loops that freeze simulated time.
    // Default 30s, override with TAPI_WATCHDOG_SECS env var.
    // Under start_paused=true, tokio::time::timeout uses simulated time which
    // doesn't advance during hot loops. This thread uses real wall-clock time.
    let watchdog_secs: u64 = std::env::var("TAPI_WATCHDOG_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(30);
    let watchdog_log = event_log.clone();
    let (watchdog_cancel_tx, watchdog_cancel_rx) = std::sync::mpsc::channel::<()>();
    let watchdog_handle = std::thread::spawn(move || {
        match watchdog_cancel_rx.recv_timeout(std::time::Duration::from_secs(watchdog_secs)) {
            Ok(()) | Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => return,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
        }
        eprintln!("WATCHDOG: fuzz_tapir_transactions exceeded {watchdog_secs}s wall-clock (seed={seed})");
        watchdog_log.dump(seed);
        eprintln!("WATCHDOG: failing test — check event log above for last completed step");
        panic!(
            "WATCHDOG: fuzz_tapir_transactions exceeded {watchdog_secs}s wall-clock timeout (seed={seed})"
        );
    });

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
    let replica_counts: Vec<usize> = (0..num_shards)
        .map(|_| [3, 5, 7][rng.gen_range(0..3usize)])
        .collect();
    let num_clients = 3;
    let num_keys: i64 = 10;
    let min_iterations: u32 = 20;
    let max_iterations: u32 = 100;

    event_log.record(FuzzEvent::Config {
        seed,
        num_shards,
        replica_counts: replica_counts.clone(),
        num_clients,
        num_keys,
    });
    eprintln!("fuzz_tapir_transactions: num_shards={num_shards} replica_counts={replica_counts:?} seed={seed}");

    // Pre-build 8 potential new shard replica counts (for splits and compacts).
    // Generated early so we know the max replica count for node allocation.
    let new_shard_replica_counts: Vec<usize> = (0..8)
        .map(|_| [3, 5, 7][rng.gen_range(0..3usize)])
        .collect();

    let sync_interval = Duration::from_millis(200);

    // Build TAPIR discovery cluster (separate 3-replica String,String cluster).
    let disc = crate::testing::discovery::build_test_discovery(&mut lib_rng, 3);
    let cluster_remote = disc.create_remote(&mut lib_rng);

    // Create simulated nodes: num_nodes = max replica count across all shards
    // (guarantees no 2 replicas of same shard on same node).
    let num_nodes = replica_counts.iter()
        .chain(new_shard_replica_counts.iter())
        .copied().max().unwrap();
    let node_locals: Vec<Arc<InMemoryShardDirectory<usize>>> = (0..num_nodes)
        .map(|_| Arc::new(InMemoryShardDirectory::new()))
        .collect();
    let node_cachings: Vec<_> = node_locals.iter()
        .map(|local| CachingShardDirectory::<usize, i64, _>::new(
            Arc::clone(local), Arc::clone(&cluster_remote), sync_interval,
        ))
        .collect();

    // Create client directories (one per client, separate from replica nodes).
    let client_locals: Vec<Arc<InMemoryShardDirectory<usize>>> = (0..num_clients)
        .map(|_| Arc::new(InMemoryShardDirectory::new()))
        .collect();
    let client_cachings: Vec<_> = client_locals.iter()
        .map(|local| CachingShardDirectory::<usize, i64, _>::new(
            Arc::clone(local), Arc::clone(&cluster_remote), sync_interval,
        ))
        .collect();

    let (shards, clients, client_transports, registry, node_shard_sets) = build_sharded_kv_faulty(
        &mut lib_rng,
        true,
        &replica_counts,
        num_clients,
        &config,
        seed,
        &node_locals,
        &client_locals,
    );

    // Register own_shards on each node's CachingShardDirectory for PUSH filtering.
    for (node_idx, shard_set) in node_shard_sets.iter().enumerate() {
        for &shard in shard_set {
            node_cachings[node_idx].add_own_shard(shard);
        }
    }

    // Wait for discovery propagation:
    //   1st sync cycle (≤200ms): replicas PUSH membership → cluster_remote
    //   2nd sync cycle (≤400ms): clients PULL from cluster_remote → client local
    tokio::time::sleep(Duration::from_millis(500)).await;
    tokio::task::yield_now().await;

    // Build router for key-to-shard mapping.
    let shard_dir = ShardDirectory::new(build_shard_entries(num_shards, num_keys));
    let router = Arc::new(DynamicRouter::new(Arc::new(RwLock::new(shard_dir))));

    // Build RoutingClient instances for automatic key-to-shard routing.
    let routing_clients: Vec<_> = clients
        .iter()
        .enumerate()
        .map(|(i, c)| {
            let log = event_log.clone();
            let client_id = i;
            Arc::new(
                RoutingClient::new(Arc::clone(c), Arc::clone(&router))
                    .with_retry_callback(move |msg| {
                        log.record(FuzzEvent::TxnOutOfRangeRetry {
                            client_id,
                            message: msg.to_string(),
                        });
                    }),
            )
        })
        .collect();

    // Track committed increments per key.
    let committed_counts: Arc<Mutex<BTreeMap<i64, i64>>> =
        Arc::new(Mutex::new(BTreeMap::new()));
    let total_committed = Arc::new(AtomicU64::new(0));
    let total_attempted = Arc::new(AtomicU64::new(0));
    let may_have_committed = Arc::new(AtomicU64::new(0));
    let collector: Arc<Mutex<Vec<TxnRecord>>> = Arc::new(Mutex::new(Vec::new()));
    let next_txn_index = Arc::new(AtomicU64::new(0));

    // Generate per-client seeds up front (rng is not Send).
    let client_seeds: Vec<u64> = (0..num_clients).map(|_| rng.r#gen()).collect();

    // Pre-compute resharding resources.
    // Compute initial shard memberships from address offsets.
    let mut address_offset = 0usize;
    let initial_memberships: Vec<IrMembership<usize>> = replica_counts.iter().map(|&count| {
        let m = IrMembership::new((address_offset..address_offset + count).collect());
        address_offset += count;
        m
    }).collect();
    // Account for client addresses.
    address_offset += num_clients;

    let shard_entries = build_shard_entries(num_shards, num_keys);
    let mut new_shard_replicas = Vec::new();
    let mut new_shard_memberships = Vec::new();
    // Use seeded RNG for node assignment of new shard replicas.
    let mut new_assign_rng = StdRng::seed_from_u64(seed.wrapping_add(88888));
    for (i, &count) in new_shard_replica_counts.iter().enumerate() {
        let new_shard_num = ShardNumber(num_shards + i as u32);
        let new_seed = seed.wrapping_add(20000 + i as u64 * 100);

        // Assign new shard replicas to existing nodes.
        let mut node_indices: Vec<usize> = (0..num_nodes).collect();
        node_indices.shuffle(&mut new_assign_rng);
        let per_replica_locals: Vec<_> = (0..count)
            .map(|r| {
                node_cachings[node_indices[r]].add_own_shard(new_shard_num);
                Arc::clone(&node_locals[node_indices[r]])
            })
            .collect();

        let replicas = build_shard_faulty(
            &mut lib_rng,
            new_shard_num,
            true,
            count,
            &registry,
            &config,
            new_seed,
            &per_replica_locals,
        );
        let membership = IrMembership::new(
            (address_offset..address_offset + count).collect(),
        );
        address_offset += count;
        new_shard_memberships.push(membership);
        new_shard_replicas.push(replicas);
    }

    let manager_rng = lib_rng.fork();
    let manager_seed: u64 = rng.r#gen();
    let reshard_seed: u64 = rng.r#gen();
    let final_shards: Arc<Mutex<Option<Vec<ShardEntry<i64>>>>> =
        Arc::new(Mutex::new(None));

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
    //   4. Discovery cluster view change: forces a view change on a random
    //      discovery replica, exercising CachingShardDirectory's resilience
    //      to discovery cluster unavailability during view transitions.
    let fault_seed = rng.r#gen::<u64>();
    let fault_shards = shards.clone();
    let fault_clients = clients.clone();
    let fault_client_transports = client_transports.clone();
    let fault_discovery_replicas = disc.replicas.clone();
    let fault_event_log = event_log.clone();
    let fault_handle = tokio::spawn(async move {
        let mut rng = StdRng::seed_from_u64(fault_seed);
        let num_fault_rounds = rng.gen_range(2..=4u32);

        for round in 0..num_fault_rounds {
            // Wait before injecting faults (let some transactions complete).
            FaultyTransport::sleep(Duration::from_millis(rng.gen_range(50..=200))).await;

            let event: u8 = rng.gen_range(0..100);

            if event < 30 {
                // --- Replica-initiated view change ---
                let shard_idx = rng.gen_range(0..fault_shards.len());
                let replica_idx = rng.gen_range(0..fault_shards[shard_idx].len());
                fault_event_log.record(FuzzEvent::FaultReplicaViewChange {
                    round, shard: shard_idx, replica: replica_idx,
                });
                fault_shards[shard_idx][replica_idx].force_view_change();
            } else if event < 55 {
                // --- Client-initiated view change ---
                let client_idx = rng.gen_range(0..fault_clients.len());
                let shard_idx = rng.gen_range(0..fault_shards.len());
                fault_event_log.record(FuzzEvent::FaultClientViewChange {
                    round, client: client_idx, shard: shard_idx,
                });
                fault_clients[client_idx]
                    .force_view_change(ShardNumber(shard_idx as u32));
            } else if event < 85 {
                // --- Network partition + heal ---
                // Partition must be applied on ALL transports for a full
                // network partition (each transport has independent fault state).
                let shard_idx = rng.gen_range(0..fault_shards.len());
                let replica_idx = rng.gen_range(0..fault_shards[shard_idx].len());
                let target_addr = fault_shards[shard_idx][replica_idx].address();
                fault_event_log.record(FuzzEvent::FaultPartition {
                    round, shard: shard_idx, replica: replica_idx, address: target_addr,
                });

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
                fault_event_log.record(FuzzEvent::FaultHeal {
                    round, address: target_addr, hold_ms,
                });
                for shard in &fault_shards {
                    for replica in shard {
                        replica.transport().heal_node(target_addr);
                    }
                }
                for ct in &fault_client_transports {
                    ct.heal_node(target_addr);
                }
            } else {
                // --- Discovery cluster view change ---
                // Forces a view change on a random discovery replica,
                // temporarily making the discovery cluster unavailable.
                // CachingShardDirectory uses cached entries during this.
                let replica_idx = rng.gen_range(0..fault_discovery_replicas.len());
                fault_discovery_replicas[replica_idx].force_view_change();
            }

            // Let view change propagate (replicas exchange DoViewChange,
            // coordinator collects quorum, runs sync/merge, broadcasts
            // StartView).
            FaultyTransport::sleep(Duration::from_millis(rng.gen_range(100..=500))).await;
        }
    });

    // Stop flag: resharding task sets this when all rounds are done.
    let stop_flag = Arc::new(AtomicBool::new(false));

    // Spawn background route-update task: poll CachingShardDirectory for key_range
    // changes and update the DynamicRouter. When ShardManager publishes route
    // changes via publish_route_changes(), the CachingShardDirectory's next PULL
    // cycle (≤200ms) picks them up, and this task propagates them to the router —
    // allowing clients to route to the new shard during the drain window.
    let route_poll_caching = Arc::clone(&client_cachings[0]);
    let route_poll_router = Arc::clone(&router);
    let route_poll_stop = Arc::clone(&stop_flag);
    let route_poll_handle = tokio::spawn(async move {
        loop {
            tokio::time::sleep(sync_interval).await;
            let ranges = route_poll_caching.key_ranges();
            if !ranges.is_empty() {
                let entries: Vec<ShardEntry<i64>> = ranges
                    .into_iter()
                    .map(|(shard, range)| ShardEntry { shard, range })
                    .collect();
                route_poll_router.directory().write().unwrap().update(entries);
            }
            if route_poll_stop.load(Ordering::Relaxed) {
                break;
            }
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
            let may_have_committed = Arc::clone(&may_have_committed);
            let collector = Arc::clone(&collector);
            let next_txn_index = Arc::clone(&next_txn_index);
            let client_seed = client_seeds[client_idx];
            let txn_event_log = event_log.clone();
            let stop = Arc::clone(&stop_flag);

            tokio::spawn(async move {
                let mut rng = StdRng::seed_from_u64(client_seed);

                let mut iteration = 0u32;
                loop {
                    if iteration >= max_iterations { break; }
                    if iteration >= min_iterations && stop.load(Ordering::Relaxed) { break; }

                    let txn_type: u8 = rng.gen_range(0..100);
                    let is_rmw = txn_type < 80;
                    let txn_type_str = if is_rmw { "rmw" } else { "read_only" };
                    let stale_note = if is_rmw { "(may be stale, checked at commit)" } else { "" };

                    // Pre-generate keys for this logical operation (reused across retries).
                    let keys: Vec<i64> = if is_rmw {
                        let n_keys = rng.gen_range(1..=3u8);
                        let mut used = std::collections::HashSet::new();
                        let mut k = Vec::new();
                        for _ in 0..n_keys {
                            let key: i64 = rng.gen_range(0..num_keys);
                            if used.insert(key) {
                                k.push(key);
                            }
                        }
                        k
                    } else {
                        let n_reads = rng.gen_range(1..=2u8);
                        (0..n_reads).map(|_| rng.gen_range(0..num_keys)).collect()
                    };

                    // Pre-generate scan parameters (for rmw with 50% chance).
                    let scan_params: Option<(i64, i64)> = if is_rmw && rng.gen_range(0..2u8) == 0 {
                        let lo: i64 = rng.gen_range(0..num_keys);
                        let hi: i64 = rng.gen_range(lo..num_keys);
                        Some((lo, hi))
                    } else {
                        None
                    };

                    let max_retries = 2u8;
                    let mut attempt = 0u8;

                    if is_rmw {
                        // RMW transaction (80%): read-modify-write with optional scan.
                        'retry: loop {
                            let txn_index = next_txn_index.fetch_add(1, Ordering::Relaxed) as usize;
                            total_attempted.fetch_add(1, Ordering::Relaxed);
                            let wall_start = tokio::time::Instant::now();
                            let txn = routing_client.begin();
                            let mut reads: Vec<(i64, Option<i64>)> = Vec::new();
                            let mut writes: Vec<(i64, i64)> = Vec::new();
                            let mut write_targets: Vec<i64> = Vec::new();

                            txn_event_log.record(FuzzEvent::TxnBegin {
                                txn_index, client_id: client_idx,
                                txn_type: txn_type_str, keys: keys.clone(),
                            });

                            // 10% chance: drop transaction without committing (misuse simulation).
                            if rng.gen_range(0..10u8) == 0 {
                                drop(txn);
                                txn_event_log.record(FuzzEvent::TxnDropped {
                                    txn_index, client_id: client_idx,
                                });
                                break;
                            }

                            for &key in &keys {
                                let raw = match txn.get(key).await {
                                    Ok(val) => val,
                                    Err(_) => {
                                        txn_event_log.record(FuzzEvent::TxnOutOfRange {
                                            txn_index, client_id: client_idx, key,
                                        });
                                        break 'retry;
                                    }
                                };
                                txn_event_log.record(FuzzEvent::TxnGet {
                                    txn_index, client_id: client_idx, key, value: raw, stale_note,
                                });
                                reads.push((key, raw));
                                let old = raw.unwrap_or(0);
                                txn.put(key, Some(old + 1));
                                txn_event_log.record(FuzzEvent::TxnPut {
                                    txn_index, client_id: client_idx, key, value: old + 1,
                                });
                                writes.push((key, old + 1));
                                write_targets.push(key);
                            }
                            if let Some((lo, hi)) = scan_params {
                                let results = match txn.scan(lo, hi).await {
                                    Ok(val) => val,
                                    Err(_) => {
                                        txn_event_log.record(FuzzEvent::TxnOutOfRange {
                                            txn_index, client_id: client_idx, key: lo,
                                        });
                                        break 'retry;
                                    }
                                };
                                txn_event_log.record(FuzzEvent::TxnScan {
                                    txn_index, client_id: client_idx, lo, hi,
                                    count: results.len(), stale_note,
                                });
                            }

                            match timeout(Duration::from_secs(10), txn.commit()).await {
                                Ok(Some(ts)) => {
                                    let wall_end = tokio::time::Instant::now();
                                    txn_event_log.record(FuzzEvent::TxnCommitted {
                                        txn_index, client_id: client_idx, commit_ts: ts.time,
                                    });
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
                                    break;
                                }
                                Ok(None) => {
                                    let wall_end = tokio::time::Instant::now();
                                    txn_event_log.record(FuzzEvent::TxnAborted {
                                        txn_index, client_id: client_idx,
                                    });
                                    collector.lock().unwrap().push(TxnRecord {
                                        index: txn_index,
                                        client_id: client_idx,
                                        read_set: reads,
                                        write_set: writes,
                                        outcome: TxnOutcome::Aborted,
                                        wall_start,
                                        wall_end,
                                    });
                                    // 70% retry, 30% give up.
                                    if attempt < max_retries && rng.gen_range(0..10u8) < 7 {
                                        attempt += 1;
                                        txn_event_log.record(FuzzEvent::TxnRetry {
                                            txn_index, client_id: client_idx,
                                            attempt, keys: keys.clone(),
                                        });
                                        continue;
                                    }
                                    break;
                                }
                                Err(_) => {
                                    let wall_end = tokio::time::Instant::now();
                                    txn_event_log.record(FuzzEvent::TxnTimedOut {
                                        txn_index, client_id: client_idx,
                                    });
                                    may_have_committed.fetch_add(1, Ordering::Relaxed);
                                    collector.lock().unwrap().push(TxnRecord {
                                        index: txn_index,
                                        client_id: client_idx,
                                        read_set: reads,
                                        write_set: writes,
                                        outcome: TxnOutcome::TimedOut,
                                        wall_start,
                                        wall_end,
                                    });
                                    // 70% retry, 30% give up.
                                    if attempt < max_retries && rng.gen_range(0..10u8) < 7 {
                                        attempt += 1;
                                        txn_event_log.record(FuzzEvent::TxnRetry {
                                            txn_index, client_id: client_idx,
                                            attempt, keys: keys.clone(),
                                        });
                                        continue;
                                    }
                                    break;
                                }
                            }
                        }
                    } else {
                        // Read-only transaction (20%): quorum reads, no commit.
                        'retry_ro: loop {
                            let txn_index = next_txn_index.fetch_add(1, Ordering::Relaxed) as usize;
                            total_attempted.fetch_add(1, Ordering::Relaxed);
                            let txn = routing_client.begin_read_only();

                            txn_event_log.record(FuzzEvent::TxnBegin {
                                txn_index, client_id: client_idx,
                                txn_type: txn_type_str, keys: keys.clone(),
                            });

                            // 10% chance: drop transaction (misuse simulation).
                            if rng.gen_range(0..10u8) == 0 {
                                drop(txn);
                                txn_event_log.record(FuzzEvent::TxnDropped {
                                    txn_index, client_id: client_idx,
                                });
                                break;
                            }

                            // Wrap quorum reads in a timeout — quorum reads
                            // can block indefinitely if the shard is ViewChanging
                            // during resharding.
                            let ro_result = timeout(Duration::from_secs(10), async {
                                for &key in &keys {
                                    let val = match txn.get(key).await {
                                        Ok(val) => val,
                                        Err(e) => return Err(e),
                                    };
                                    txn_event_log.record(FuzzEvent::TxnGet {
                                        txn_index, client_id: client_idx, key, value: val, stale_note,
                                    });
                                }
                                Ok(())
                            }).await;

                            match ro_result {
                                Ok(Ok(())) => {
                                    // Quorum reads complete — no commit needed.
                                    break;
                                }
                                Ok(Err(_)) => {
                                    // OutOfRange — retry.
                                    txn_event_log.record(FuzzEvent::TxnOutOfRange {
                                        txn_index, client_id: client_idx, key: keys[0],
                                    });
                                    if attempt < max_retries && rng.gen_range(0..10u8) < 7 {
                                        attempt += 1;
                                        txn_event_log.record(FuzzEvent::TxnRetry {
                                            txn_index, client_id: client_idx,
                                            attempt, keys: keys.clone(),
                                        });
                                        continue 'retry_ro;
                                    }
                                    break;
                                }
                                Err(_) => {
                                    // Timed out (shard ViewChanging during resharding).
                                    txn_event_log.record(FuzzEvent::TxnTimedOut {
                                        txn_index, client_id: client_idx,
                                    });
                                    if attempt < max_retries && rng.gen_range(0..10u8) < 7 {
                                        attempt += 1;
                                        txn_event_log.record(FuzzEvent::TxnRetry {
                                            txn_index, client_id: client_idx,
                                            attempt, keys: keys.clone(),
                                        });
                                        continue 'retry_ro;
                                    }
                                    break;
                                }
                            }
                        }
                    }

                    // Inter-transaction delay — spread across resharding duration.
                    FaultyTransport::sleep(Duration::from_millis(rng.gen_range(100..=2000))).await;
                    iteration += 1;
                }
            })
        })
        .collect();

    // Spawn resharding task (admin client workload).
    let reshard_cluster_remote = Arc::clone(&cluster_remote);
    let reshard_config = config.clone();
    let reshard_shard_entries = shard_entries.clone();
    let reshard_event_log = event_log.clone();
    let reshard_final_shards = Arc::clone(&final_shards);
    let reshard_stop = Arc::clone(&stop_flag);
    let reshard_handle = tokio::spawn(async move {
        use crate::tapir::shard_manager::ShardManager;

        // Seed manager's local directory with known memberships.
        // ShardManager initiates all membership changes (join/leave), so its
        // directory is always current without background discovery sync.
        let manager_local = Arc::new(InMemoryShardDirectory::new());
        for (i, m) in initial_memberships.iter().enumerate() {
            manager_local.put(ShardNumber(i as u32), m.clone(), 0);
        }
        for (i, m) in new_shard_memberships.iter().enumerate() {
            manager_local.put(ShardNumber(num_shards + i as u32), m.clone(), 0);
        }
        let manager_channel = registry.channel(
            move |_, _| None, Arc::clone(&manager_local),
        );
        let manager_transport = FaultyChannelTransport::new(
            manager_channel, reshard_config, manager_seed,
        );
        let mut manager = ShardManager::new(
            manager_rng, manager_transport,
            Arc::clone(&reshard_cluster_remote),
        );
        for (i, entry) in reshard_shard_entries.iter().enumerate() {
            manager.register_shard(
                entry.shard, initial_memberships[i].clone(), entry.range.clone(),
            ).await;
        }

        let mut reshard_rng = StdRng::seed_from_u64(reshard_seed);
        let mut next_shard_idx = 0usize;
        let num_reshard_rounds = reshard_rng.gen_range(3..=5usize);

        // Random resharding loop — no guards, attempt anything.
        // No explicit view change forcing needed: split/merge/compact handle
        // CDC internally (freeze triggers view change → produces delta → drain
        // picks it up). The fault injection task provides additional random
        // view changes.
        for round in 0..num_reshard_rounds {
            let round_for_log = round;
            let log_for_cb = reshard_event_log.clone();
            manager.set_progress_callback(move |phase| {
                log_for_cb.record(FuzzEvent::ReshardPhase {
                    round: round_for_log,
                    phase: phase.to_string(),
                });
            });
            #[allow(clippy::disallowed_methods)] // Immediately sorted on next line
            let mut shard_keys: Vec<_> = manager.shards.keys().cloned().collect();
            shard_keys.sort();

            // Try the preferred operation first, then rotate through alternatives.
            // Without fallback, rounds become silent no-ops when the randomly chosen
            // operation's preconditions aren't met (e.g., merge requires >= 2 shards
            // but num_shards == 1), which can produce zero resharding events and
            // fail the "resharding should overlap with client transactions" assertion.
            let preferred_op: u8 = reshard_rng.gen_range(0..3);
            let candidates = [preferred_op, (preferred_op + 1) % 3, (preferred_op + 2) % 3];

            for op in candidates {
                if op == 0 {
                    // Attempt split: random shard, midpoint key.
                    let source = shard_keys[reshard_rng.gen_range(0..shard_keys.len())];
                    let range = &manager.shards[&source].key_range;
                    let start = range.start.unwrap_or(0);
                    let end = range.end.unwrap_or(num_keys);
                    if end - start >= 2 && next_shard_idx < new_shard_memberships.len() {
                        let split_key = start + (end - start) / 2;
                        let new_shard_number = ShardNumber(num_shards + next_shard_idx as u32);
                        let membership = new_shard_memberships[next_shard_idx].clone();
                        reshard_event_log.record(FuzzEvent::ReshardSplitAttempt {
                            round, source_shard: source.0, split_key,
                        });
                        let op_wall_start = std::time::Instant::now();
                        let op_sim_start = tokio::time::Instant::now();
                        match manager.split(source, split_key, new_shard_number, membership).await {
                            Ok(()) => {
                                next_shard_idx += 1;
                                reshard_event_log.record(FuzzEvent::ReshardSplitOk {
                                    round,
                                    wall_ms: op_wall_start.elapsed().as_millis(),
                                    sim_ms: op_sim_start.elapsed().as_millis(),
                                });
                            }
                            Err(e) => reshard_event_log.record(FuzzEvent::ReshardSplitErr {
                                round, error: format!("{e:?}"),
                                wall_ms: op_wall_start.elapsed().as_millis(),
                                sim_ms: op_sim_start.elapsed().as_millis(),
                            }),
                        }
                        break;
                    }
                } else if op == 1 {
                    if shard_keys.len() >= 2 {
                        // Attempt merge: pick any two registered shards.
                        // May be same shard or non-adjacent — ShardManager rejects.
                        let absorbed = shard_keys[reshard_rng.gen_range(0..shard_keys.len())];
                        let surviving = shard_keys[reshard_rng.gen_range(0..shard_keys.len())];

                        reshard_event_log.record(FuzzEvent::ReshardMergeAttempt {
                            round, absorbed: absorbed.0, surviving: surviving.0,
                        });
                        let op_wall_start = std::time::Instant::now();
                        let op_sim_start = tokio::time::Instant::now();
                        match manager.merge(absorbed, surviving).await {
                            Ok(()) => {
                                reshard_event_log.record(FuzzEvent::ReshardMergeOk {
                                    round,
                                    wall_ms: op_wall_start.elapsed().as_millis(),
                                    sim_ms: op_sim_start.elapsed().as_millis(),
                                });
                            }
                            Err(e) => reshard_event_log.record(FuzzEvent::ReshardMergeErr {
                                round, error: format!("{e:?}"),
                                wall_ms: op_wall_start.elapsed().as_millis(),
                                sim_ms: op_sim_start.elapsed().as_millis(),
                            }),
                        }
                        break;
                    }
                } else if next_shard_idx < new_shard_memberships.len() {
                    // Attempt compact: replace source shard with clean replica group.
                    let source = shard_keys[reshard_rng.gen_range(0..shard_keys.len())];
                    let new_shard_number = ShardNumber(num_shards + next_shard_idx as u32);
                    let membership = new_shard_memberships[next_shard_idx].clone();
                    reshard_event_log.record(FuzzEvent::ReshardCompactAttempt {
                        round, source_shard: source.0,
                    });
                    let op_wall_start = std::time::Instant::now();
                    let op_sim_start = tokio::time::Instant::now();
                    match manager.compact(source, new_shard_number, membership).await {
                        Ok(()) => {
                            next_shard_idx += 1;
                            reshard_event_log.record(FuzzEvent::ReshardCompactOk {
                                round,
                                wall_ms: op_wall_start.elapsed().as_millis(),
                                sim_ms: op_sim_start.elapsed().as_millis(),
                            });
                        }
                        Err(e) => reshard_event_log.record(FuzzEvent::ReshardCompactErr {
                            round, error: format!("{e:?}"),
                            wall_ms: op_wall_start.elapsed().as_millis(),
                            sim_ms: op_sim_start.elapsed().as_millis(),
                        }),
                    }
                    break;
                }
            }

        }

        // Signal clients to stop after resharding completes.
        reshard_stop.store(true, Ordering::Relaxed);

        *reshard_final_shards.lock().unwrap() =
            Some(manager.shard_entries());
    });

    // Wait for all workloads with overall timeout.
    let workload_wall_start = std::time::Instant::now();
    let all_done = timeout(Duration::from_secs(300), async {
        let _ = fault_handle.await;
        let mut workload_panic = None;
        for handle in handles {
            if let Err(e) = handle.await
                && workload_panic.is_none()
            {
                workload_panic = Some(e);
            }
        }
        if let Err(e) = reshard_handle.await
            && workload_panic.is_none()
        {
            workload_panic = Some(e);
        }
        workload_panic
    })
    .await;

    let workload_wall_ms = workload_wall_start.elapsed().as_millis();
    match &all_done {
        Err(_) => {
            event_log.record(FuzzEvent::WorkloadTimedOut);
            event_log.dump(seed);
            panic!("fuzz_tapir_transactions: workload timed out (seed={seed})");
        }
        Ok(Some(join_err)) => {
            event_log.dump(seed);
            panic!("fuzz_tapir_transactions: workload panicked (seed={seed}): {join_err}");
        }
        Ok(None) => {
            eprintln!("fuzz: workload completed in {workload_wall_ms}ms wall-clock (seed={seed})");
        }
    }

    route_poll_handle.abort();

    let attempted = total_attempted.load(Ordering::Relaxed);
    let committed = total_committed.load(Ordering::Relaxed);
    let timed_out = may_have_committed.load(Ordering::Relaxed);
    event_log.record(FuzzEvent::MayHaveCommittedCount { count: timed_out });
    eprintln!(
        "fuzz_tapir_transactions: attempted={attempted} committed={committed} \
         timed_out={timed_out} shards={num_shards} seed={seed}"
    );
    if committed == 0 {
        event_log.dump(seed);
        panic!("no transactions committed (seed={seed})");
    }

    // Verify resharding and client transactions actually interleaved.
    if let Some((txn_first, txn_last)) = event_log.txn_time_window()
        && !event_log.has_reshard_event_between(txn_first, txn_last)
    {
        event_log.dump(seed);
        panic!("resharding should overlap with client transactions (seed={seed})");
    }

    // --- Verification phase (30s overall timeout) ---
    let verify_wall_start = std::time::Instant::now();
    let verify_sim_start = tokio::time::Instant::now();
    let verify_result = timeout(Duration::from_secs(30), async {

    let drain_start = std::time::Instant::now();
    eprintln!("fuzz: starting drain sleep (seed={seed})");

    // Let replicas drain pending operations after all view changes settle.
    FaultyTransport::sleep(Duration::from_secs(5)).await;

    let drain_ms = drain_start.elapsed().as_millis();
    eprintln!("fuzz: drain sleep done in {drain_ms}ms (seed={seed})");

    let sync_start = std::time::Instant::now();
    eprintln!("fuzz: starting sync wait (seed={seed})");

    // Wait for final sync propagation after all workloads complete.
    tokio::time::sleep(Duration::from_millis(500)).await;
    tokio::task::yield_now().await;

    let sync_ms = sync_start.elapsed().as_millis();
    eprintln!("fuzz: sync wait done in {sync_ms}ms (seed={seed})");

    let cluster_start = std::time::Instant::now();
    eprintln!("fuzz: verifying cluster_remote shards (seed={seed})");

    // Verify cluster_remote has all active shards.
    use crate::discovery::RemoteShardDirectory;
    {
        let active_shards = final_shards.lock().unwrap().clone();
        if let Some(ref shard_entries) = active_shards {
            for entry in shard_entries {
                let shard_wall_start = std::time::Instant::now();
                let shard_sim_start = tokio::time::Instant::now();
                eprintln!("fuzz: checking cluster_remote shard {:?} (seed={seed})", entry.shard);
                let result = timeout(
                    Duration::from_secs(5),
                    RemoteShardDirectory::<usize, i64>::weak_get(&*cluster_remote, entry.shard),
                ).await;
                match result {
                    Ok(Ok(Some(_))) => {
                        event_log.record(FuzzEvent::VerifyClusterRemoteShardOk {
                            shard: entry.shard.0,
                            wall_ms: shard_wall_start.elapsed().as_millis(),
                            sim_ms: shard_sim_start.elapsed().as_millis(),
                        });
                    }
                    Ok(Ok(None)) => {
                        event_log.dump(seed);
                        panic!(
                            "cluster_remote should contain shard {:?} (seed={seed})",
                            entry.shard
                        );
                    }
                    Ok(Err(e)) => {
                        event_log.dump(seed);
                        panic!(
                            "cluster_remote get shard {:?} failed: {e:?} (seed={seed})",
                            entry.shard
                        );
                    }
                    Err(_) => {
                        event_log.record(FuzzEvent::VerifyPhaseTimedOut {
                            phase: format!("cluster_remote_shard_{}", entry.shard.0),
                            wall_ms: shard_wall_start.elapsed().as_millis(),
                            sim_ms: shard_sim_start.elapsed().as_millis(),
                        });
                        event_log.dump(seed);
                        panic!(
                            "cluster_remote.get(shard={:?}) timed out after 5s (seed={seed})",
                            entry.shard
                        );
                    }
                }
            }
        } else {
            for s in 0..num_shards {
                let shard_wall_start = std::time::Instant::now();
                let shard_sim_start = tokio::time::Instant::now();
                eprintln!("fuzz: checking cluster_remote shard {s} (seed={seed})");
                let shard_num = ShardNumber(s);
                let result = timeout(
                    Duration::from_secs(5),
                    RemoteShardDirectory::<usize, i64>::weak_get(&*cluster_remote, shard_num),
                ).await;
                match result {
                    Ok(Ok(Some(_))) => {
                        event_log.record(FuzzEvent::VerifyClusterRemoteShardOk {
                            shard: s,
                            wall_ms: shard_wall_start.elapsed().as_millis(),
                            sim_ms: shard_sim_start.elapsed().as_millis(),
                        });
                    }
                    Ok(Ok(None)) => {
                        event_log.dump(seed);
                        panic!("cluster_remote should contain shard {s} (seed={seed})");
                    }
                    Ok(Err(e)) => {
                        event_log.dump(seed);
                        panic!("cluster_remote get shard {s} failed: {e:?} (seed={seed})");
                    }
                    Err(_) => {
                        event_log.record(FuzzEvent::VerifyPhaseTimedOut {
                            phase: format!("cluster_remote_shard_{s}"),
                            wall_ms: shard_wall_start.elapsed().as_millis(),
                            sim_ms: shard_sim_start.elapsed().as_millis(),
                        });
                        event_log.dump(seed);
                        panic!(
                            "cluster_remote.get(shard={s}) timed out after 5s (seed={seed})"
                        );
                    }
                }
            }
        }
    } // drop active_shards before counter verification locks final_shards

    let cluster_ms = cluster_start.elapsed().as_millis();
    eprintln!("fuzz: cluster_remote verification done in {cluster_ms}ms (seed={seed})");

    // Keep all discovery infrastructure alive until end of assertions.
    drop(node_cachings);
    drop(client_cachings);

    // Run invariant checker: serializability, strict serializability,
    // cross-shard atomicity (via dependency graph + real-time ordering).
    let inv_start = std::time::Instant::now();
    event_log.record(FuzzEvent::InvariantCheckStart);
    let records = collector.lock().unwrap().clone();
    let checker = InvariantChecker::new(records, seed);
    let check_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        checker.check_all();
    }));
    if let Err(e) = check_result {
        event_log.dump(seed);
        std::panic::resume_unwind(e);
    }
    event_log.record(FuzzEvent::InvariantCheckPassed);

    // Counter invariant: for each key, final value == committed increments.
    // Cross-validate checker's expected_counts against inline committed_counts.
    let checker_counts = checker.expected_counts();
    let inline_counts = committed_counts.lock().unwrap().clone();
    if checker_counts != inline_counts {
        event_log.dump(seed);
        panic!(
            "checker vs inline committed_counts mismatch (seed={seed})\n\
             checker: {checker_counts:?}\n\
             inline:  {inline_counts:?}"
        );
    }

    let inv_ms = inv_start.elapsed().as_millis();
    eprintln!("fuzz: invariant check done in {inv_ms}ms, starting counter verification (seed={seed})");

    // Counter verification uses read-only transactions (quorum reads) rather
    // than RW transactions (single-replica unlogged reads). After resharding,
    // CDC ships data to new shards via invoke_inconsistent (f+1 delivery).
    // A single-replica read might hit one of the f replicas that didn't
    // receive the shipped data. Quorum reads query f+1 replicas and pick the
    // highest timestamp — by quorum intersection, at least one has the data.
    //
    // Verification uses a separate authoritative router ("god's view") built
    // from ShardManager's final shard entries, not the workload router which
    // reflects eventual consistency and may be stale.
    let verify_entries = final_shards.lock().unwrap().clone()
        .unwrap_or_else(|| build_shard_entries(num_shards, num_keys));
    let verify_dir = ShardDirectory::new(verify_entries);
    let verify_router = Arc::new(DynamicRouter::new(Arc::new(RwLock::new(verify_dir))));
    let verify_client = RoutingClient::new(Arc::clone(&clients[0]), verify_router);
    let mut counter_mismatches: Vec<String> = Vec::new();
    let counter_wall_start = std::time::Instant::now();
    for (key, expected) in &inline_counts {
        event_log.record(FuzzEvent::VerifyCounterKeyStart { key: *key });
        let key_wall_start = std::time::Instant::now();
        let key_sim_start = tokio::time::Instant::now();
        eprintln!("fuzz: verify key={key} (seed={seed})");
        let get_result = timeout(Duration::from_secs(5), async {
            let mut attempt = 0u32;
            loop {
                attempt += 1;
                eprintln!("fuzz: verify key={key} attempt={attempt} sim={}ms (seed={seed})",
                    key_sim_start.elapsed().as_millis());
                let txn = verify_client.begin_read_only();
                match txn.get(*key).await {
                    Ok(val) => break Ok(val),
                    Err(TransactionError::Unavailable) => {
                        eprintln!("fuzz: verify key={key} attempt={attempt} got Unavailable, retrying (seed={seed})");
                        tokio::time::sleep(Duration::from_millis(500)).await;
                        continue;
                    }
                    Err(e) => {
                        eprintln!("fuzz: verify key={key} attempt={attempt} got error: {e:?} (seed={seed})");
                        break Err(e);
                    }
                }
            }
        }).await;
        match get_result {
            Ok(Ok(val)) => {
                let actual = val.unwrap_or(0);
                let wall_ms = key_wall_start.elapsed().as_millis();
                let sim_ms = key_sim_start.elapsed().as_millis();
                event_log.record(FuzzEvent::VerifyCounterKeyDone {
                    key: *key, expected: *expected, actual, wall_ms, sim_ms,
                });
                eprintln!(
                    "fuzz: verify key={key} actual={actual} expected={expected} \
                     wall={wall_ms}ms sim={sim_ms}ms (seed={seed})"
                );
                if actual != *expected {
                    counter_mismatches.push(format!(
                        "key={key}: actual={actual} expected={expected}"
                    ));
                }
            }
            Ok(Err(e)) => {
                event_log.dump(seed);
                panic!("counter verify key={key}: {e:?} (seed={seed})");
            }
            Err(_) => {
                event_log.record(FuzzEvent::VerifyPhaseTimedOut {
                    phase: format!("counter_key_{key}"),
                    wall_ms: key_wall_start.elapsed().as_millis(),
                    sim_ms: key_sim_start.elapsed().as_millis(),
                });
                event_log.dump(seed);
                panic!(
                    "counter verify key={key} timed out after 5s (seed={seed}). \
                     quorum_read likely stuck in invoke_inconsistent_with_result retry loop — \
                     check if replicas are ViewChanging."
                );
            }
        }
    }
    let counter_ms = counter_wall_start.elapsed().as_millis();
    eprintln!("fuzz: counter verification done in {counter_ms}ms (seed={seed})");
    if !counter_mismatches.is_empty() {
        event_log.dump(seed);
        panic!(
            "counter invariant violated (seed={seed}):\n  {}",
            counter_mismatches.join("\n  ")
        );
    }
    event_log.record(FuzzEvent::CounterVerifyPassed);

    // --- End of 30s overall verification timeout ---
    }).await;

    if verify_result.is_err() {
        event_log.record(FuzzEvent::VerifyPhaseTimedOut {
            phase: "entire-verification".to_string(),
            wall_ms: verify_wall_start.elapsed().as_millis(),
            sim_ms: verify_sim_start.elapsed().as_millis(),
        });
        event_log.dump(seed);
        panic!(
            "fuzz_tapir_transactions: verification phase timed out after 30s (seed={seed}). \
             Check event log for last completed step."
        );
    }

    eprintln!(
        "fuzz_tapir_transactions: seed={seed} shards={num_shards} \
         replica_counts={replica_counts:?} \
         {committed}/{attempted} committed, invariants passed"
    );

    event_log.dump_if(seed, false);

    // Cancel wall-clock watchdog — test completed successfully.
    let _ = watchdog_cancel_tx.send(());
    if let Err(panic_payload) = watchdog_handle.join() {
        // Watchdog fired just before we cancelled — propagate its panic.
        std::panic::resume_unwind(panic_payload);
    }

    // Keep pre-built shard replicas alive until end of test.
    drop(new_shard_replicas);
}
