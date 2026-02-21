use super::*;

// --- Faulty transport helpers ---

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

    let replicas = (0..num_replicas)
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
                    let upcalls = TapirReplica::new(shard, linearizable);
                    IrReplica::new(replica_rng, membership, upcalls, transport, Some(TapirReplica::tick))
                },
            )
        })
        .collect::<Vec<_>>();

    replicas
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
    for i in 0..num_clients {
        let client_seed = seed.wrapping_add(10000 + i as u64);
        let channel = registry.channel(move |_, _| unreachable!(), Arc::clone(&directories[i]));
        let transport = FaultyChannelTransport::new(channel, config.clone(), client_seed);
        transports.push(transport.clone());
        clients.push(Arc::new(TapirClient::new(rng.fork(), transport)));
    }
    (clients, transports)
}

/// Returns (shards, clients, client_transports, registry, node_shard_sets).
/// `node_shard_sets[node_idx]` = set of ShardNumbers assigned to that node.
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
    Vec<std::collections::HashSet<ShardNumber>>,
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
    let mut node_shard_sets: Vec<std::collections::HashSet<ShardNumber>> =
        (0..num_nodes).map(|_| std::collections::HashSet::new()).collect();

    let mut shards = Vec::new();
    for shard in 0..num_shards {
        let shard_seed = seed.wrapping_add(shard as u64 * 100);
        let count = replica_counts[shard];

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

/// Build a 3-replica discovery shard (K=String, V=String) on a separate
/// ChannelRegistry for the TAPIR-backed discovery cluster.
///
/// Returns (replicas, registry, directory) for fault injection and
/// ShardClient construction.
fn build_discovery_shard(
    rng: &mut crate::Rng,
) -> (
    Vec<Arc<IrReplica<TapirReplica<String, String>, ChannelTransport<TapirReplica<String, String>>>>>,
    ChannelRegistry<TapirReplica<String, String>>,
    Arc<InMemoryShardDirectory<usize>>,
) {
    let registry = ChannelRegistry::default();
    let directory = Arc::new(InMemoryShardDirectory::new());
    let shard = ShardNumber(0);
    let num_replicas = 3;

    let membership = IrMembership::new((0..num_replicas).collect::<Vec<_>>());
    let replicas: Vec<_> = (0..num_replicas)
        .map(|_| {
            let replica_rng = rng.fork();
            let dir = Arc::clone(&directory);
            let m = membership.clone();
            Arc::new_cyclic(
                |weak: &std::sync::Weak<
                    IrReplica<TapirReplica<String, String>, ChannelTransport<TapirReplica<String, String>>>,
                >| {
                    let weak = weak.clone();
                    let channel = registry
                        .channel(move |from, message| weak.upgrade()?.receive(from, message), dir);
                    channel.set_shard(shard);
                    let upcalls = TapirReplica::new(shard, false);
                    IrReplica::new(replica_rng, m, upcalls, channel, Some(TapirReplica::tick))
                },
            )
        })
        .collect();

    directory.put(shard, membership, 0);

    (replicas, registry, directory)
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
// Run once:
//   TAPI_TEST_SEED=<seed> cargo test --lib fuzz_tapir_transactions -- --nocapture
//
// Run with structured event timeline (auto-dumps on failure):
//   FUZZ_VERBOSE=1 TAPI_TEST_SEED=<seed> cargo test --lib fuzz_tapir_transactions -- --nocapture
//
// Run many seeds with summary report:
//   ./scripts/fuzz-multi-seed.sh              (or: make fuzz)
//
// Detect indeterminism (same seed, many runs):
//   ./scripts/detect-fuzz-indeterminism.sh
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn fuzz_tapir_transactions() {
    let seed: u64 = std::env::var("TAPI_TEST_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| thread_rng().r#gen());

    eprintln!("fuzz_tapir_transactions seed={seed}");

    let mut rng = StdRng::seed_from_u64(seed);
    let mut lib_rng = test_rng(seed);

    let event_log = FuzzEventLog::new();

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
    let (discovery_replicas, discovery_registry, discovery_directory) =
        build_discovery_shard(&mut lib_rng);
    // Create a ShardClient for discovery shard 0 and build TapirRemoteShardDirectory.
    let disc_channel: ChannelTransport<TapirReplica<String, String>> = discovery_registry.channel(
        move |_, _| None,
        Arc::clone(&discovery_directory),
    );
    let disc_membership = IrMembership::new((0..3usize).collect());
    let cluster_remote = Arc::new(
        crate::discovery::tapir::TapirRemoteShardDirectory::<usize, _>::with_eventual_consistent_read(
            lib_rng.fork(),
            disc_membership,
            disc_channel,
        ),
    );

    // Create simulated nodes: num_nodes = max replica count across all shards
    // (guarantees no 2 replicas of same shard on same node).
    let num_nodes = replica_counts.iter()
        .chain(new_shard_replica_counts.iter())
        .copied().max().unwrap();
    let node_locals: Vec<Arc<InMemoryShardDirectory<usize>>> = (0..num_nodes)
        .map(|_| Arc::new(InMemoryShardDirectory::new()))
        .collect();
    let node_cachings: Vec<_> = node_locals.iter()
        .map(|local| CachingShardDirectory::new(
            Arc::clone(local), Arc::clone(&cluster_remote), sync_interval,
        ))
        .collect();

    // Create client directories (one per client, separate from replica nodes).
    let client_locals: Vec<Arc<InMemoryShardDirectory<usize>>> = (0..num_clients)
        .map(|_| Arc::new(InMemoryShardDirectory::new()))
        .collect();
    let client_cachings: Vec<_> = client_locals.iter()
        .map(|local| CachingShardDirectory::new(
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
    let final_shards: Arc<Mutex<Option<Vec<ShardNumber>>>> =
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
    let fault_discovery_replicas = discovery_replicas.clone();
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

                        if is_rmw {
                            // RMW transaction (80%): read-modify-write with optional scan.
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
                        } else {
                            // Read-only transaction (20%).
                            for &key in &keys {
                                let val = match txn.get(key).await {
                                    Ok(val) => val,
                                    Err(_) => {
                                        txn_event_log.record(FuzzEvent::TxnOutOfRange {
                                            txn_index, client_id: client_idx, key,
                                        });
                                        break 'retry;
                                    }
                                };
                                txn_event_log.record(FuzzEvent::TxnGet {
                                    txn_index, client_id: client_idx, key, value: val, stale_note,
                                });
                                reads.push((key, val));
                            }
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

                    // Inter-transaction delay — spread across resharding duration.
                    FaultyTransport::sleep(Duration::from_millis(rng.gen_range(100..=2000))).await;
                    iteration += 1;
                }
            })
        })
        .collect();

    // Spawn resharding task (admin client workload).
    let reshard_router = Arc::clone(&router);
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
                        match manager.split(source, split_key, new_shard_number, membership).await {
                            Ok(()) => {
                                next_shard_idx += 1;
                                reshard_router.directory().write().unwrap().update(
                                    manager.shard_entries(),
                                );
                                reshard_event_log.record(FuzzEvent::ReshardSplitOk { round });
                            }
                            Err(e) => reshard_event_log.record(FuzzEvent::ReshardSplitErr {
                                round, error: format!("{e:?}"),
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
                        match manager.merge(absorbed, surviving).await {
                            Ok(()) => {
                                reshard_router.directory().write().unwrap().update(
                                    manager.shard_entries(),
                                );
                                reshard_event_log.record(FuzzEvent::ReshardMergeOk { round });
                            }
                            Err(e) => reshard_event_log.record(FuzzEvent::ReshardMergeErr {
                                round, error: format!("{e:?}"),
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
                    match manager.compact(source, new_shard_number, membership).await {
                        Ok(()) => {
                            next_shard_idx += 1;
                            reshard_router.directory().write().unwrap().update(
                                manager.shard_entries(),
                            );
                            reshard_event_log.record(FuzzEvent::ReshardCompactOk { round });
                        }
                        Err(e) => reshard_event_log.record(FuzzEvent::ReshardCompactErr {
                            round, error: format!("{e:?}"),
                        }),
                    }
                    break;
                }
            }
        }

        // Signal clients to stop after resharding completes.
        reshard_stop.store(true, Ordering::Relaxed);

        *reshard_final_shards.lock().unwrap() =
            Some(manager.shards.keys().cloned().collect());
    });

    // Wait for all workloads with overall timeout.
    let all_done = timeout(Duration::from_secs(300), async {
        let _ = fault_handle.await;
        let mut workload_panic = None;
        for handle in handles {
            if let Err(e) = handle.await {
                if workload_panic.is_none() {
                    workload_panic = Some(e);
                }
            }
        }
        if let Err(e) = reshard_handle.await {
            if workload_panic.is_none() {
                workload_panic = Some(e);
            }
        }
        workload_panic
    })
    .await;

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
        Ok(None) => {}
    }

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
    if let Some((txn_first, txn_last)) = event_log.txn_time_window() {
        if !event_log.has_reshard_event_between(txn_first, txn_last) {
            event_log.dump(seed);
            panic!("resharding should overlap with client transactions (seed={seed})");
        }
    }

    // Let replicas drain pending operations after all view changes settle.
    FaultyTransport::sleep(Duration::from_secs(5)).await;

    // Wait for final sync propagation after all workloads complete.
    tokio::time::sleep(Duration::from_millis(500)).await;
    tokio::task::yield_now().await;

    // Verify cluster_remote has all active shards.
    let active_shards = final_shards.lock().unwrap();
    if let Some(ref shard_list) = *active_shards {
        for &shard in shard_list {
            if cluster_remote.get(shard).await.unwrap().is_none() {
                event_log.dump(seed);
                panic!(
                    "cluster_remote should contain shard {shard:?} (seed={seed})"
                );
            }
        }
    } else {
        for s in 0..num_shards {
            if cluster_remote.get(ShardNumber(s)).await.unwrap().is_none() {
                event_log.dump(seed);
                panic!("cluster_remote should contain shard {s} (seed={seed})");
            }
        }
    }

    // Keep all discovery infrastructure alive until end of assertions.
    drop(node_cachings);
    drop(client_cachings);

    // Run invariant checker: serializability, strict serializability,
    // cross-shard atomicity (via dependency graph + real-time ordering).
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

    // Counter verification uses read-only transactions (quorum reads) rather
    // than RW transactions (single-replica unlogged reads). After resharding,
    // CDC ships data to new shards via invoke_inconsistent (f+1 delivery).
    // A single-replica read might hit one of the f replicas that didn't
    // receive the shipped data. Quorum reads query f+1 replicas and pick the
    // highest timestamp — by quorum intersection, at least one has the data.
    // This also models production behavior where directory propagation is
    // eventually consistent: quorum reads are robust to stale routing.
    let verify_client = RoutingClient::new(Arc::clone(&clients[0]), Arc::clone(&router));
    let mut counter_mismatches: Vec<String> = Vec::new();
    for (key, expected) in &inline_counts {
        let txn = verify_client.begin_read_only();
        let actual = txn.get(*key).await.unwrap().unwrap_or(0);
        if actual != *expected {
            counter_mismatches.push(format!(
                "key={key}: actual={actual} expected={expected}"
            ));
        }
    }
    if !counter_mismatches.is_empty() {
        event_log.dump(seed);
        panic!(
            "counter invariant violated (seed={seed}):\n  {}",
            counter_mismatches.join("\n  ")
        );
    }
    event_log.record(FuzzEvent::CounterVerifyPassed);

    eprintln!(
        "fuzz_tapir_transactions: seed={seed} shards={num_shards} \
         replica_counts={replica_counts:?} \
         {committed}/{attempted} committed, invariants passed"
    );

    event_log.dump_if(seed, false);

    // Keep pre-built shard replicas alive until end of test.
    drop(new_shard_replicas);
}
