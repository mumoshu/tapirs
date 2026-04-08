use std::sync::Arc;
use std::time::Duration;

use crate::mvcc::disk::disk_io::OpenFlags;
use crate::mvcc::disk::memory_io::MemoryIo;
use crate::tapir::{ShardNumber, Sharded};
use crate::transport::{FaultyChannelTransport, NetworkFaultConfig};
use crate::unified::combined::CombinedStoreInner;
use crate::unified::combined::record_handle::CombinedRecordHandle;
use crate::{
    ChannelRegistry, IrMembership, TapirReplica,
    discovery::{InMemoryShardDirectory, ShardDirectory as _},
};
use crate::IrClientId;

type FaultyTransport = FaultyChannelTransport<TapirReplica<String, String>>;
type IrRecordStore = CombinedRecordHandle<String, String, MemoryIo>;
type IrRep = crate::ir::Replica<TapirReplica<String, String>, FaultyTransport, IrRecordStore>;

/// Build a shard: 3 MemoryIo replicas with FaultyChannelTransport.
fn build_shard_faulty(
    rng: &mut crate::Rng,
    shard: ShardNumber,
    registry: &ChannelRegistry<TapirReplica<String, String>>,
    directory: &Arc<InMemoryShardDirectory<usize>>,
    config: &NetworkFaultConfig,
    seed: u64,
) -> Vec<Arc<IrRep>> {
    let num_replicas = 3;
    let initial_address = registry.len();
    let membership = IrMembership::new(
        (0..num_replicas).map(|n| n + initial_address).collect::<Vec<_>>(),
    );

    let replicas: Vec<Arc<IrRep>> = (0..num_replicas)
        .map(|i| {
            let inner = CombinedStoreInner::<String, String, MemoryIo>::open(
                &MemoryIo::temp_path(),
                OpenFlags { create: true, direct: false },
                shard,
                true,
            ).unwrap();
            let record_handle = inner.into_record_handle();
            let tapir_handle = record_handle.tapir_handle();
            let upcalls = TapirReplica::new_with_store(tapir_handle);

            let node_seed = seed.wrapping_add(initial_address as u64 + i as u64);
            let replica_rng = rng.fork();
            let d = Arc::clone(directory);
            let m = membership.clone();
            let cfg = config.clone();
            Arc::new_cyclic(|weak: &std::sync::Weak<IrRep>| {
                let weak = weak.clone();
                let channel = registry.channel(
                    move |from, message| weak.upgrade()?.receive(from, message),
                    Arc::clone(&d),
                );
                let transport = FaultyChannelTransport::new(channel, cfg, node_seed);
                transport.set_shard(shard);
                crate::ir::Replica::new(
                    replica_rng,
                    m.clone(),
                    upcalls,
                    transport,
                    Some(TapirReplica::tick),
                    record_handle,
                )
            })
        })
        .collect();

    directory.put(shard, membership, 0);
    replicas
}

/// Apply partition on all transports (replicas + client).
fn partition_all(
    replicas: &[Arc<IrRep>],
    client_transport: &FaultyTransport,
    target_addr: usize,
) {
    for replica in replicas {
        replica.transport().partition_node(target_addr);
    }
    client_transport.partition_node(target_addr);
}

/// Heal partition on all transports (replicas + client).
fn heal_all(
    replicas: &[Arc<IrRep>],
    client_transport: &FaultyTransport,
    target_addr: usize,
) {
    for replica in replicas {
        replica.transport().heal_node(target_addr);
    }
    client_transport.heal_node(target_addr);
}

/// Multiple view changes with different replicas partitioned each time.
///
/// Exercises the delta-only view change path (install_start_view_unified with
/// ViewRange-based segment skipping) under network partitions. Each view change
/// partitions a different replica so that:
/// - Participating replicas receive delta payloads (ViewRange skipping)
/// - Previously-partitioned replicas catch up via full StartView payloads
/// - Data written between view changes is preserved across all replicas
#[tokio::test(start_paused = true)]
async fn e2e_delta_view_change_under_partition() {
    let shard = ShardNumber(0);
    let fault_config = NetworkFaultConfig::default();

    let mut rng = crate::Rng::from_seed(77);
    let registry = ChannelRegistry::<TapirReplica<String, String>>::default();
    let dir = Arc::new(InMemoryShardDirectory::new());

    let replicas = build_shard_faulty(
        &mut rng, shard, &registry, &dir, &fault_config, 100,
    );
    tokio::time::sleep(Duration::from_secs(3)).await;

    let client_channel = registry.channel(
        move |_, _| unreachable!(),
        Arc::clone(&dir),
    );
    let client_transport = FaultyChannelTransport::new(
        client_channel, fault_config.clone(), 200,
    );
    let client = Arc::new(
        crate::tapir::Client::<String, String, FaultyTransport>::new(
            rng.fork(), client_transport.clone(),
        ),
    );

    // Write k1=v1 and force view change to seal data into segments.
    eprintln!("[e2e-delta] writing k1=v1...");
    let txn = client.begin();
    txn.put(Sharded { shard, key: "k1".to_string() }, Some("v1".to_string()));
    let commit_ts = txn.commit().await.expect("write k1 should commit");
    eprintln!("[e2e-delta] committed k1 at ts={commit_ts:?}");

    eprintln!("[e2e-delta] VC 0: initial view change to seal segments...");
    replicas[0].force_view_change();
    tokio::time::sleep(Duration::from_secs(3)).await;

    // --- VC 1: partition replica[2] (C), A+B complete ---
    let addr_c = replicas[2].address();
    eprintln!("[e2e-delta] VC 1: partitioning replica[2] (addr={addr_c})...");
    partition_all(&replicas, &client_transport, addr_c);

    replicas[0].force_view_change();
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Write k2=v2 (quorum from A+B)
    eprintln!("[e2e-delta] writing k2=v2 (A+B quorum)...");
    let txn2 = client.begin();
    txn2.put(Sharded { shard, key: "k2".to_string() }, Some("v2".to_string()));
    let commit2 = txn2.commit().await.expect("write k2 should commit with A+B quorum");
    eprintln!("[e2e-delta] committed k2 at ts={commit2:?}");

    // --- VC 2: heal C, partition replica[0] (A), B+C complete ---
    let addr_a = replicas[0].address();
    eprintln!("[e2e-delta] VC 2: healing C, partitioning replica[0] (addr={addr_a})...");
    heal_all(&replicas, &client_transport, addr_c);
    partition_all(&replicas, &client_transport, addr_a);

    replicas[1].force_view_change();
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Write k3=v3 (quorum from B+C)
    eprintln!("[e2e-delta] writing k3=v3 (B+C quorum)...");
    let txn3 = client.begin();
    txn3.put(Sharded { shard, key: "k3".to_string() }, Some("v3".to_string()));
    let commit3 = txn3.commit().await.expect("write k3 should commit with B+C quorum");
    eprintln!("[e2e-delta] committed k3 at ts={commit3:?}");

    // --- VC 3: heal A, partition replica[1] (B), A+C complete ---
    let addr_b = replicas[1].address();
    eprintln!("[e2e-delta] VC 3: healing A, partitioning replica[1] (addr={addr_b})...");
    heal_all(&replicas, &client_transport, addr_a);
    partition_all(&replicas, &client_transport, addr_b);

    replicas[2].force_view_change();
    tokio::time::sleep(Duration::from_secs(3)).await;

    // --- VC 4: heal B, all 3 participate ---
    eprintln!("[e2e-delta] VC 4: healing B, all replicas participate...");
    heal_all(&replicas, &client_transport, addr_b);

    replicas[0].force_view_change();
    tokio::time::sleep(Duration::from_secs(3)).await;

    // --- Verify: quorum_read all keys ---
    let verify_channel = registry.channel(
        move |_, _| unreachable!(),
        Arc::clone(&dir),
    );
    let verify_transport = FaultyChannelTransport::new(
        verify_channel, fault_config.clone(), 500,
    );
    let shard_client = crate::tapir::ShardClient::<String, String, FaultyTransport>::new(
        rng.fork(),
        IrClientId::new(&mut rng),
        shard,
        IrMembership::new(vec![0, 1, 2]),
        verify_transport,
    );

    eprintln!("[e2e-delta] quorum_read k1 at ts={commit_ts:?}...");
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        shard_client.quorum_read("k1".to_string(), commit_ts),
    ).await.expect("quorum_read k1 timed out");
    assert!(result.is_ok(), "quorum_read k1 failed: {result:?}");
    let (value, _) = result.unwrap();
    assert_eq!(value.as_deref(), Some("v1"), "k1 should be v1");

    eprintln!("[e2e-delta] quorum_read k2 at ts={commit2:?}...");
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        shard_client.quorum_read("k2".to_string(), commit2),
    ).await.expect("quorum_read k2 timed out");
    assert!(result.is_ok(), "quorum_read k2 failed: {result:?}");
    let (value, _) = result.unwrap();
    assert_eq!(value.as_deref(), Some("v2"), "k2 should be v2");

    eprintln!("[e2e-delta] quorum_read k3 at ts={commit3:?}...");
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        shard_client.quorum_read("k3".to_string(), commit3),
    ).await.expect("quorum_read k3 timed out");
    assert!(result.is_ok(), "quorum_read k3 failed: {result:?}");
    let (value, _) = result.unwrap();
    assert_eq!(value.as_deref(), Some("v3"), "k3 should be v3");

    eprintln!("[e2e-delta] all quorum_reads passed!");
}
