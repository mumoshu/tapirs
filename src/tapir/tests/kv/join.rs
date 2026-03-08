use super::*;

use crate::ShardManager;
use crate::testing::discovery::build_single_node_discovery;

#[tokio::test(start_paused = true)]
async fn test_join_with_preload() {
    init_tracing();

    let mut rng = test_rng(42);
    let shard = ShardNumber(0);
    let registry = ChannelRegistry::default();
    let dir = Arc::new(InMemoryShardDirectory::new());

    // Build 3-replica shard and 1 client.
    let replicas = build_shard(&mut rng, shard, true, 3, &registry, &dir);
    let clients = build_clients(&mut rng, 1, &registry, &dir);
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
            IrReplica<TapirReplica<K, V>, ChannelTransport<TapirReplica<K, V>>, TapirIrRecord>,
        >| {
            let weak = weak.clone();
            let channel =
                registry.channel(move |from, message| weak.upgrade()?.receive(from, message), Arc::clone(&dir));
            channel.set_shard(shard);
            let upcalls = TapirReplica::new_with_backend(shard, true,
                DiskStore::<K, V, Timestamp, MemoryIo>::open(
                    MemoryIo::temp_path(),
                ).unwrap(),
            );
            // Start with membership=[self] only — the real membership comes via AddMember.
            IrReplica::new(
                rng.fork(),
                IrMembership::new(vec![new_address]),
                upcalls,
                channel,
                Some(TapirReplica::tick),
                Default::default(),
            )
        },
    );

    // Set up ShardManager with the original 3-replica membership.
    let disc = build_single_node_discovery(&mut rng);
    let manager_channel = registry.channel(move |_, _| None, Arc::clone(&dir));
    let original_membership =
        IrMembership::new((0..3).collect::<Vec<_>>());
    let mut manager = ShardManager::new(rng.fork(), manager_channel, disc.create_remote(&mut rng));
    manager.register_active_shard(shard, original_membership, KeyRange {
        start: None,
        end: None,
    }).await.unwrap();

    // join: discover membership from remote → fetch leader_record → bootstrap R4 → AddMember.
    manager.join(shard, new_address).await.unwrap();

    // Wait for the view change (AddMember → N+3) to complete.
    tokio::time::sleep(Duration::from_secs(10)).await;

    // Verify: read key=1 through the 4-replica group.
    // The 4th replica should have the committed data from the bootstrap.
    let ro = routing_client.begin_read_only(Duration::ZERO);
    let val = ro.get(1_i64).await.unwrap();
    assert_eq!(val, Some(42), "key=1 should be readable after join");

    // Verify: new write succeeds through the 4-replica group.
    let txn = routing_client.begin();
    txn.put(2_i64, Some(99_i64));
    assert!(
        txn.commit().await.is_some(),
        "new write should succeed after join"
    );

    // In IR, invoke_inconsistent only delivers the propose to f+1 replicas
    // (JoinUntil early-returns after the quorum). The remaining replicas get
    // the data during the next view change merge. Trigger a view change to
    // ensure all 4 replicas have the committed data before reading.
    replicas[0].force_view_change();
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Verify: read back the new write (any replica should have it after merge).
    let ro = routing_client.begin_read_only(Duration::ZERO);
    let val = ro.get(2_i64).await.unwrap();
    assert_eq!(val, Some(99), "key=2 should be readable after view change merge");

    // Gracefully shut down discovery cluster to drain spawned tasks.
    disc.shutdown().await;
    // Keep replicas alive for the duration of the test.
    drop(new_replica);
    drop(replicas);
}
