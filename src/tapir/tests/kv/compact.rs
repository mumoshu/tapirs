use super::*;

use crate::tapir::shard_manager::ShardManager;
use crate::tapir::ShardClient;
use crate::IrClientId;
use crate::OccPrepareResult;

#[tokio::test(start_paused = true)]
async fn test_compact_new_shard_rejects_old_prepare_after_range_scan_on_old_shard() {
    init_tracing();

    let mut rng = test_rng(200);
    let registry = ChannelRegistry::default();
    let dir = Arc::new(InMemoryShardDirectory::new());

    // Build shard 0 (3 replicas, linearizable).
    let _replicas_0 = build_shard(&mut rng, ShardNumber(0), true, 3, &registry, &dir);
    let clients = build_clients(&mut rng, 1, &registry, &dir);

    // Commit a key so there's data.
    let txn = clients[0].begin();
    txn.put(Sharded { shard: ShardNumber(0), key: 10 }, Some(100));
    assert!(txn.commit().await.is_some(), "initial write should succeed");
    Transport::sleep(Duration::from_millis(1)).await;

    // Do IO::QuorumScan at a known timestamp via ShardClient.
    // This creates a range_reads entry on shard 0 replicas.
    let scan_ts = TapirTimestamp { time: 500, client_id: IrClientId(999) };
    let membership_0 = IrMembership::new(vec![0, 1, 2]);
    let shard_client_0: ShardClient<K, V, Transport> = ShardClient::new(
        rng.fork(),
        IrClientId::new(&mut rng),
        ShardNumber(0),
        membership_0,
        registry.channel(move |_, _| unreachable!(), Arc::clone(&dir)),
    );
    let _scan_results = shard_client_0.quorum_scan(0, 100, scan_ts).await;

    // Force view change so CDC deltas are captured.
    _replicas_0[0].force_view_change();
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Build new shard replicas for shard 1 (compact target).
    let _replicas_1 = build_shard(&mut rng, ShardNumber(1), true, 3, &registry, &dir);

    // Set up ShardManager, register shard 0, compact to shard 1.
    let manager_channel = registry.channel(move |_, _| None, Arc::clone(&dir));
    let mut manager = ShardManager::new(rng.fork(), manager_channel, Arc::new(InMemoryRemoteDirectory::new()));
    manager.register_shard(
        ShardNumber(0),
        IrMembership::new(vec![0, 1, 2]),
        KeyRange { start: None, end: None },
    ).await;

    // Addresses assigned: shard 0 = [0,1,2], clients = [3], shard_client_0 = [4],
    // shard 1 = [5,6,7].
    let new_membership = IrMembership::new(vec![5, 6, 7]);
    manager
        .compact(ShardNumber(0), ShardNumber(1), new_membership)
        .await
        .unwrap();

    // After compact: prepare a write to key=10 on the new shard (shard 1)
    // with commit_ts.time < scan_ts.time (500). This MUST be rejected as TooLate.
    let shard_client_1: ShardClient<K, V, Transport> = ShardClient::new(
        rng.fork(),
        IrClientId::new(&mut rng),
        ShardNumber(1),
        IrMembership::new(vec![5, 6, 7]),
        registry.channel(move |_, _| unreachable!(), Arc::clone(&dir)),
    );
    let old_ts = TapirTimestamp { time: 100, client_id: IrClientId(888) };
    let txn_id = crate::OccTransactionId {
        client_id: IrClientId(888),
        number: 1,
    };
    let mut write_set = BTreeMap::new();
    write_set.insert(
        Sharded { shard: ShardNumber(1), key: 10 },
        Some(999),
    );
    let transaction = Arc::new(crate::OccTransaction {
        read_set: BTreeMap::new(),
        write_set,
        scan_set: Vec::new(),
    });
    let result = shard_client_1.prepare(txn_id, &transaction, old_ts).await;
    assert!(
        matches!(result, OccPrepareResult::TooLate),
        "prepare at time={} (below scan_ts.time={}) should be TooLate, got {:?}",
        old_ts.time, scan_ts.time, result
    );

    drop(_replicas_0);
    drop(_replicas_1);
}

#[tokio::test(start_paused = true)]
async fn test_compact_new_shard_rejects_old_prepare_after_quorum_read_on_old_shard() {
    init_tracing();

    let mut rng = test_rng(201);
    let registry = ChannelRegistry::default();
    let dir = Arc::new(InMemoryShardDirectory::new());

    // Build shard 0 (3 replicas, linearizable).
    let _replicas_0 = build_shard(&mut rng, ShardNumber(0), true, 3, &registry, &dir);
    let clients = build_clients(&mut rng, 1, &registry, &dir);

    // Commit a key so there's data to read.
    let txn = clients[0].begin();
    txn.put(Sharded { shard: ShardNumber(0), key: 20 }, Some(200));
    assert!(txn.commit().await.is_some(), "initial write should succeed");
    Transport::sleep(Duration::from_millis(1)).await;

    // Do IO::QuorumRead at a known timestamp via ShardClient.
    // This sets last_read_commit_ts on the version in shard 0 replicas.
    let read_ts = TapirTimestamp { time: 600, client_id: IrClientId(999) };
    let membership_0 = IrMembership::new(vec![0, 1, 2]);
    let shard_client_0: ShardClient<K, V, Transport> = ShardClient::new(
        rng.fork(),
        IrClientId::new(&mut rng),
        ShardNumber(0),
        membership_0,
        registry.channel(move |_, _| unreachable!(), Arc::clone(&dir)),
    );
    let (value, _write_ts) = shard_client_0.quorum_read(20, read_ts).await.unwrap();
    assert_eq!(value, Some(200), "quorum_read should return committed value");

    // Force view change so CDC deltas are captured.
    _replicas_0[0].force_view_change();
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Build new shard replicas for shard 1 (compact target).
    let _replicas_1 = build_shard(&mut rng, ShardNumber(1), true, 3, &registry, &dir);

    // Set up ShardManager, register shard 0, compact to shard 1.
    let manager_channel = registry.channel(move |_, _| None, Arc::clone(&dir));
    let mut manager = ShardManager::new(rng.fork(), manager_channel, Arc::new(InMemoryRemoteDirectory::new()));
    manager.register_shard(
        ShardNumber(0),
        IrMembership::new(vec![0, 1, 2]),
        KeyRange { start: None, end: None },
    ).await;

    // Addresses assigned: shard 0 = [0,1,2], clients = [3], shard_client_0 = [4],
    // shard 1 = [5,6,7].
    let new_membership = IrMembership::new(vec![5, 6, 7]);
    manager
        .compact(ShardNumber(0), ShardNumber(1), new_membership)
        .await
        .unwrap();

    // After compact: prepare a write to key=20 on the new shard (shard 1)
    // with commit_ts.time < read_ts.time (600). This MUST be rejected as TooLate.
    let shard_client_1: ShardClient<K, V, Transport> = ShardClient::new(
        rng.fork(),
        IrClientId::new(&mut rng),
        ShardNumber(1),
        IrMembership::new(vec![5, 6, 7]),
        registry.channel(move |_, _| unreachable!(), Arc::clone(&dir)),
    );
    let old_ts = TapirTimestamp { time: 150, client_id: IrClientId(888) };
    let txn_id = crate::OccTransactionId {
        client_id: IrClientId(888),
        number: 1,
    };
    let mut write_set = BTreeMap::new();
    write_set.insert(
        Sharded { shard: ShardNumber(1), key: 20 },
        Some(999),
    );
    let transaction = Arc::new(crate::OccTransaction {
        read_set: BTreeMap::new(),
        write_set,
        scan_set: Vec::new(),
    });
    let result = shard_client_1.prepare(txn_id, &transaction, old_ts).await;
    assert!(
        matches!(result, OccPrepareResult::TooLate),
        "prepare at time={} (below read_ts.time={}) should be TooLate, got {:?}",
        old_ts.time, read_ts.time, result
    );

    drop(_replicas_0);
    drop(_replicas_1);
}
