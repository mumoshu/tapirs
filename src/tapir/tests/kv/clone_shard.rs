use super::*;

use crate::nodecluster::SoloClusterManager;
use crate::tapir::ShardClient;
use crate::IrClientId;

#[tokio::test(start_paused = true)]
async fn test_clone_shard_basic() {
    init_tracing();

    let mut rng = test_rng(301);
    let registry = ChannelRegistry::default();
    let dir = Arc::new(InMemoryShardDirectory::new());

    // Build source shard 0 (3 replicas).
    let _replicas_0 = build_shard(&mut rng, ShardNumber(0), false, 3, &registry, &dir);
    // Address space: [0, 1, 2] = shard 0 replicas.
    let clients = build_clients(&mut rng, 1, &registry, &dir);
    // Address space: [3] = client.

    // Commit some data to the source shard.
    let txn = clients[0].begin();
    txn.put(Sharded { shard: ShardNumber(0), key: 100 }, Some(1000));
    txn.put(Sharded { shard: ShardNumber(0), key: 200 }, Some(2000));
    txn.put(Sharded { shard: ShardNumber(0), key: 300 }, Some(3000));
    assert!(txn.commit().await.is_some(), "initial writes should succeed");
    Transport::sleep(Duration::from_millis(1)).await;

    // Commit more data.
    let txn = clients[0].begin();
    txn.put(Sharded { shard: ShardNumber(0), key: 400 }, Some(4000));
    assert!(txn.commit().await.is_some(), "second write should succeed");
    Transport::sleep(Duration::from_millis(1)).await;

    // Force view change so CDC deltas are captured.
    _replicas_0[0].force_view_change();
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Build destination shard 1 (3 replicas).
    let _replicas_1 = build_shard(&mut rng, ShardNumber(1), false, 3, &registry, &dir);
    // Address space: [4, 5, 6] = shard 1 replicas.

    // Create ShardClients for source and destination.
    let source_client: ShardClient<K, V, Transport> = ShardClient::new(
        rng.fork(),
        IrClientId::new(&mut rng),
        ShardNumber(0),
        IrMembership::new(vec![0, 1, 2]),
        registry.channel(move |_, _| unreachable!(), Arc::clone(&dir)),
    );
    // Address space: [7] = source ShardClient.

    let dest_client: ShardClient<K, V, Transport> = ShardClient::new(
        rng.fork(),
        IrClientId::new(&mut rng),
        ShardNumber(1),
        IrMembership::new(vec![4, 5, 6]),
        registry.channel(move |_, _| unreachable!(), Arc::clone(&dir)),
    );
    // Address space: [8] = dest ShardClient.

    // Clone shard from source to destination.
    let mut manager = SoloClusterManager::new(rng.fork());
    manager
        .clone_shard(&source_client, &dest_client, ShardNumber(1))
        .await
        .unwrap();

    // Verify: read data from destination shard via unlogged get.
    let (val, _ts) = dest_client.get(100, None).await.unwrap();
    assert_eq!(val, Some(1000), "key 100 should be cloned to dest");

    let (val, _ts) = dest_client.get(200, None).await.unwrap();
    assert_eq!(val, Some(2000), "key 200 should be cloned to dest");

    let (val, _ts) = dest_client.get(300, None).await.unwrap();
    assert_eq!(val, Some(3000), "key 300 should be cloned to dest");

    let (val, _ts) = dest_client.get(400, None).await.unwrap();
    assert_eq!(val, Some(4000), "key 400 should be cloned to dest");

    // Verify: key that was never written should not exist on dest.
    let (val, _ts) = dest_client.get(999, None).await.unwrap();
    assert_eq!(val, None, "key 999 should not exist on dest");

    drop(_replicas_0);
    drop(_replicas_1);
}

#[tokio::test(start_paused = true)]
async fn test_clone_shard_transfers_read_protection() {
    use crate::OccPrepareResult;

    init_tracing();

    let mut rng = test_rng(302);
    let registry = ChannelRegistry::default();
    let dir = Arc::new(InMemoryShardDirectory::new());

    // Build source shard 0 (3 replicas, linearizable for quorum read).
    let _replicas_0 = build_shard(&mut rng, ShardNumber(0), true, 3, &registry, &dir);
    let clients = build_clients(&mut rng, 1, &registry, &dir);

    // Commit data to source.
    let txn = clients[0].begin();
    txn.put(Sharded { shard: ShardNumber(0), key: 50 }, Some(500));
    assert!(txn.commit().await.is_some(), "write should succeed");
    Transport::sleep(Duration::from_millis(1)).await;

    // Do a quorum read at a known timestamp — sets last_read_commit_ts.
    let read_ts = TapirTimestamp { time: 800, client_id: IrClientId(999) };
    let membership_0 = IrMembership::new(vec![0, 1, 2]);
    let shard_client_0: ShardClient<K, V, Transport> = ShardClient::new(
        rng.fork(),
        IrClientId::new(&mut rng),
        ShardNumber(0),
        membership_0,
        registry.channel(move |_, _| unreachable!(), Arc::clone(&dir)),
    );
    let (value, _wts) = shard_client_0.quorum_read(50, read_ts).await.unwrap();
    assert_eq!(value, Some(500), "quorum_read should return committed value");

    // Force view change so CDC deltas are captured.
    _replicas_0[0].force_view_change();
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Build destination shard 1 (3 replicas).
    let _replicas_1 = build_shard(&mut rng, ShardNumber(1), true, 3, &registry, &dir);

    // Create ShardClients for clone.
    let source_clone: ShardClient<K, V, Transport> = ShardClient::new(
        rng.fork(),
        IrClientId::new(&mut rng),
        ShardNumber(0),
        IrMembership::new(vec![0, 1, 2]),
        registry.channel(move |_, _| unreachable!(), Arc::clone(&dir)),
    );
    let dest_clone: ShardClient<K, V, Transport> = ShardClient::new(
        rng.fork(),
        IrClientId::new(&mut rng),
        ShardNumber(1),
        IrMembership::new(vec![5, 6, 7]),
        registry.channel(move |_, _| unreachable!(), Arc::clone(&dir)),
    );

    // Clone shard.
    let mut manager = SoloClusterManager::new(rng.fork());
    manager
        .clone_shard(&source_clone, &dest_clone, ShardNumber(1))
        .await
        .unwrap();

    // After clone: prepare a write to key=50 on dest with commit_ts.time < read_ts.time (800).
    // This MUST be rejected as TooLate due to transferred read protection.
    let dest_check: ShardClient<K, V, Transport> = ShardClient::new(
        rng.fork(),
        IrClientId::new(&mut rng),
        ShardNumber(1),
        IrMembership::new(vec![5, 6, 7]),
        registry.channel(move |_, _| unreachable!(), Arc::clone(&dir)),
    );
    let old_ts = TapirTimestamp { time: 200, client_id: IrClientId(888) };
    let txn_id = crate::OccTransactionId {
        client_id: IrClientId(888),
        number: 1,
    };
    let mut write_set = BTreeMap::new();
    write_set.insert(
        Sharded { shard: ShardNumber(1), key: 50 },
        Some(999),
    );
    let transaction = Arc::new(crate::OccTransaction {
        read_set: BTreeMap::new(),
        write_set,
        scan_set: Vec::new(),
    });
    let result = dest_check.prepare(txn_id, &transaction, old_ts).await;
    assert!(
        matches!(result, OccPrepareResult::TooLate),
        "prepare at time={} (below barrier from read_ts.time={}) should be TooLate, got {:?}",
        old_ts.time, read_ts.time, result
    );

    drop(_replicas_0);
    drop(_replicas_1);
}
