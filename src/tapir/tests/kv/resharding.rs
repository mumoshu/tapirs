use super::*;

use crate::discovery::{RemoteShardDirectory, ShardStatus};
use crate::ShardManager;
use crate::testing::discovery::build_single_node_discovery;

#[tokio::test(start_paused = true)]
async fn test_merge_two_shards() {
    init_tracing();

    let mut rng = test_rng(42);
    let registry = ChannelRegistry::default();
    let dir = Arc::new(InMemoryShardDirectory::new());

    // Build 2 adjacent shards: shard 0 covers [None, 50), shard 1 covers [50, None).
    let _replicas_0 = build_shard(&mut rng, ShardNumber(0), false, 3, &registry, &dir);
    let _replicas_1 = build_shard(&mut rng, ShardNumber(1), false, 3, &registry, &dir);
    let clients = build_clients(&mut rng, 1, &registry, &dir);

    // Commit data on each shard.
    let txn = clients[0].begin();
    txn.put(Sharded { shard: ShardNumber(0), key: 10 }, Some(100));
    assert!(txn.commit().await.is_some(), "shard 0 write should succeed");

    Transport::sleep(Duration::from_millis(1)).await;

    let txn = clients[0].begin();
    txn.put(Sharded { shard: ShardNumber(1), key: 60 }, Some(600));
    assert!(txn.commit().await.is_some(), "shard 1 write should succeed");

    // Force view changes so CDC deltas are captured.
    _replicas_0[0].force_view_change();
    _replicas_1[0].force_view_change();
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Set up ShardManager.
    let disc = build_single_node_discovery(&mut rng);
    let manager_channel = registry.channel(move |_, _| None, Arc::clone(&dir));
    let mut manager = ShardManager::new(rng.fork(), manager_channel, disc.create_remote(&mut rng));
    manager.register_active_shard(
        ShardNumber(0),
        IrMembership::new(vec![0, 1, 2]),
        KeyRange { start: None, end: Some(50) },
    ).await;
    manager.register_active_shard(
        ShardNumber(1),
        IrMembership::new(vec![3, 4, 5]),
        KeyRange { start: Some(50), end: None },
    ).await;

    // Merge: shard 1 absorbed into shard 0.
    manager.merge(ShardNumber(1), ShardNumber(0)).await.unwrap();

    // Verify: read key=10 (originally on shard 0) — still accessible.
    let txn = clients[0].begin();
    let val = txn.get(Sharded { shard: ShardNumber(0), key: 10 }).await.unwrap();
    assert_eq!(val, Some(100), "key=10 should still be readable after merge");
    assert!(txn.commit().await.is_some());

    // Verify: read key=60 (originally on shard 1, shipped to shard 0).
    Transport::sleep(Duration::from_millis(1)).await;
    let txn = clients[0].begin();
    let val = txn.get(Sharded { shard: ShardNumber(0), key: 60 }).await.unwrap();
    assert_eq!(val, Some(600), "key=60 should be readable on surviving shard after merge");
    assert!(txn.commit().await.is_some());

    // Verify: new write on the merged range succeeds.
    Transport::sleep(Duration::from_millis(1)).await;
    let txn = clients[0].begin();
    txn.put(Sharded { shard: ShardNumber(0), key: 70 }, Some(700));
    assert!(txn.commit().await.is_some(), "write key=70 should succeed after merge");

    // Verify: shard 1 is tombstoned in remote discovery.
    let record: Option<crate::discovery::ShardRecord<usize, i64>> =
        manager.remote.strong_get_shard(ShardNumber(1)).await.unwrap();
    assert_eq!(record.unwrap().status, ShardStatus::Tombstoned,
        "shard 1 should be tombstoned after merge");

    // Verify: shard 0 now covers [None, None) in remote discovery.
    let record: Option<crate::discovery::ShardRecord<usize, i64>> =
        manager.remote.strong_get_shard(ShardNumber(0)).await.unwrap();
    let r = record.unwrap();
    assert_eq!(r.status, ShardStatus::Active);
    let range = r.key_range.unwrap();
    assert_eq!(range.start, None, "shard 0 start should be None");
    assert_eq!(range.end, None, "shard 0 end should be None");

    disc.shutdown().await;
    drop(_replicas_0);
    drop(_replicas_1);
}

#[tokio::test(start_paused = true)]
async fn test_split_merge_two_shards() {
    init_tracing();

    let mut rng = test_rng(42);
    let registry = ChannelRegistry::default();
    let dir = Arc::new(InMemoryShardDirectory::new());

    // Build 1 shard covering [None, None) with 3 replicas.
    let _replicas_0 = build_shard(&mut rng, ShardNumber(0), false, 3, &registry, &dir);
    let clients = build_clients(&mut rng, 1, &registry, &dir);

    // Commit 4 keys.
    for (key, val) in [(10, 100), (30, 300), (60, 600), (80, 800)] {
        let txn = clients[0].begin();
        txn.put(Sharded { shard: ShardNumber(0), key }, Some(val));
        assert!(txn.commit().await.is_some(), "commit key={key} should succeed");
        Transport::sleep(Duration::from_millis(1)).await;
    }

    // Force view change for CDC deltas.
    _replicas_0[0].force_view_change();
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Set up ShardManager.
    let disc = build_single_node_discovery(&mut rng);
    let manager_channel = registry.channel(move |_, _| None, Arc::clone(&dir));
    let mut manager = ShardManager::new(rng.fork(), manager_channel, disc.create_remote(&mut rng));
    manager.register_active_shard(
        ShardNumber(0),
        IrMembership::new(vec![0, 1, 2]),
        KeyRange { start: None, end: None },
    ).await;

    // Split at key=50: shard 0 gets [None, 50), shard 1 gets [50, None).
    let _replicas_1 = build_shard(&mut rng, ShardNumber(1), false, 3, &registry, &dir);
    let new_membership = IrMembership::new(vec![4, 5, 6]);
    manager.split(ShardNumber(0), 50, ShardNumber(1), new_membership).await.unwrap();

    // Verify split: read all 4 keys from correct shards.
    Transport::sleep(Duration::from_millis(1)).await;
    for (shard, key, expected) in [
        (0, 10, 100), (0, 30, 300),
        (1, 60, 600), (1, 80, 800),
    ] {
        let txn = clients[0].begin();
        let val = txn.get(Sharded { shard: ShardNumber(shard), key }).await.unwrap();
        assert_eq!(val, Some(expected), "after split: shard={shard} key={key}");
        assert!(txn.commit().await.is_some());
        Transport::sleep(Duration::from_millis(1)).await;
    }

    // Write additional keys on each shard.
    let txn = clients[0].begin();
    txn.put(Sharded { shard: ShardNumber(0), key: 20 }, Some(200));
    assert!(txn.commit().await.is_some());
    Transport::sleep(Duration::from_millis(1)).await;

    let txn = clients[0].begin();
    txn.put(Sharded { shard: ShardNumber(1), key: 70 }, Some(700));
    assert!(txn.commit().await.is_some());

    // Force view changes for CDC deltas before merge.
    _replicas_0[0].force_view_change();
    _replicas_1[0].force_view_change();
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Merge: shard 1 absorbed back into shard 0.
    manager.merge(ShardNumber(1), ShardNumber(0)).await.unwrap();

    // Verify merge: read all 6 keys from shard 0.
    Transport::sleep(Duration::from_millis(1)).await;
    for (key, expected) in [
        (10, 100), (20, 200), (30, 300),
        (60, 600), (70, 700), (80, 800),
    ] {
        let txn = clients[0].begin();
        let val = txn.get(Sharded { shard: ShardNumber(0), key }).await.unwrap();
        assert_eq!(val, Some(expected), "after merge: key={key}");
        assert!(txn.commit().await.is_some());
        Transport::sleep(Duration::from_millis(1)).await;
    }

    // Verify: new write on the merged range succeeds.
    let txn = clients[0].begin();
    txn.put(Sharded { shard: ShardNumber(0), key: 90 }, Some(900));
    assert!(txn.commit().await.is_some(), "write key=90 should succeed after merge");

    // Verify: shard 1 is tombstoned, shard 0 covers [None, None).
    let record: Option<crate::discovery::ShardRecord<usize, i64>> =
        manager.remote.strong_get_shard(ShardNumber(1)).await.unwrap();
    assert_eq!(record.unwrap().status, ShardStatus::Tombstoned,
        "shard 1 should be tombstoned after merge");
    let record: Option<crate::discovery::ShardRecord<usize, i64>> =
        manager.remote.strong_get_shard(ShardNumber(0)).await.unwrap();
    let r = record.unwrap();
    assert_eq!(r.status, ShardStatus::Active);
    let range = r.key_range.unwrap();
    assert_eq!(range.start, None);
    assert_eq!(range.end, None);

    disc.shutdown().await;
    drop(_replicas_0);
    drop(_replicas_1);
}
