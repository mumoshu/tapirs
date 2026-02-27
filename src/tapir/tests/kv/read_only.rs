use super::*;

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
    let ro = clients[1].begin_read_only(Duration::ZERO);
    let val = ro
        .get(Sharded {
            shard: ShardNumber(0),
            key: 42,
        })
        .await
        .unwrap();
    assert_eq!(val, Some(100));

    // Non-existent key returns None.
    let val = ro
        .get(Sharded {
            shard: ShardNumber(0),
            key: 999,
        })
        .await
        .unwrap();
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
    let ro = clients[1].begin_read_only(Duration::ZERO);
    let v1 = ro
        .get(Sharded {
            shard: ShardNumber(0),
            key: 1,
        })
        .await
        .unwrap();
    let v2 = ro
        .get(Sharded {
            shard: ShardNumber(0),
            key: 2,
        })
        .await
        .unwrap();
    assert_eq!(v1, Some(10));
    assert_eq!(v2, Some(20));

    // Reading the same key again within the transaction returns cached value.
    let v1_again = ro
        .get(Sharded {
            shard: ShardNumber(0),
            key: 1,
        })
        .await
        .unwrap();
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
    let ro = clients[1].begin_read_only(Duration::ZERO);
    let v1 = ro
        .get(Sharded {
            shard: ShardNumber(0),
            key: 1,
        })
        .await
        .unwrap();
    let v2 = ro
        .get(Sharded {
            shard: ShardNumber(1),
            key: 2,
        })
        .await
        .unwrap();
    let v3 = ro
        .get(Sharded {
            shard: ShardNumber(2),
            key: 3,
        })
        .await
        .unwrap();
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
    let ro = clients[1].begin_read_only(Duration::ZERO);
    let results = ro
        .scan(
            Sharded { shard: ShardNumber(0), key: 10 },
            Sharded { shard: ShardNumber(0), key: 30 },
        )
        .await
        .unwrap();
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
    let ro = clients[1].begin_read_only(Duration::ZERO);
    let scan_results = ro
        .scan(
            Sharded { shard: ShardNumber(0), key: 40 },
            Sharded { shard: ShardNumber(0), key: 50 },
        )
        .await
        .unwrap();
    assert_eq!(scan_results, vec![(42, 999)]);

    let val = ro
        .get(Sharded { shard: ShardNumber(0), key: 42 })
        .await
        .unwrap();
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
    let ro = clients[1].begin_read_only(Duration::ZERO);
    let results = ro
        .scan(
            Sharded { shard: ShardNumber(0), key: 0 },
            Sharded { shard: ShardNumber(0), key: 50 },
        )
        .await
        .unwrap();
    assert!(results.is_empty());

    drop(_replicas);
}

#[tokio::test(start_paused = true)]
async fn read_only_scan_multi_shard() {
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

    let ro = routing_client.begin_read_only(Duration::ZERO);
    let results = ro.scan(10, 120).await.unwrap();
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
    let ro = clients[1].begin_read_only(Duration::ZERO);
    let results = ro
        .scan(
            Sharded { shard: ShardNumber(0), key: 0 },
            Sharded { shard: ShardNumber(0), key: 50 },
        )
        .await
        .unwrap();
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
