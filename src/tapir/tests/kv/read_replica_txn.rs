use super::*;

#[tokio::test(start_paused = true)]
async fn read_replica_txn_basic() {
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

    // Advance time so the read-replica snapshot is strictly after the commit.
    tokio::time::advance(Duration::from_millis(1)).await;

    // Read it back via read-replica transaction.
    // This uses invoke_unlogged(GetAt) — one replica, one round trip.
    let rr = clients[1].begin_read_replica();
    let val = rr
        .get(Sharded {
            shard: ShardNumber(0),
            key: 42,
        })
        .await
        .unwrap();
    assert_eq!(val, Some(100));

    // Non-existent key returns None.
    let val = rr
        .get(Sharded {
            shard: ShardNumber(0),
            key: 999,
        })
        .await
        .unwrap();
    assert_eq!(val, None);

    // Read cache: second get of same key returns cached value.
    let val = rr
        .get(Sharded {
            shard: ShardNumber(0),
            key: 42,
        })
        .await
        .unwrap();
    assert_eq!(val, Some(100));

    drop(_replicas);
}

#[tokio::test(start_paused = true)]
async fn read_replica_txn_scan() {
    let (_replicas, clients) = build_kv(true, 3, 2);

    // Write multiple keys.
    let txn = clients[0].begin();
    for i in 10..15 {
        txn.put(
            Sharded {
                shard: ShardNumber(0),
                key: i,
            },
            Some(i * 100),
        );
    }
    assert!(txn.commit().await.is_some());

    tokio::time::advance(Duration::from_millis(1)).await;

    // Scan via read-replica transaction.
    let rr = clients[1].begin_read_replica();
    let results = rr
        .scan(
            Sharded {
                shard: ShardNumber(0),
                key: 10,
            },
            Sharded {
                shard: ShardNumber(0),
                key: 15,
            },
        )
        .await
        .unwrap();

    assert_eq!(results.len(), 5);
    for (i, (key, value)) in results.iter().enumerate() {
        assert_eq!(*key, 10 + i as i64);
        assert_eq!(*value, (10 + i as i64) * 100);
    }

    drop(_replicas);
}

#[tokio::test(start_paused = true)]
async fn read_replica_txn_snapshot_isolation() {
    let (_replicas, clients) = build_kv(true, 3, 2);

    // Write initial value.
    let txn = clients[0].begin();
    txn.put(
        Sharded {
            shard: ShardNumber(0),
            key: 1,
        },
        Some(100),
    );
    assert!(txn.commit().await.is_some());

    tokio::time::advance(Duration::from_millis(1)).await;

    // Start read-replica transaction — snapshot captures ts after first write.
    let rr = clients[1].begin_read_replica();

    // Read key=1 — sees initial value.
    let val = rr
        .get(Sharded {
            shard: ShardNumber(0),
            key: 1,
        })
        .await
        .unwrap();
    assert_eq!(val, Some(100));

    // Write a new value AFTER the snapshot.
    tokio::time::advance(Duration::from_millis(1)).await;
    let txn2 = clients[0].begin();
    txn2.put(
        Sharded {
            shard: ShardNumber(0),
            key: 1,
        },
        Some(200),
    );
    assert!(txn2.commit().await.is_some());

    // Read the same key again in the SAME read-replica transaction.
    // Should still see 100 (cached from first read), not 200.
    let val = rr
        .get(Sharded {
            shard: ShardNumber(0),
            key: 1,
        })
        .await
        .unwrap();
    assert_eq!(val, Some(100), "read cache should provide SI within the transaction");

    drop(_replicas);
}
