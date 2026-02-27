use super::*;

/// RO QuorumRead sees committed write without explicit FINALIZE sleep.
/// Under start_paused=true, advance(1ms) is for timestamp ordering
/// (snapshot_ts > commit_ts); the retry mechanism handles delayed
/// FINALIZE even without it.
#[tokio::test(start_paused = true)]
async fn quorum_read_sees_committed_write_without_finalize_sleep() {
    let (_replicas, clients) = build_kv(true, 3, 2);

    let txn = clients[0].begin();
    txn.put(
        Sharded {
            shard: ShardNumber(0),
            key: 42,
        },
        Some(100),
    );
    txn.commit().await.unwrap();

    // Advance for timestamp ordering only.
    tokio::time::advance(Duration::from_millis(1)).await;

    let ro = clients[1].begin_read_only(Duration::ZERO);
    let val = ro
        .get(Sharded {
            shard: ShardNumber(0),
            key: 42,
        })
        .await
        .unwrap();
    assert_eq!(val, Some(100));

    drop(_replicas);
}

/// RO QuorumScan sees committed write without explicit FINALIZE sleep.
#[tokio::test(start_paused = true)]
async fn quorum_scan_sees_committed_write_without_finalize_sleep() {
    let (_replicas, clients) = build_kv(true, 3, 2);

    let txn = clients[0].begin();
    txn.put(
        Sharded {
            shard: ShardNumber(0),
            key: 42,
        },
        Some(100),
    );
    txn.commit().await.unwrap();

    tokio::time::advance(Duration::from_millis(1)).await;

    let ro = clients[1].begin_read_only(Duration::ZERO);
    let results = ro
        .scan(
            Sharded {
                shard: ShardNumber(0),
                key: 10,
            },
            Sharded {
                shard: ShardNumber(0),
                key: 50,
            },
        )
        .await
        .unwrap();
    assert_eq!(results, vec![(42, 100)]);

    drop(_replicas);
}
