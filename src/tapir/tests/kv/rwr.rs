use super::*;

#[tokio::test]
async fn fuzz_rwr_3() {
    fuzz_rwr(3).await;
}

#[tokio::test]
async fn fuzz_rwr_5() {
    fuzz_rwr(5).await;
}

#[tokio::test]
async fn fuzz_rwr_7() {
    fuzz_rwr(7).await;
}

async fn fuzz_rwr(replicas: usize) {
    for _ in 0..16 {
        for linearizable in [false, true] {
            timeout(
                Duration::from_secs((replicas as u64 + 5) * 10),
                rwr(linearizable, replicas),
            )
            .await
            .unwrap();
        }
    }
}

async fn rwr(linearizable: bool, num_replicas: usize) {
    let (_replicas, clients) = build_kv(linearizable, num_replicas, 2);

    let txn = clients[0].begin();
    assert_eq!(txn.get(0).await.unwrap(), None);
    txn.put(1, Some(2));
    let first = txn.commit().await.unwrap();

    Transport::sleep(Duration::from_millis(10)).await;

    if linearizable {
        let txn = clients[1].begin();
        let result = txn.get(1).await.unwrap();
        if result.is_none() {
            // We read stale data so shouldn't be able to commit.
            assert_eq!(txn.commit().await, None, "prev = {first:?}");
        } else {
            // Up to date, should be able to commit.
            //assert!(txn.commit().await.is_some());
        }
    } else {
        let txn = clients[1].begin();
        let result = txn.get(1).await.unwrap();
        if let Some(commit) = txn.commit().await {
            if result.is_none() {
                assert!(commit < first, "{commit:?} {first:?}");
            } else {
                assert_eq!(result, Some(2));
                assert!(commit > first);
            }
        }
    }
}

#[tokio::test]
async fn sharded() {
    let (_shards, clients) = build_sharded_kv(true, 5, 3, 2);

    let txn = clients[0].begin();
    assert_eq!(
        txn.get(Sharded {
            shard: ShardNumber(0),
            key: 0
        })
        .await
        .unwrap(),
        None
    );
    assert_eq!(
        txn.get(Sharded {
            shard: ShardNumber(1),
            key: 0
        })
        .await
        .unwrap(),
        None
    );
    txn.put(
        Sharded {
            shard: ShardNumber(2),
            key: 0,
        },
        Some(0),
    );
    assert!(txn.commit().await.is_some());
}
