use super::*;

#[tokio::test(start_paused = true)]
async fn time_travel_basic_get() {
    let (_replicas, clients) = build_kv(true, 3, 2);

    let txn = clients[0].begin();
    txn.put(Sharded { shard: ShardNumber(0), key: 42 }, Some(100));
    let commit_ts = txn.commit().await.unwrap();

    // Let FINALIZE propagate so the write is applied to MVCC.
    tokio::time::advance(Duration::from_millis(1)).await;

    let tt = clients[1].begin_time_travel(commit_ts);
    let val = tt.get(Sharded { shard: ShardNumber(0), key: 42 }).await.unwrap();
    assert_eq!(val, Some(100));

    let val = tt.get(Sharded { shard: ShardNumber(0), key: 999 }).await.unwrap();
    assert_eq!(val, None);

    drop(_replicas);
}

#[tokio::test(start_paused = true)]
async fn time_travel_does_not_see_future_writes() {
    let (_replicas, clients) = build_kv(true, 3, 2);

    let early_ts = Timestamp::default(); // time=0, client_id=0

    tokio::time::advance(Duration::from_millis(1)).await;

    let txn = clients[0].begin();
    txn.put(Sharded { shard: ShardNumber(0), key: 42 }, Some(100));
    assert!(txn.commit().await.is_some());

    // Let FINALIZE propagate so the write is applied to MVCC.
    tokio::time::advance(Duration::from_millis(1)).await;

    let tt = clients[1].begin_time_travel(early_ts);
    let val = tt.get(Sharded { shard: ShardNumber(0), key: 42 }).await.unwrap();
    assert_eq!(val, None);

    drop(_replicas);
}

#[tokio::test(start_paused = true)]
async fn time_travel_sees_past_through_overwrites() {
    let (_replicas, clients) = build_kv(true, 3, 2);

    let txn = clients[0].begin();
    txn.put(Sharded { shard: ShardNumber(0), key: 42 }, Some(100));
    let ts1 = txn.commit().await.unwrap();

    // Let FINALIZE propagate so the first write is applied to MVCC.
    tokio::time::advance(Duration::from_millis(1)).await;

    let txn = clients[0].begin();
    txn.put(Sharded { shard: ShardNumber(0), key: 42 }, Some(200));
    let ts2 = txn.commit().await.unwrap();

    // Let FINALIZE propagate so the second write is applied to MVCC.
    tokio::time::advance(Duration::from_millis(1)).await;

    let tt1 = clients[1].begin_time_travel(ts1);
    assert_eq!(
        tt1.get(Sharded { shard: ShardNumber(0), key: 42 }).await.unwrap(),
        Some(100)
    );

    let tt2 = clients[1].begin_time_travel(ts2);
    assert_eq!(
        tt2.get(Sharded { shard: ShardNumber(0), key: 42 }).await.unwrap(),
        Some(200)
    );

    drop(_replicas);
}

#[tokio::test(start_paused = true)]
async fn time_travel_scan_basic() {
    let (_replicas, clients) = build_kv(true, 3, 2);

    let txn = clients[0].begin();
    txn.put(Sharded { shard: ShardNumber(0), key: 10 }, Some(100));
    txn.put(Sharded { shard: ShardNumber(0), key: 20 }, Some(200));
    txn.put(Sharded { shard: ShardNumber(0), key: 30 }, Some(300));
    let commit_ts = txn.commit().await.unwrap();

    // Let FINALIZE propagate so the writes are applied to MVCC.
    tokio::time::advance(Duration::from_millis(1)).await;

    let tt = clients[1].begin_time_travel(commit_ts);
    let results = tt
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
async fn time_travel_routing_multi_shard_scan() {
    let (_shards, clients) = build_sharded_kv(true, 2, 3, 2);

    let txn = clients[0].begin();
    txn.put(Sharded { shard: ShardNumber(0), key: 10 }, Some(100));
    txn.put(Sharded { shard: ShardNumber(1), key: 110 }, Some(1100));
    let commit_ts = txn.commit().await.unwrap();

    // Let FINALIZE propagate so the writes are applied to MVCC.
    tokio::time::advance(Duration::from_millis(1)).await;

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

    let tt = routing_client.begin_time_travel(commit_ts);

    let val = tt.get(10).await.unwrap();
    assert_eq!(val, Some(100));

    let results = tt.scan(10, 110).await.unwrap();
    assert_eq!(results, vec![(10, 100), (110, 1100)]);

    drop(_shards);
}
