use super::*;

use crate::tapir::ShardClient;
use crate::IrClientId;

#[tokio::test(start_paused = true)]
async fn cdc_view_based_scan_changes() {
    init_tracing();

    let mut rng = test_rng(42);
    let shard = ShardNumber(0);
    let registry = ChannelRegistry::default();
    let dir = Arc::new(InMemoryShardDirectory::new());

    // Build 3-replica shard.
    let replicas = build_shard(&mut rng, shard, false, 3, &registry, &dir);
    let clients = build_clients(&mut rng, 1, &registry, &dir);

    // Commit two transactions.
    let txn = clients[0].begin();
    txn.put(1_i64, Some(100_i64));
    let ts1 = txn.commit().await;
    assert!(ts1.is_some(), "first commit should succeed");

    Transport::sleep(Duration::from_millis(1)).await;

    let txn = clients[0].begin();
    txn.put(2_i64, Some(200_i64));
    let ts2 = txn.commit().await;
    assert!(ts2.is_some(), "second commit should succeed");

    // Force a view change so the leader record delta captures the commits.
    replicas[0].force_view_change();
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Create a ShardClient to call scan_changes.
    let membership = IrMembership::new(vec![0, 1, 2]);
    let channel = registry.channel(move |_, _| unreachable!(), Arc::clone(&dir));
    let shard_client: ShardClient<K, V, Transport> =
        ShardClient::new(rng.fork(), IrClientId::new(&mut rng), shard, membership, channel);

    // First scan: from_view=0 should return all committed changes.
    let r = shard_client.scan_changes(0).await;
    assert!(
        !r.deltas.is_empty(),
        "should have at least one delta after view change"
    );
    let all_changes: Vec<_> = r.deltas.iter().flat_map(|d| &d.changes).collect();
    assert!(
        all_changes.len() >= 2,
        "should have at least 2 changes, got {}",
        all_changes.len()
    );
    // Verify both keys are present.
    assert!(
        all_changes.iter().any(|c| c.key == 1 && c.value == Some(100)),
        "should contain key=1 value=100"
    );
    assert!(
        all_changes.iter().any(|c| c.key == 2 && c.value == Some(200)),
        "should contain key=2 value=200"
    );
    // Verify each delta has valid from_view/to_view.
    for delta in &r.deltas {
        assert!(
            delta.to_view > delta.from_view,
            "to_view ({}) should be > from_view ({})",
            delta.to_view,
            delta.from_view
        );
    }
    // effective_end_view is Some(max base_view key); for the first view change
    // base_view=0, so effective_end_view=Some(0).
    let first_delta = &r.deltas[0];
    assert_eq!(first_delta.from_view, 0, "first delta should be from view 0");
    assert!(r.effective_end_view.is_some(), "should have Some effective_end_view after view change");

    let last_view = r.effective_end_view.unwrap();

    // Commit a third transaction.
    Transport::sleep(Duration::from_millis(1)).await;
    let txn = clients[0].begin();
    txn.put(3_i64, Some(300_i64));
    let ts3 = txn.commit().await;
    assert!(ts3.is_some(), "third commit should succeed");

    // Force another view change.
    replicas[0].force_view_change();
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Second scan: from_view=last_view+1 should return only the new changes.
    let r2 = shard_client.scan_changes(last_view + 1).await;
    assert!(
        !r2.deltas.is_empty(),
        "should have new deltas after second view change"
    );
    let new_changes: Vec<_> = r2.deltas.iter().flat_map(|d| &d.changes).collect();
    assert!(
        new_changes.iter().any(|c| c.key == 3 && c.value == Some(300)),
        "should contain key=3 value=300"
    );
    // Should NOT contain the earlier keys (those are in earlier views).
    assert!(
        !new_changes.iter().any(|c| c.key == 1),
        "should not contain key=1 in incremental scan"
    );
    assert!(
        r2.effective_end_view.unwrap() > last_view,
        "effective_end_view should advance"
    );

    drop(replicas);
}
