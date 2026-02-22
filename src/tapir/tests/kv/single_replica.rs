use super::*;

/// Verify a single-replica TAPIR cluster can serve transactions over time.
///
/// A single-replica cluster has no peers for view change quorum. This test
/// ensures the replica stays in Normal status (never hangs on ViewChanging)
/// even after multiple tick intervals elapse (tick fires every 2s by default).
#[tokio::test(start_paused = true)]
async fn single_replica_transactions() {
    timeout(Duration::from_secs(120), async {
        let (_replicas, clients) = build_kv(true, 1, 1);

        let mut committed = 0;
        for round in 0..5 {
            // Sleep past tick interval (2s) to verify tick() doesn't trigger
            // a view change that would hang the single-replica cluster.
            Transport::sleep(Duration::from_secs(3)).await;

            let txn = clients[0].begin();
            let old = txn.get(0).await.unwrap().unwrap_or_default();
            txn.put(0, Some(old + 1));
            if txn.commit().await.is_some() {
                assert_eq!(committed, old, "round {round}: stale read");
                committed += 1;
            }
        }

        eprintln!("single_replica_transactions: committed = {committed}");
        assert_eq!(committed, 5, "all transactions should commit on a single-replica cluster");
    })
    .await
    .unwrap();
}
