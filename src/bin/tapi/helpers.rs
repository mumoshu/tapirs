use crate::node::Node;
use std::collections::BTreeMap;
use std::sync::Arc;
use tapirs::{
    DynamicRouter, RoutingClient, ShardNumber, TapirReplica, TcpTransport,
};
use tokio::time::Duration;

type K = String;
type V = String;
type TestTransport = TcpTransport<TapirReplica<K, V>>;

/// Collect the current view number agreed upon by a quorum (f+1) of replicas
/// for the given shard. Each node is expected to host one replica for the shard.
///
/// With f = (n-1)/2 (e.g., f=1 for n=3 or n=4), this requires at least 2
/// matching view numbers. Panics if no quorum agrees — this indicates an
/// unexpected state (e.g., replicas still mid-view-change).
pub fn quorum_view_number(nodes: &[&Arc<Node>], shard: ShardNumber) -> u64 {
    let views: Vec<u64> = nodes
        .iter()
        .filter_map(|n| n.shard_view_number(shard))
        .collect();
    let n = views.len();
    assert!(n > 0, "no replicas found for {shard:?}");
    let f = (n - 1) / 2;
    let quorum = f + 1;

    let mut counts: BTreeMap<u64, usize> = BTreeMap::new();
    for &v in &views {
        *counts.entry(v).or_default() += 1;
    }
    for (&view, &count) in &counts {
        if count >= quorum {
            return view;
        }
    }
    panic!("no quorum agreement on view number for {shard:?}: views={views:?}");
}

/// Wait for a view change to complete across the replicas on the given nodes.
///
/// Phase 1: Poll `shard_view_number()` until f+1 replicas report view > `old_view`.
///          This confirms a quorum received DoViewChange and entered ViewChanging,
///          closing the race window between the fire-and-forget `add_member`/
///          `remove_member` send and replicas actually starting the view change.
///
/// Phase 2: Probe with a `put + commit` transaction until it succeeds. TAPIR rejects
///          proposals during ViewChanging, so a successful commit proves a quorum of
///          replicas returned to Normal status.
pub async fn wait_for_view_change(
    client: &Arc<RoutingClient<K, V, TestTransport, DynamicRouter<K>>>,
    nodes: &[&Arc<Node>],
    shard: ShardNumber,
    old_view: u64,
    probe_key: &str,
) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);

    let n = nodes.iter().filter(|n| n.shard_view_number(shard).is_some()).count();
    let f = if n > 0 { (n - 1) / 2 } else { 0 };
    let quorum = f + 1;

    // Phase 1: f+1 replicas advanced past the pre-operation view.
    loop {
        let advanced = nodes
            .iter()
            .filter(|n| n.shard_view_number(shard).is_some_and(|v| v > old_view))
            .count();
        if advanced >= quorum {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timeout waiting for f+1 replicas to advance past view {old_view} on {shard:?}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Phase 2: transaction succeeds → quorum of replicas in Normal.
    loop {
        let txn = client.begin();
        txn.put(probe_key.to_string(), Some("probe".to_string()));
        if txn.commit().await.is_some() {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timeout waiting for shard to accept writes (probe key: {probe_key})"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Wait for a shard to become operational (accepting writes).
///
/// Unlike `wait_for_view_change`, this does NOT wait for view number
/// advancement. It only probes with a `put + commit` transaction until
/// one succeeds, proving that a quorum of replicas are in Normal status
/// and processing proposals.
///
/// Use this after operations that don't trigger a view change
/// (e.g., `remove_replica`, post-restore settlement).
pub async fn wait_for_operational(
    client: &Arc<RoutingClient<K, V, TestTransport, DynamicRouter<K>>>,
    probe_key: &str,
) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);

    loop {
        let txn = client.begin();
        txn.put(probe_key.to_string(), Some("probe".to_string()));
        if txn.commit().await.is_some() {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timeout waiting for shard to accept writes (probe key: {probe_key})"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
