use crate::config::ReplicaConfig;
use crate::discovery_backend::DiscoveryBackend;
use crate::helpers::{quorum_view_number, wait_for_operational, wait_for_view_change};
use crate::node::Node;
use rand::{thread_rng, Rng as _};
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use tapirs::discovery::{tapir, InMemoryShardDirectory, RemoteShardDirectory};
use tapirs::sharding::shardmanager_client::HttpShardManagerClient;
use tapirs::{
    DynamicRouter, KeyRange, RoutingClient, ShardDirectory, ShardEntry, ShardNumber, TapirClient,
    TapirReplica, TcpAddress, TcpTransport,
};
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::time::Duration;

type DiscTapirDir =
    tapir::TapirRemoteShardDirectory<TcpAddress, TcpTransport<TapirReplica<String, String>>>;

/// Pin K=String so method calls are unambiguous (TapirRemoteShardDirectory
/// implements RemoteShardDirectory for all K). String matches what
/// strong_atomic_update_shards stores in key_range fields.
fn disc(c: DiscTapirDir) -> impl RemoteShardDirectory<TcpAddress, String> {
    c
}

fn alloc_addr() -> SocketAddr {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let a = l.local_addr().unwrap();
    drop(l);
    a
}

/// Allocate a TCP port and keep the listener alive to prevent TOCTOU races.
///
/// The returned listener must be passed to `Node::add_replica_with_listener`
/// (or similar) so the transport can adopt it without re-binding.
fn alloc_listener() -> (SocketAddr, std::net::TcpListener) {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let a = l.local_addr().unwrap();
    (a, l)
}

fn env_or(var: &str, default: u32) -> u32 {
    std::env::var(var)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// Create a standalone TapirRemoteShardDirectory client for direct discovery access.
async fn create_disc_remote(endpoint: &str) -> DiscTapirDir {
    let addr = TcpAddress(alloc_addr());
    let dir = Arc::new(InMemoryShardDirectory::new());
    let persist_dir = format!("/tmp/tapi_test_disc_{}", thread_rng().r#gen::<u32>());
    let transport = TcpTransport::with_directory(addr, persist_dir, dir);
    let rng = tapirs::Rng::from_seed(thread_rng().r#gen());
    tapir::parse_tapir_endpoint::<TcpAddress, _>(endpoint, transport, rng)
        .await
        .expect("create discovery client")
}

/// Create a DiscoveryBackend::Tapir from an endpoint string.
async fn create_disc_backend(endpoint: &str) -> DiscoveryBackend {
    DiscoveryBackend::Tapir(create_disc_remote(endpoint).await)
}

struct TestCluster {
    disc_endpoint: String,
    shard_manager_addr: SocketAddr,
    nodes: Vec<Arc<Node>>,
    replica_addrs: Vec<Vec<SocketAddr>>, // [shard][replica_idx]
    client: Arc<RoutingClient<K, V, TestTransport, DynamicRouter<K>>>,
    _temp_dirs: Vec<TempDir>,
    _client_temp_dir: TempDir,
    _discovery_dir: Arc<tapirs::discovery::CachingShardDirectory<TcpAddress, String, DiscoveryBackend>>,
    _disc_nodes: Vec<Arc<Node>>,
    _disc_temp_dirs: Vec<TempDir>,
}

type K = String;
type V = String;
type TestTransport = TcpTransport<TapirReplica<K, V>>;

/// Mirrors the operator workflow:
///   tapi discovery-tier -> tapi shard-manager -> tapi node -> tapi admin add-replica
async fn bootstrap_cluster(
    num_shards: u32,
    replicas_per_shard: u32,
    num_nodes: u32,
) -> TestCluster {
    // === Discovery tier: 3-node Tapir cluster for shard metadata ===
    let mut disc_nodes = Vec::new();
    let mut disc_temp_dirs = Vec::new();

    // Keep listeners alive to prevent TOCTOU port races.
    let mut disc_listeners = Vec::new();
    for _ in 0..3 {
        disc_listeners.push(alloc_listener());
    }
    let disc_addrs: Vec<SocketAddr> = disc_listeners.iter().map(|(a, _)| *a).collect();
    let disc_membership: Vec<String> = disc_addrs.iter().map(|a| a.to_string()).collect();
    let disc_endpoint = disc_membership.join(",");

    for (disc_addr, listener) in disc_listeners {
        let td = TempDir::new().unwrap();
        let node = Arc::new(Node::new(td.path().to_str().unwrap().to_string()));
        node.add_replica_with_listener(
            &ReplicaConfig {
                shard: 0,
                listen_addr: disc_addr.to_string(),
                membership: disc_membership.clone(),
            },
            listener,
        )
        .await
        .unwrap();
        disc_nodes.push(node);
        disc_temp_dirs.push(td);
    }

    // Trigger initial view change so the discovery cluster enters Normal mode.
    disc_nodes[0].force_view_change(ShardNumber(0));
    tokio::time::sleep(Duration::from_secs(3)).await;

    // === Shard manager ===
    let mgr_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mgr_addr = mgr_listener.local_addr().unwrap();
    let sm_backend = create_disc_backend(&disc_endpoint).await;
    tokio::spawn(tapirs::sharding::shardmanager_server::serve(
        mgr_listener,
        tapirs::Rng::from_seed(thread_rng().r#gen()),
        Arc::new(sm_backend),
        #[cfg(feature = "tls")]
        None,
        #[cfg(feature = "tls")]
        None,
    ));

    // === Data nodes (one per num_nodes) ===
    let shard_manager_url = format!("http://{mgr_addr}");
    let mut nodes = Vec::new();
    let mut temp_dirs = Vec::new();
    for _ in 0..num_nodes {
        let td = TempDir::new().unwrap();
        let backend = create_disc_backend(&disc_endpoint).await;
        let node = Arc::new(Node::with_discovery_backend_and_shard_manager(
            td.path().to_str().unwrap().to_string(),
            backend,
            &shard_manager_url,
        ));
        nodes.push(node);
        temp_dirs.push(td);
    }

    // === Static membership bootstrap ===
    // Pre-allocate all replica addresses, then start all replicas with
    // the full membership. TAPIR is leaderless — replicas sharing the
    // same view number and membership form an operational shard at view 0
    // without any view change ceremony. Matches the operator workflow:
    //   `tapi admin add-replica --membership addr1,addr2,addr3`
    // Keep listeners alive to prevent TOCTOU port races.
    let mut replica_listeners: Vec<Vec<(SocketAddr, std::net::TcpListener)>> =
        (0..num_shards).map(|_| Vec::new()).collect();
    for shard_idx in 0..num_shards {
        for _ in 0..replicas_per_shard {
            replica_listeners[shard_idx as usize].push(alloc_listener());
        }
    }
    let replica_addrs: Vec<Vec<SocketAddr>> = replica_listeners
        .iter()
        .map(|shard| shard.iter().map(|(a, _)| *a).collect())
        .collect();

    let shard_entries = build_shard_entries(num_shards);
    for shard_idx in 0..num_shards {
        let membership: Vec<String> = replica_addrs[shard_idx as usize]
            .iter()
            .map(|a| a.to_string())
            .collect();
        let shard_listeners =
            std::mem::take(&mut replica_listeners[shard_idx as usize]);
        for (replica_idx, (addr, listener)) in shard_listeners.into_iter().enumerate() {
            let node_idx = replica_idx % num_nodes as usize;
            let cfg = ReplicaConfig {
                shard: shard_idx,
                listen_addr: addr.to_string(),
                membership: membership.clone(),
            };
            nodes[node_idx]
                .add_replica_with_listener(&cfg, listener)
                .await
                .unwrap_or_else(|e| {
                    panic!("add_replica shard {shard_idx} replica {replica_idx}: {e}")
                });
        }

        // Register shard via shard-manager HTTP API (matches operator workflow).
        let entry = &shard_entries[shard_idx as usize];
        HttpShardManagerClient::new(&shard_manager_url)
            .register(
                shard_idx,
                entry.range.start.as_deref(),
                entry.range.end.as_deref(),
                Some(&membership),
            )
            .unwrap();
    }

    // === Create client (after all replicas registered in discovery) ===
    let (client, client_temp_dir, discovery_dir) =
        create_test_client(&disc_endpoint, num_shards).await;

    TestCluster {
        disc_endpoint,
        shard_manager_addr: mgr_addr,
        nodes,
        replica_addrs,
        client,
        _temp_dirs: temp_dirs,
        _client_temp_dir: client_temp_dir,
        _discovery_dir: discovery_dir,
        _disc_nodes: disc_nodes,
        _disc_temp_dirs: disc_temp_dirs,
    }
}

/// Create client — same setup as tapi client (src/bin/tapi/client.rs).
/// Uses CachingShardDirectory for address resolution, NOT manual
/// transport.set_shard_addresses().
async fn create_test_client(
    disc_endpoint: &str,
    num_shards: u32,
) -> (
    Arc<RoutingClient<K, V, TestTransport, DynamicRouter<K>>>,
    TempDir,
    Arc<tapirs::discovery::CachingShardDirectory<TcpAddress, String, DiscoveryBackend>>,
) {
    let local_addr = alloc_addr();
    let td = TempDir::new().unwrap();
    let dir = Arc::new(InMemoryShardDirectory::new());

    // CachingShardDirectory auto-syncs shard->membership from discovery, populating dir.
    let backend = create_disc_backend(disc_endpoint).await;
    let disc_client = Arc::new(backend);
    let discovery_dir = tapirs::discovery::CachingShardDirectory::<TcpAddress, String, _>::new(
        Arc::clone(&dir),
        disc_client,
        std::time::Duration::from_millis(50), // fast sync for tests
    );

    // Wait for initial sync to populate dir from discovery.
    tokio::time::sleep(Duration::from_millis(60)).await;

    // Same as client.rs: TcpTransport with shared directory.
    let transport: TestTransport = TcpTransport::with_directory(
        TcpAddress(local_addr),
        td.path().to_str().unwrap().to_string(),
        Arc::clone(&dir),
    );

    // Key routing — discovery doesn't store key ranges, so this is
    // still needed (same as client.rs).
    let entries = build_shard_entries(num_shards);
    let directory = Arc::new(RwLock::new(ShardDirectory::new(entries)));
    let router = Arc::new(DynamicRouter::new(directory));
    let tapir_client = Arc::new(TapirClient::new(tapirs::Rng::from_seed(thread_rng().r#gen()), transport));
    let rc = Arc::new(RoutingClient::new(tapir_client, router));

    // Must return discovery_dir to keep background sync alive.
    (rc, td, discovery_dir)
}

/// Partition key space (a-z) evenly across shards.
fn build_shard_entries(n: u32) -> Vec<ShardEntry<K>> {
    if n == 0 {
        return vec![];
    }
    if n == 1 {
        return vec![ShardEntry {
            shard: ShardNumber(0),
            range: KeyRange {
                start: None,
                end: None,
            },
        }];
    }

    let chars: Vec<char> = ('a'..='z').collect();
    let per = chars.len() / n as usize;
    let mut entries = Vec::new();
    for i in 0..n {
        let start = if i == 0 {
            None
        } else {
            Some(chars[i as usize * per].to_string())
        };
        let end = if i == n - 1 {
            None
        } else {
            Some(chars[(i as usize + 1) * per].to_string())
        };
        entries.push(ShardEntry {
            shard: ShardNumber(i),
            range: KeyRange { start, end },
        });
    }
    entries
}

/// Read a key via a read-write transaction with OCC validation.
///
/// invoke_unlogged reads from a single replica (not a quorum). A read-only
/// transaction cannot detect stale reads. By writing the read value to
/// `dest_key` (a rw dependency), OCC prepare validates the read timestamp
/// across a quorum. If the read was stale, the transaction aborts and we
/// retry — same pattern a real application would use.
async fn rw_get(
    client: &Arc<RoutingClient<K, V, TestTransport, DynamicRouter<K>>>,
    key: &str,
    dest_key: &str,
) -> Option<V> {
    for attempt in 0..10 {
        let txn = client.begin();
        let val = txn.get(key.to_string()).await.unwrap();
        txn.put(dest_key.to_string(), val.clone());
        if txn.commit().await.is_some() {
            return val;
        }
        // OCC abort — read was stale (Decide not yet propagated), retry.
        tracing::info!("rw_get({key}): OCC abort on attempt {attempt}, retrying");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("rw_get({key}): failed after 10 retries");
}

// ---- Tests ----

#[tokio::test(flavor = "multi_thread")]
async fn test_cluster_bootstrap() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

    let num_replicas = env_or("TAPI_TEST_REPLICAS", 3);
    let num_nodes = env_or("TAPI_TEST_NODES", 3);
    let cluster = bootstrap_cluster(
        env_or("TAPI_TEST_SHARDS", 2),
        num_replicas,
        num_nodes,
    )
    .await;

    for shard_addrs in &cluster.replica_addrs {
        assert_eq!(shard_addrs.len(), num_replicas as usize);
    }
    // Same as `tapi admin status`.
    for node in &cluster.nodes {
        assert!(!node.shard_list().is_empty());
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_read_write_transactions() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

    let cluster = bootstrap_cluster(2, 3, 3).await;
    let client = &cluster.client;

    // PUT — same as tapi client REPL.
    let txn = client.begin();
    txn.put("key1".to_string(), Some("value1".to_string()));
    assert!(txn.commit().await.is_some(), "initial put should commit");

    // Read-write transaction: read key1 then copy to key1_read.
    // invoke_unlogged reads from a single replica, so the read may be
    // stale if Decide hasn't propagated yet. By including a write that
    // depends on the read (rw dependency), OCC prepare validates the
    // read timestamp across a quorum. If stale, the txn aborts and we
    // retry — same pattern a real application would use.
    let val = rw_get(client, "key1", "key1_read").await;
    assert_eq!(val, Some("value1".to_string()));

    // Overwrite + verify via rw dependency.
    let txn = client.begin();
    txn.put("key1".to_string(), Some("value2".to_string()));
    assert!(txn.commit().await.is_some(), "overwrite should commit");

    let val = rw_get(client, "key1", "key1_read2").await;
    assert_eq!(val, Some("value2".to_string()));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_view_change_during_transactions() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

    let cluster = bootstrap_cluster(2, 3, 3).await;
    let client = &cluster.client;

    let txn = client.begin();
    txn.put("survive".to_string(), Some("yes".to_string()));
    assert!(txn.commit().await.is_some());

    // Same as `tapi admin view-change --shard 0`.
    let shard = ShardNumber(0);
    let shard_nodes: Vec<&Arc<Node>> = cluster.nodes.iter()
        .filter(|n| n.shard_view_number(shard).is_some())
        .collect();
    let old_view = quorum_view_number(&shard_nodes, shard);
    cluster.nodes[0].force_view_change(shard);
    wait_for_view_change(client, &shard_nodes, shard, old_view, "vc_probe").await;

    let val = rw_get(client, "survive", "survive_read").await;
    assert_eq!(val, Some("yes".to_string()));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_remove_replica() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

    let cluster = bootstrap_cluster(1, 3, 3).await;
    let client = &cluster.client;

    let txn = client.begin();
    txn.put("before".to_string(), Some("ok".to_string()));
    assert!(txn.commit().await.is_some());

    // Same as `tapi admin remove-replica --shard 0`.
    assert!(cluster.nodes[0].remove_replica(ShardNumber(0)));
    wait_for_operational(client, "remove_probe").await;

    // Data should still be readable from remaining 2 replicas.
    let val = rw_get(client, "before", "before_read").await;
    assert_eq!(val, Some("ok".to_string()));
}

/// Verify a shard survives complete replica replacement.
///
/// Continuously adds new replicas then removes original ones via the
/// leave API (RemoveMember → view change), until no bootstrapped
/// replicas remain. Verifies R/W availability before and after each add.
///
/// With the leave API keeping IR membership clean, quorum stays bounded:
///
///   Step           IR membership  n  f  quorum(f+1)  alive
///   Start          {A,B,C}        3  1  2            3
///   +D (add)       {A,B,C,D}      4  1  2            4
///   -A (leave)     {B,C,D}        3  1  2            3
///   +E (add)       {B,C,D,E}      4  1  2            4
///   -B (leave)     {C,D,E}        3  1  2            3
///   +F (add)       {C,D,E,F}      4  1  2            4
///   -C (leave)     {D,E,F}        3  1  2            3
///
/// n stays bounded at 3-4, f=1, quorum=2 throughout. Always 1+ replica
/// of headroom above quorum, unlike raw remove_replica which would grow
/// IR membership to 6 with f=2, quorum=3 and zero headroom.
#[tokio::test(flavor = "multi_thread")]
async fn test_rolling_membership_replacement() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

    let cluster = bootstrap_cluster(1, 3, 3).await;
    let client = &cluster.client;
    let disc_client = disc(create_disc_remote(&cluster.disc_endpoint).await);
    let shard = ShardNumber(0);

    // Write initial data and verify R/W.
    let txn = client.begin();
    txn.put("data".to_string(), Some("initial".to_string()));
    assert!(txn.commit().await.is_some(), "initial put should commit");

    let val = rw_get(client, "data", "data_check").await;
    assert_eq!(val, Some("initial".to_string()));

    let original_addrs = cluster.replica_addrs[0].clone();
    let mut live_addrs: Vec<SocketAddr> = original_addrs.clone();

    let shard_manager_url = format!("http://{}", cluster.shard_manager_addr);

    // Force a view change to establish leader_record. Static-membership
    // bootstrap starts at view 0 without a leader_record, but the
    // shard-manager's join() path (used below for runtime AddMember)
    // needs to fetch_leader_record from existing replicas.
    let shard_nodes: Vec<&Arc<Node>> = cluster
        .nodes
        .iter()
        .filter(|n| n.shard_view_number(shard).is_some())
        .collect();
    let old_view = quorum_view_number(&shard_nodes, shard);
    cluster.nodes[0].force_view_change(shard);
    wait_for_view_change(client, &shard_nodes, shard, old_view, "rolling_vc_setup").await;

    // Update discovery with the actual view after the view change.
    let new_view = quorum_view_number(&shard_nodes, shard);
    let addrs: Vec<TcpAddress> = live_addrs.iter().map(|a| TcpAddress(*a)).collect();
    disc_client
        .strong_put_active_shard_view_membership(ShardNumber(0), tapirs::IrMembership::new(addrs), new_view)
        .await
        .unwrap();

    // Keep new nodes and temp dirs alive for the duration of the test.
    let mut new_nodes: Vec<Arc<Node>> = Vec::new();
    let mut _new_temp_dirs: Vec<TempDir> = Vec::new();

    // Rolling replacement: for each original replica, add a new one then
    // leave+remove the old one.
    for (i, original_addr) in original_addrs.iter().enumerate().take(3) {
        tracing::info!("--- round {i}: adding new replica ---");

        // === ADD new replica ===
        let td = TempDir::new().unwrap();
        let backend = create_disc_backend(&cluster.disc_endpoint).await;
        let new_node = Arc::new(Node::with_discovery_backend_and_shard_manager(
            td.path().to_str().unwrap().to_string(),
            backend,
            &shard_manager_url,
        ));

        // Record view number before AddMember.
        let all_nodes: Vec<&Arc<Node>> = cluster
            .nodes
            .iter()
            .chain(new_nodes.iter())
            .filter(|n| n.shard_view_number(shard).is_some())
            .collect();
        let old_view = quorum_view_number(&all_nodes, shard);

        let new_addr = loop {
            let candidate = alloc_addr();
            match new_node.add_replica_join(shard, candidate, "memory").await {
                Ok(()) => break candidate,
                Err(e) if e.contains("already in use") => continue,
                Err(e) => panic!("add_replica_join failed in round {i}: {e}"),
            }
        };

        live_addrs.push(new_addr);

        // Wait for AddMember view change — includes new node.
        new_nodes.push(new_node);
        _new_temp_dirs.push(td);

        let all_nodes: Vec<&Arc<Node>> = cluster
            .nodes
            .iter()
            .chain(new_nodes.iter())
            .filter(|n| n.shard_view_number(shard).is_some())
            .collect();
        wait_for_view_change(client, &all_nodes, shard, old_view, &format!("probe_add_{i}"))
            .await;

        // Update discovery with actual IR view after the view change.
        let new_view = quorum_view_number(&all_nodes, shard);
        let addrs: Vec<TcpAddress> = live_addrs.iter().map(|a| TcpAddress(*a)).collect();
        disc_client
            .strong_put_active_shard_view_membership(ShardNumber(0), tapirs::IrMembership::new(addrs), new_view)
            .await
            .unwrap();

        // Verify R/W after add.
        let txn = client.begin();
        txn.put(
            format!("after_add_{i}"),
            Some(format!("added_{i}")),
        );
        assert!(
            txn.commit().await.is_some(),
            "put after add round {i} should commit"
        );
        let val = rw_get(client, "data", &format!("data_after_add_{i}")).await;
        assert_eq!(val, Some("initial".to_string()));

        // === LEAVE + REMOVE original replica ===
        tracing::info!("--- round {i}: leaving original replica ---");

        // Record view number before RemoveMember.
        let all_nodes: Vec<&Arc<Node>> = cluster
            .nodes
            .iter()
            .chain(new_nodes.iter())
            .filter(|n| n.shard_view_number(shard).is_some())
            .collect();
        let old_view = quorum_view_number(&all_nodes, shard);

        cluster.nodes[i]
            .leave_shard(shard)
            .await
            .unwrap_or_else(|e| panic!("leave_shard failed in round {i}: {e}"));

        // Wait for RemoveMember view change.
        let remaining_nodes: Vec<&Arc<Node>> = cluster
            .nodes
            .iter()
            .chain(new_nodes.iter())
            .filter(|n| n.shard_view_number(shard).is_some())
            .collect();
        wait_for_view_change(
            client,
            &remaining_nodes,
            shard,
            old_view,
            &format!("probe_remove_{i}"),
        )
        .await;

        // Update discovery with actual IR view after the view change.
        let new_view = quorum_view_number(&remaining_nodes, shard);
        live_addrs.retain(|a| *a != *original_addr);
        let addrs: Vec<TcpAddress> = live_addrs.iter().map(|a| TcpAddress(*a)).collect();
        disc_client
            .strong_put_active_shard_view_membership(ShardNumber(0), tapirs::IrMembership::new(addrs), new_view)
            .await
            .unwrap();

        // Drop the local handle.
        assert!(
            cluster.nodes[i].remove_replica(shard),
            "remove_replica should succeed in round {i}"
        );

        // Verify R/W after remove.
        let txn = client.begin();
        txn.put(
            format!("after_remove_{i}"),
            Some(format!("removed_{i}")),
        );
        assert!(
            txn.commit().await.is_some(),
            "put after remove round {i} should commit"
        );
        let val = rw_get(client, "data", &format!("data_after_remove_{i}")).await;
        assert_eq!(val, Some("initial".to_string()));
    }

    // === Final assertions ===

    // No original nodes should have shard 0.
    for (i, node) in cluster.nodes.iter().enumerate() {
        let shards = node.shard_list();
        assert!(
            !shards.iter().any(|(s, _)| *s == shard),
            "original node {i} should not have shard 0"
        );
    }

    // All new nodes should have shard 0.
    for (i, node) in new_nodes.iter().enumerate() {
        let shards = node.shard_list();
        assert!(
            shards.iter().any(|(s, _)| *s == shard),
            "new node {i} should have shard 0"
        );
    }

    // Verify all data from all rounds is still readable.
    let val = rw_get(client, "data", "data_final").await;
    assert_eq!(val, Some("initial".to_string()));
    for i in 0..3 {
        let val = rw_get(client, &format!("after_add_{i}"), &format!("final_add_{i}")).await;
        assert_eq!(val, Some(format!("added_{i}")));
        let val = rw_get(
            client,
            &format!("after_remove_{i}"),
            &format!("final_remove_{i}"),
        )
        .await;
        assert_eq!(val, Some(format!("removed_{i}")));
    }
}

/// Test disaster recovery: backup a shard, destroy all replicas,
/// restore to fresh replicas from backup, verify data survives.
///
/// Test flow:
/// 1. bootstrap_cluster(1, 3, 3) — 1 shard, 3 replicas, 3 nodes
/// 2. Write 3 key-value pairs via OCC transactions, verify readable
/// 3. Force view change — ensures leader_record reflects all committed
///    data. IR records are NOT consistent across intra-shard replicas
///    until view change merges their divergent records.
/// 4. Wait for view change to settle
/// 5. node.backup_shard(shard).await — fetches leader_record as backup
/// 6. node.remove_replica(shard) on all 3 nodes — destroy entire shard
/// 7. Create 3 new nodes, allocate 3 new addresses
/// 8. node.restore_shard(shard, addr, &backup, new_addrs) on each
/// 9. Register new addresses in discovery
/// 10. Wait for StartView processing + view change settlement
/// 11. Create fresh client, verify all 3 keys readable from restored shard
/// 12. Write new data to restored shard, verify it commits and is readable
#[tokio::test(flavor = "multi_thread")]
async fn test_disaster_recovery_backup_restore() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

    // 1. Bootstrap cluster: 1 shard, 3 replicas, 3 nodes.
    let cluster = bootstrap_cluster(1, 3, 3).await;
    let client = &cluster.client;
    let disc_client = disc(create_disc_remote(&cluster.disc_endpoint).await);
    let shard = ShardNumber(0);

    // 2. Write test data.
    let test_data = vec![
        ("dr_key1", "value1"),
        ("dr_key2", "value2"),
        ("dr_key3", "value3"),
    ];
    for (key, value) in &test_data {
        let txn = client.begin();
        txn.put(key.to_string(), Some(value.to_string()));
        assert!(
            txn.commit().await.is_some(),
            "put {key}={value} should commit"
        );
    }

    // Verify data is readable before backup.
    for (key, value) in &test_data {
        let val = rw_get(client, key, &format!("{key}_pre")).await;
        assert_eq!(val, Some(value.to_string()), "pre-backup read of {key}");
    }

    // 3. Force view change to synchronize leader_record with current state.
    //    IR records diverge across replicas between view changes — this
    //    ensures the backup captures all committed operations.
    let shard_nodes: Vec<&Arc<Node>> = cluster.nodes.iter()
        .filter(|n| n.shard_view_number(shard).is_some())
        .collect();
    let old_view = quorum_view_number(&shard_nodes, shard);
    cluster.nodes[0].force_view_change(shard);
    wait_for_view_change(client, &shard_nodes, shard, old_view, "dr_vc_probe").await;

    // 5. Take backup from node 0's replica.
    let backup = cluster.nodes[0]
        .backup_shard(shard)
        .await
        .expect("backup should succeed");

    tracing::info!(
        "backup taken: view={:?}, consensus_entries={}, inconsistent_entries={}",
        backup.view.number,
        backup.record.consensus.len(),
        backup.record.inconsistent.len(),
    );

    // 6. Destroy ALL original replicas.
    for node in &cluster.nodes {
        node.remove_replica(shard);
    }

    // 7. Create new nodes and allocate addresses (listeners kept alive).
    let shard_manager_url = format!("http://{}", cluster.shard_manager_addr);

    let mut new_nodes: Vec<Arc<Node>> = Vec::new();
    let mut _new_temp_dirs: Vec<TempDir> = Vec::new();
    let mut new_listeners: Vec<(SocketAddr, std::net::TcpListener)> = Vec::new();

    for _ in 0..3 {
        new_listeners.push(alloc_listener());
        let td = TempDir::new().unwrap();
        let backend = create_disc_backend(&cluster.disc_endpoint).await;
        let node = Arc::new(Node::with_discovery_backend_and_shard_manager(
            td.path().to_str().unwrap().to_string(),
            backend,
            &shard_manager_url,
        ));
        new_nodes.push(node);
        _new_temp_dirs.push(td);
    }
    let new_addrs: Vec<SocketAddr> = new_listeners.iter().map(|(a, _)| *a).collect();

    // 8. Restore each replica from the backup (pre-bound listeners, no TOCTOU race).
    for (i, (addr, listener)) in new_listeners.into_iter().enumerate() {
        new_nodes[i]
            .restore_shard_with_listener(shard, addr, &backup, new_addrs.clone(), listener)
            .await
            .unwrap_or_else(|e| panic!("restore_shard failed for node {i}: {e}"));
    }

    // 9. Register new addresses in discovery.
    //
    //    Use the restore view number (backup.view + 10, same as restore_shard)
    //    so the monotonic-write check accepts the put — the discovery server
    //    already has a higher view from the original replicas' PUSH.
    let restore_view = backup.view.number.0 + 10;
    let addrs: Vec<TcpAddress> = new_addrs.iter().map(|a| TcpAddress(*a)).collect();
    disc_client
        .strong_put_active_shard_view_membership(ShardNumber(0), tapirs::IrMembership::new(addrs), restore_view)
        .await
        .unwrap();

    // 10. Create a fresh client (discovery sync runs in parallel with
    //     replica bootstrap) and wait for restored shard to accept writes.
    let (client2, _td2, _disc_dir2) = create_test_client(&cluster.disc_endpoint, 1).await;
    wait_for_operational(&client2, "dr_restore_probe").await;

    // 11. Verify ALL data is readable from restored shard.
    for (key, value) in &test_data {
        let val = rw_get(&client2, key, &format!("{key}_post")).await;
        assert_eq!(
            val,
            Some(value.to_string()),
            "post-restore read of {key} should return {value}"
        );
    }

    // 12. Verify new writes work on restored shard.
    let txn = client2.begin();
    txn.put("dr_after_restore".to_string(), Some("works".to_string()));
    assert!(
        txn.commit().await.is_some(),
        "write after restore should commit"
    );

    let val = rw_get(&client2, "dr_after_restore", "dr_after_restore_read").await;
    assert_eq!(val, Some("works".to_string()));

    tracing::info!("disaster recovery test passed");
}

/// Send a single JSON-line admin request and return the raw response string.
async fn send_admin_line(addr: &SocketAddr, request: &str) -> String {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    let stream = tokio::net::TcpStream::connect(addr)
        .await
        .unwrap_or_else(|e| panic!("connect to admin at {addr}: {e}"));
    let (reader, mut writer) = stream.into_split();
    let mut line = request.to_string();
    line.push('\n');
    writer.write_all(line.as_bytes()).await.unwrap();
    let mut lines = BufReader::new(reader).lines();
    lines.next_line().await.unwrap().unwrap_or_default()
}

/// Test cluster-level backup and restore via admin protocol.
///
/// This exercises the full admin protocol codepath for multi-shard
/// disaster recovery:
///   1. Bootstrap a 2-shard, 3-replica, 3-node cluster
///   2. Write test data to both shards
///   3. Take cluster backup via admin protocol (status + view_change + backup_shard)
///   4. Destroy all replicas
///   5. Restore via admin protocol (restore_shard commands)
///   6. Verify all data survives restoration
///   7. Verify new writes work on restored cluster
#[tokio::test(flavor = "multi_thread")]
async fn test_cluster_backup_restore_via_admin() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

    // 1. Bootstrap cluster: 2 shards, 3 replicas, 3 nodes.
    let cluster = bootstrap_cluster(2, 3, 3).await;
    let client = &cluster.client;
    let disc_client = disc(create_disc_remote(&cluster.disc_endpoint).await);

    // 2. Write test data -- keys distributed across both shards.
    //    With 2 shards: shard 0 = [a..n), shard 1 = [n..z).
    let test_data = vec![
        ("apple", "red"),
        ("banana", "yellow"),
        ("orange", "orange"),
        ("plum", "purple"),
    ];
    for (key, value) in &test_data {
        let txn = client.begin();
        txn.put(key.to_string(), Some(value.to_string()));
        assert!(
            txn.commit().await.is_some(),
            "put {key}={value} should commit"
        );
    }
    for (key, value) in &test_data {
        let val = rw_get(client, key, &format!("{key}_pre")).await;
        assert_eq!(val, Some(value.to_string()), "pre-backup read of {key}");
    }

    // 3. Start admin servers on each node.
    let mut admin_addrs: Vec<SocketAddr> = Vec::new();
    for node in &cluster.nodes {
        let addr = alloc_addr();
        crate::node::admin_server::start(
                addr,
                Arc::clone(node),
                #[cfg(feature = "tls")]
                None,
            )
            .await;
        admin_addrs.push(addr);
    }

    // 4. Backup each shard via admin protocol.
    let backup_dir = TempDir::new().unwrap();
    let backup_path = backup_dir.path().to_str().unwrap().to_string();

    // Query status to discover shards.
    let mut shard_to_admin: std::collections::BTreeMap<u32, SocketAddr> =
        std::collections::BTreeMap::new();
    let mut replicas_per_shard: std::collections::HashMap<u32, usize> =
        std::collections::HashMap::new();

    for &admin_addr in &admin_addrs {
        let resp = send_admin_line(&admin_addr, r#"{"command":"status"}"#).await;
        let parsed: serde_json::Value = serde_json::from_str(&resp).unwrap();
        assert!(parsed["ok"].as_bool().unwrap(), "status should succeed");
        if let Some(shards) = parsed["shards"].as_array() {
            for s in shards {
                let shard_id = s["shard"].as_u64().unwrap() as u32;
                shard_to_admin.entry(shard_id).or_insert(admin_addr);
                *replicas_per_shard.entry(shard_id).or_default() += 1;
            }
        }
    }
    assert_eq!(shard_to_admin.len(), 2, "should have 2 shards");

    // Force view changes and backup each shard.
    for (&shard_id, &admin_addr) in &shard_to_admin {
        let shard = ShardNumber(shard_id);
        let shard_nodes: Vec<&Arc<Node>> = cluster.nodes.iter()
            .filter(|n| n.shard_view_number(shard).is_some())
            .collect();
        let old_view = quorum_view_number(&shard_nodes, shard);

        let vc_req = format!(r#"{{"command":"view_change","shard":{shard_id}}}"#);
        let resp = send_admin_line(&admin_addr, &vc_req).await;
        let parsed: serde_json::Value = serde_json::from_str(&resp).unwrap();
        assert!(
            parsed["ok"].as_bool().unwrap(),
            "view_change shard {shard_id}"
        );
        wait_for_view_change(client, &shard_nodes, shard, old_view,
            &format!("admin_vc_probe_s{shard_id}")).await;

        let backup_req = format!(r#"{{"command":"backup_shard","shard":{shard_id}}}"#);
        let resp = send_admin_line(&admin_addr, &backup_req).await;
        let parsed: serde_json::Value = serde_json::from_str(&resp).unwrap();
        assert!(
            parsed["ok"].as_bool().unwrap(),
            "backup_shard {shard_id} should succeed"
        );
        assert!(
            parsed["backup"].is_object(),
            "backup data should be present for shard {shard_id}"
        );

        let path = format!("{backup_path}/shard_{shard_id}.json");
        std::fs::write(&path, serde_json::to_string(&parsed["backup"]).unwrap()).unwrap();
    }

    // Write cluster.json metadata.
    let metadata = serde_json::json!({
        "shards": shard_to_admin.keys().collect::<Vec<_>>(),
        "replicas_per_shard": replicas_per_shard,
    });
    std::fs::write(
        format!("{backup_path}/cluster.json"),
        serde_json::to_string_pretty(&metadata).unwrap(),
    )
    .unwrap();

    tracing::info!("cluster backup complete: 2 shards");

    // 5. Destroy all replicas.
    for shard_id in 0..2u32 {
        for node in &cluster.nodes {
            node.remove_replica(ShardNumber(shard_id));
        }
    }

    // 6. Create new nodes and restore via admin protocol.
    let shard_manager_url = format!("http://{}", cluster.shard_manager_addr);

    let mut new_nodes: Vec<Arc<Node>> = Vec::new();
    let mut _new_temp_dirs: Vec<TempDir> = Vec::new();
    let mut new_admin_addrs: Vec<SocketAddr> = Vec::new();

    for _ in 0..3 {
        let td = TempDir::new().unwrap();
        let backend = create_disc_backend(&cluster.disc_endpoint).await;
        let node = Arc::new(Node::with_discovery_backend_and_shard_manager(
            td.path().to_str().unwrap().to_string(),
            backend,
            &shard_manager_url,
        ));
        let admin_addr = alloc_addr();
        crate::node::admin_server::start(
            admin_addr,
            Arc::clone(&node),
            #[cfg(feature = "tls")]
            None,
        )
        .await;
        new_admin_addrs.push(admin_addr);
        new_nodes.push(node);
        _new_temp_dirs.push(td);
    }

    // Restore each shard via admin protocol.
    for shard_id in 0..2u32 {
        let backup_json = std::fs::read_to_string(format!("{backup_path}/shard_{shard_id}.json"))
            .unwrap_or_else(|e| panic!("read shard_{shard_id}.json: {e}"));
        let backup_value: serde_json::Value = serde_json::from_str(&backup_json).unwrap();

        // Allocate listen addresses for each replica (3 replicas, one per node).
        let mut new_addrs: Vec<SocketAddr> = Vec::new();
        for _ in 0..3 {
            new_addrs.push(alloc_addr());
        }
        let new_membership: Vec<String> = new_addrs.iter().map(|a| a.to_string()).collect();

        // Send restore_shard to each node.
        for i in 0..3 {
            let req = serde_json::json!({
                "command": "restore_shard",
                "shard": shard_id,
                "listen_addr": new_addrs[i].to_string(),
                "backup": backup_value,
                "new_membership": new_membership,
            });

            loop {
                let resp =
                    send_admin_line(&new_admin_addrs[i], &serde_json::to_string(&req).unwrap())
                        .await;
                let parsed: serde_json::Value = serde_json::from_str(&resp).unwrap();
                if parsed["ok"].as_bool().unwrap() {
                    break;
                }
                let msg = parsed["message"].as_str().unwrap_or("");
                if msg.contains("already in use") {
                    // Port conflict — reallocate and retry with a fresh address.
                    tracing::info!("port conflict for shard {shard_id} node {i}, retrying");
                    continue;
                }
                panic!(
                    "restore shard {shard_id} on node {i} failed: {:?}",
                    parsed["message"]
                );
            }
        }

        // Register restored shard with discovery.
        let restore_view = backup_value["view"]["number"].as_u64().unwrap_or(0) + 10;
        let addrs: Vec<TcpAddress> = new_addrs.iter().map(|a| TcpAddress(*a)).collect();
        disc_client
            .strong_put_active_shard_view_membership(ShardNumber(shard_id), tapirs::IrMembership::new(addrs), restore_view)
            .await
            .unwrap();
    }

    // 7. Create fresh client (discovery sync runs in parallel with
    //    replica bootstrap) and wait for restored shards to accept writes.
    let (client2, _td2, _disc_dir2) = create_test_client(&cluster.disc_endpoint, 2).await;
    // Probe shard 0 (key < "n") and shard 1 (key >= "n").
    wait_for_operational(&client2, "admin_restore_probe").await;
    wait_for_operational(&client2, "restore_probe_s1").await;

    // 8. Verify all data survived.
    for (key, value) in &test_data {
        let val = rw_get(&client2, key, &format!("{key}_post")).await;
        assert_eq!(
            val,
            Some(value.to_string()),
            "post-restore read of {key} should return {value}"
        );
    }

    // 9. Verify new writes work on restored cluster.
    let txn = client2.begin();
    txn.put(
        "cluster_after_restore".to_string(),
        Some("works".to_string()),
    );
    assert!(
        txn.commit().await.is_some(),
        "write after cluster restore should commit"
    );

    let val = rw_get(&client2, "cluster_after_restore", "cluster_ar_read").await;
    assert_eq!(val, Some("works".to_string()));

    tracing::info!("cluster backup/restore via admin test passed");
}
