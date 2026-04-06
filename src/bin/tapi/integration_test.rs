use crate::config::ReplicaConfig;
use crate::discovery_backend::DiscoveryBackend;
use crate::helpers::{quorum_view_number, wait_for_operational, wait_for_view_change};
use crate::node::{production_rng, Node};
use rand::{thread_rng, Rng as _};
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use tapirs::discovery::{tapir, InMemoryShardDirectory, RemoteShardDirectory};
use tapirs::backup::local::LocalBackupStorage;
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
    let transport = TcpTransport::with_directory(addr, dir);
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
        let node = Arc::new(Node::new(td.path().to_str().unwrap().to_string(), production_rng));
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
            production_rng,
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
            production_rng,
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
            match new_node.add_replica_join(shard, candidate).await {
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

/// Rewrite cluster.json to replace old replica addresses with fresh ones.
///
/// Returns the new per-shard replica addresses and held listeners.
/// The listeners keep the ports reserved until the caller drops them
/// right before restore_cluster_direct binds. Without this, the
/// bind-then-drop pattern in alloc_addr() races with parallel tests.
fn rewrite_cluster_json_addrs(
    backup_path: &str,
    num_replicas_per_shard: usize,
) -> (Vec<Vec<SocketAddr>>, Vec<std::net::TcpListener>) {
    use tapirs::backup::types::ClusterMetadata;

    let cluster_json = format!("{backup_path}/cluster.json");
    let data = std::fs::read_to_string(&cluster_json).unwrap();
    let mut meta: ClusterMetadata = serde_json::from_str(&data).unwrap();

    let mut fresh_addrs_per_shard = Vec::new();
    let mut held_listeners = Vec::new();
    for shard_hist in &mut meta.shards {
        let mut fresh = Vec::new();
        for _ in 0..num_replicas_per_shard {
            let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let a = l.local_addr().unwrap();
            fresh.push(a);
            held_listeners.push(l);
        }
        let fresh_strings: Vec<String> = fresh.iter().map(|a| a.to_string()).collect();
        // Update all deltas with fresh addresses (restore uses the last delta's addresses).
        for delta in &mut shard_hist.deltas {
            delta.replicas_on_backup_taken = fresh_strings.clone();
        }
        fresh_addrs_per_shard.push(fresh);
    }

    let json = serde_json::to_string_pretty(&meta).unwrap();
    std::fs::write(&cluster_json, json).unwrap();
    (fresh_addrs_per_shard, held_listeners)
}

#[tokio::test(flavor = "multi_thread")]
async fn test_cluster_backup_restore_via_admin() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

    let cluster = bootstrap_cluster(2, 3, 3).await;
    let client = &cluster.client;

    // Write data to shard 0 (keys a-m).
    let txn = client.begin();
    txn.put("apple".to_string(), Some("red".to_string()));
    txn.put("banana".to_string(), Some("yellow".to_string()));
    assert!(txn.commit().await.is_some(), "initial put should commit");

    // Write data to shard 1 (keys n-z).
    let txn = client.begin();
    txn.put("plum".to_string(), Some("purple".to_string()));
    assert!(txn.commit().await.is_some(), "shard 1 put should commit");

    // Force view change on both shards to seal CDC deltas.
    for shard_idx in 0..2u32 {
        let shard = ShardNumber(shard_idx);
        let shard_nodes: Vec<&Arc<Node>> = cluster
            .nodes
            .iter()
            .filter(|n| n.shard_view_number(shard).is_some())
            .collect();
        let old_view = quorum_view_number(&shard_nodes, shard);
        cluster.nodes[0].force_view_change(shard);
        wait_for_view_change(
            client,
            &shard_nodes,
            shard,
            old_view,
            &format!("vc_seal_{shard_idx}"),
        )
        .await;
    }

    // Backup.
    let backup_dir = TempDir::new().unwrap();
    let backup_path = backup_dir.path().to_str().unwrap();
    let sm_url = format!("http://{}", cluster.shard_manager_addr);
    let sm_client = HttpShardManagerClient::new(&sm_url);
    let mgr = tapirs::backup::BackupManager::new(sm_client);
    let storage = LocalBackupStorage::new(backup_path);
    mgr.backup_cluster(&storage).await.unwrap();

    // Verify backup files exist.
    assert!(
        std::path::Path::new(&format!("{backup_path}/cluster.json")).exists(),
        "cluster.json should exist"
    );
    assert!(
        std::path::Path::new(&format!("{backup_path}/shard_0_delta_0.bin")).exists(),
        "shard_0_delta_0.bin should exist"
    );
    assert!(
        std::path::Path::new(&format!("{backup_path}/shard_1_delta_0.bin")).exists(),
        "shard_1_delta_0.bin should exist"
    );

    // Rewrite cluster.json with fresh addresses (old ports remain bound by
    // accept_loop tasks even after remove_replica — port reuse would fail).
    // Hold listeners to reserve ports until restore binds them.
    let (_fresh_addrs, held) = rewrite_cluster_json_addrs(backup_path, 3);

    // Create fresh Node objects for restored replicas (old nodes still hold
    // the original ports, so restored replicas need separate nodes).
    let mut restore_nodes = Vec::new();
    let mut restore_temp_dirs = Vec::new();
    let mut admin_addrs = Vec::new();
    for _ in 0..3u32 {
        let td = TempDir::new().unwrap();
        let backend = create_disc_backend(&cluster.disc_endpoint).await;
        let node = Arc::new(Node::with_discovery_backend_and_shard_manager(
            td.path().to_str().unwrap().to_string(),
            backend,
            &sm_url,
            production_rng,
        ));
        let admin_addr = alloc_addr();
        tapirs::node::node_server::start(
            admin_addr,
            Arc::clone(&node),
            #[cfg(feature = "tls")]
            None,
        )
        .await;
        admin_addrs.push(admin_addr.to_string());
        restore_nodes.push(node);
        restore_temp_dirs.push(td);
    }

    // Release reserved ports right before restore binds them.
    drop(held);

    // Restore.
    let restore_sm_client = HttpShardManagerClient::new(&sm_url);
    let restore_mgr = tapirs::backup::BackupManager::new(restore_sm_client);
    let restore_storage = LocalBackupStorage::new(backup_path);
    restore_mgr
        .restore_cluster(&admin_addrs, &restore_storage)
        .await
        .unwrap();

    // Wait for restored replicas to settle.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Create a new client for the restored cluster.
    let (restored_client, _td, _disc_dir) =
        create_test_client(&cluster.disc_endpoint, 2).await;

    // Verify all data is readable.
    let val = rw_get(&restored_client, "apple", "apple_verify").await;
    assert_eq!(val, Some("red".to_string()));
    let val = rw_get(&restored_client, "banana", "banana_verify").await;
    assert_eq!(val, Some("yellow".to_string()));
    let val = rw_get(&restored_client, "plum", "plum_verify").await;
    assert_eq!(val, Some("purple".to_string()));

    // Verify new writes work on restored cluster.
    let txn = restored_client.begin();
    txn.put("cherry".to_string(), Some("dark".to_string()));
    assert!(
        txn.commit().await.is_some(),
        "new write on restored cluster should commit"
    );

    // Keep temp dirs alive until end of test.
    drop(restore_temp_dirs);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_backup_restore_incremental() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

    let cluster = bootstrap_cluster(1, 3, 3).await;
    let client = &cluster.client;
    let shard = ShardNumber(0);
    let sm_url = format!("http://{}", cluster.shard_manager_addr);

    // Write initial data.
    let txn = client.begin();
    txn.put("alpha".to_string(), Some("1".to_string()));
    txn.put("bravo".to_string(), Some("2".to_string()));
    assert!(txn.commit().await.is_some(), "initial put should commit");

    // Force view change to seal CDC deltas.
    let shard_nodes: Vec<&Arc<Node>> = cluster
        .nodes
        .iter()
        .filter(|n| n.shard_view_number(shard).is_some())
        .collect();
    let old_view = quorum_view_number(&shard_nodes, shard);
    cluster.nodes[0].force_view_change(shard);
    wait_for_view_change(client, &shard_nodes, shard, old_view, "vc_incr_1").await;

    // Full backup.
    let backup_dir = TempDir::new().unwrap();
    let backup_path = backup_dir.path().to_str().unwrap();
    let sm_client = HttpShardManagerClient::new(&sm_url);
    let mgr = tapirs::backup::BackupManager::new(sm_client);
    let storage = LocalBackupStorage::new(backup_path);
    mgr.backup_cluster(&storage).await.unwrap();

    assert!(
        std::path::Path::new(&format!("{backup_path}/shard_0_delta_0.bin")).exists(),
        "delta 0 should exist after full backup"
    );

    // Write more data.
    let txn = client.begin();
    txn.put("charlie".to_string(), Some("3".to_string()));
    txn.put("delta".to_string(), Some("4".to_string()));
    assert!(txn.commit().await.is_some(), "second put should commit");

    // Force another view change for the second batch.
    let old_view = quorum_view_number(&shard_nodes, shard);
    cluster.nodes[0].force_view_change(shard);
    wait_for_view_change(client, &shard_nodes, shard, old_view, "vc_incr_2").await;

    // Incremental backup (reads last_backup_views from cluster.json).
    let sm_client2 = HttpShardManagerClient::new(&sm_url);
    let mgr2 = tapirs::backup::BackupManager::new(sm_client2);
    let storage2 = LocalBackupStorage::new(backup_path);
    mgr2.backup_cluster(&storage2).await.unwrap();

    assert!(
        std::path::Path::new(&format!("{backup_path}/shard_0_delta_1.bin")).exists(),
        "delta 1 should exist after incremental backup"
    );

    // Rewrite cluster.json with fresh addresses for restore.
    // Hold listeners to reserve ports until restore binds them.
    let (_fresh_addrs, held) = rewrite_cluster_json_addrs(backup_path, 3);

    // Create fresh Node objects for restore.
    let mut restore_nodes = Vec::new();
    let mut restore_temp_dirs = Vec::new();
    let mut admin_addrs = Vec::new();
    for _ in 0..3u32 {
        let td = TempDir::new().unwrap();
        let backend = create_disc_backend(&cluster.disc_endpoint).await;
        let node = Arc::new(Node::with_discovery_backend_and_shard_manager(
            td.path().to_str().unwrap().to_string(),
            backend,
            &sm_url,
            production_rng,
        ));
        let admin_addr = alloc_addr();
        tapirs::node::node_server::start(
            admin_addr,
            Arc::clone(&node),
            #[cfg(feature = "tls")]
            None,
        )
        .await;
        admin_addrs.push(admin_addr.to_string());
        restore_nodes.push(node);
        restore_temp_dirs.push(td);
    }

    // Release reserved ports right before restore binds them.
    drop(held);

    // Restore (applies both delta files).
    let restore_sm_client = HttpShardManagerClient::new(&sm_url);
    let restore_mgr = tapirs::backup::BackupManager::new(restore_sm_client);
    let restore_storage = LocalBackupStorage::new(backup_path);
    restore_mgr
        .restore_cluster(&admin_addrs, &restore_storage)
        .await
        .unwrap();

    // Wait for restored replicas to settle.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Create a new client.
    let (restored_client, _td, _disc_dir) =
        create_test_client(&cluster.disc_endpoint, 1).await;

    // Verify all 4 keys.
    let val = rw_get(&restored_client, "alpha", "alpha_verify").await;
    assert_eq!(val, Some("1".to_string()));
    let val = rw_get(&restored_client, "bravo", "bravo_verify").await;
    assert_eq!(val, Some("2".to_string()));
    let val = rw_get(&restored_client, "charlie", "charlie_verify").await;
    assert_eq!(val, Some("3".to_string()));
    let val = rw_get(&restored_client, "delta", "delta_verify").await;
    assert_eq!(val, Some("4".to_string()));

    // Keep temp dirs alive until end of test.
    drop(restore_temp_dirs);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_solo_backup_restore() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

    let cluster = bootstrap_cluster(1, 3, 3).await;
    let client = &cluster.client;
    let shard = ShardNumber(0);

    // Start admin servers on source nodes.
    let mut source_admin_addrs = Vec::new();
    for node in &cluster.nodes {
        let admin_addr = alloc_addr();
        tapirs::node::node_server::start(
            admin_addr,
            Arc::clone(node),
            #[cfg(feature = "tls")]
            None,
        )
        .await;
        source_admin_addrs.push(admin_addr.to_string());
    }

    // Write data.
    let txn = client.begin();
    txn.put("apple".to_string(), Some("red".to_string()));
    txn.put("banana".to_string(), Some("yellow".to_string()));
    assert!(txn.commit().await.is_some(), "initial put should commit");

    // Force view change to seal CDC deltas.
    let shard_nodes: Vec<&Arc<Node>> = cluster
        .nodes
        .iter()
        .filter(|n| n.shard_view_number(shard).is_some())
        .collect();
    let old_view = quorum_view_number(&shard_nodes, shard);
    cluster.nodes[0].force_view_change(shard);
    wait_for_view_change(client, &shard_nodes, shard, old_view, "vc_solo").await;

    // Solo backup.
    let backup_dir = TempDir::new().unwrap();
    let backup_path = backup_dir.path().to_str().unwrap();
    let mut mgr = tapirs::SoloClusterManager::new(tapirs::Rng::from_seed(thread_rng().r#gen()));
    let solo_storage = LocalBackupStorage::new(backup_path);
    mgr.backup_cluster_direct(&source_admin_addrs, &solo_storage)
        .await
        .unwrap();

    // Verify backup files.
    assert!(
        std::path::Path::new(&format!("{backup_path}/cluster.json")).exists(),
        "cluster.json should exist"
    );
    assert!(
        std::path::Path::new(&format!("{backup_path}/shard_0_delta_0.bin")).exists(),
        "shard_0_delta_0.bin should exist"
    );

    // Rewrite cluster.json with fresh addresses.
    // Hold listeners to reserve ports until restore binds them.
    let (fresh_addrs, held) = rewrite_cluster_json_addrs(backup_path, 3);

    // Create fresh Node objects for restore (no discovery needed for solo).
    let mut restore_nodes = Vec::new();
    let mut restore_temp_dirs = Vec::new();
    let mut restore_admin_addrs = Vec::new();
    for _ in 0..3u32 {
        let td = TempDir::new().unwrap();
        let node = Arc::new(Node::new(
            td.path().to_str().unwrap().to_string(),
            production_rng,
        ));
        let admin_addr = alloc_addr();
        tapirs::node::node_server::start(
            admin_addr,
            Arc::clone(&node),
            #[cfg(feature = "tls")]
            None,
        )
        .await;
        restore_admin_addrs.push(admin_addr.to_string());
        restore_nodes.push(node);
        restore_temp_dirs.push(td);
    }

    // Release reserved ports right before restore binds them.
    drop(held);

    // Solo restore.
    let mut restore_mgr =
        tapirs::SoloClusterManager::new(tapirs::Rng::from_seed(thread_rng().r#gen()));
    let solo_restore_storage = LocalBackupStorage::new(backup_path);
    restore_mgr
        .restore_cluster_direct(&restore_admin_addrs, &solo_restore_storage)
        .await
        .unwrap();

    // Wait for restored replicas to settle.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Create a client pointing directly to restored replicas (no discovery).
    let local_addr = alloc_addr();
    let dir = Arc::new(InMemoryShardDirectory::new());
    let transport: TestTransport = TcpTransport::with_directory(
        TcpAddress(local_addr),
        dir,
    );
    let shard_addrs: Vec<TcpAddress> =
        fresh_addrs[0].iter().map(|a| TcpAddress(*a)).collect();
    transport.set_shard_addresses(ShardNumber(0), tapirs::IrMembership::new(shard_addrs));
    let entries = build_shard_entries(1);
    let directory = Arc::new(RwLock::new(ShardDirectory::new(entries)));
    let router = Arc::new(DynamicRouter::new(directory));
    let tapir_client = Arc::new(TapirClient::new(
        tapirs::Rng::from_seed(thread_rng().r#gen()),
        transport,
    ));
    let restored_client = Arc::new(RoutingClient::new(tapir_client, router));

    // Verify data.
    let val = rw_get(&restored_client, "apple", "apple_solo_verify").await;
    assert_eq!(val, Some("red".to_string()));
    let val = rw_get(&restored_client, "banana", "banana_solo_verify").await;
    assert_eq!(val, Some("yellow".to_string()));

    // Verify new writes work on restored cluster.
    let txn = restored_client.begin();
    txn.put("cherry".to_string(), Some("dark".to_string()));
    assert!(
        txn.commit().await.is_some(),
        "new write on restored cluster should commit"
    );

    drop(restore_temp_dirs);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_solo_backup_restore_incremental() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

    let cluster = bootstrap_cluster(1, 3, 3).await;
    let client = &cluster.client;
    let shard = ShardNumber(0);

    // Start admin servers on source nodes.
    let mut source_admin_addrs = Vec::new();
    for node in &cluster.nodes {
        let admin_addr = alloc_addr();
        tapirs::node::node_server::start(
            admin_addr,
            Arc::clone(node),
            #[cfg(feature = "tls")]
            None,
        )
        .await;
        source_admin_addrs.push(admin_addr.to_string());
    }

    // Write initial data.
    let txn = client.begin();
    txn.put("alpha".to_string(), Some("1".to_string()));
    txn.put("bravo".to_string(), Some("2".to_string()));
    assert!(txn.commit().await.is_some(), "initial put should commit");

    // Force view change to seal CDC deltas.
    let shard_nodes: Vec<&Arc<Node>> = cluster
        .nodes
        .iter()
        .filter(|n| n.shard_view_number(shard).is_some())
        .collect();
    let old_view = quorum_view_number(&shard_nodes, shard);
    cluster.nodes[0].force_view_change(shard);
    wait_for_view_change(client, &shard_nodes, shard, old_view, "vc_solo_incr_1").await;

    // Full solo backup.
    let backup_dir = TempDir::new().unwrap();
    let backup_path = backup_dir.path().to_str().unwrap();
    let mut mgr = tapirs::SoloClusterManager::new(tapirs::Rng::from_seed(thread_rng().r#gen()));
    let solo_storage = LocalBackupStorage::new(backup_path);
    mgr.backup_cluster_direct(&source_admin_addrs, &solo_storage)
        .await
        .unwrap();

    assert!(
        std::path::Path::new(&format!("{backup_path}/shard_0_delta_0.bin")).exists(),
        "delta 0 should exist after full backup"
    );

    // Write more data.
    let txn = client.begin();
    txn.put("charlie".to_string(), Some("3".to_string()));
    txn.put("delta_key".to_string(), Some("4".to_string()));
    assert!(txn.commit().await.is_some(), "second put should commit");

    // Force another view change for the second batch.
    let old_view = quorum_view_number(&shard_nodes, shard);
    cluster.nodes[0].force_view_change(shard);
    wait_for_view_change(client, &shard_nodes, shard, old_view, "vc_solo_incr_2").await;

    // Incremental solo backup (reads last_backup_views from cluster.json).
    let mut mgr2 = tapirs::SoloClusterManager::new(tapirs::Rng::from_seed(thread_rng().r#gen()));
    let solo_storage2 = LocalBackupStorage::new(backup_path);
    mgr2.backup_cluster_direct(&source_admin_addrs, &solo_storage2)
        .await
        .unwrap();

    assert!(
        std::path::Path::new(&format!("{backup_path}/shard_0_delta_1.bin")).exists(),
        "delta 1 should exist after incremental backup"
    );

    // Rewrite cluster.json with fresh addresses for restore.
    // Hold listeners to reserve ports until restore binds them.
    let (fresh_addrs, held) = rewrite_cluster_json_addrs(backup_path, 3);

    // Create fresh Node objects for restore (no discovery needed for solo).
    let mut restore_nodes = Vec::new();
    let mut restore_temp_dirs = Vec::new();
    let mut restore_admin_addrs = Vec::new();
    for _ in 0..3u32 {
        let td = TempDir::new().unwrap();
        let node = Arc::new(Node::new(
            td.path().to_str().unwrap().to_string(),
            production_rng,
        ));
        let admin_addr = alloc_addr();
        tapirs::node::node_server::start(
            admin_addr,
            Arc::clone(&node),
            #[cfg(feature = "tls")]
            None,
        )
        .await;
        restore_admin_addrs.push(admin_addr.to_string());
        restore_nodes.push(node);
        restore_temp_dirs.push(td);
    }

    // Release reserved ports right before restore binds them.
    drop(held);

    // Solo restore (applies both delta files).
    let mut restore_mgr =
        tapirs::SoloClusterManager::new(tapirs::Rng::from_seed(thread_rng().r#gen()));
    let solo_restore_storage = LocalBackupStorage::new(backup_path);
    restore_mgr
        .restore_cluster_direct(&restore_admin_addrs, &solo_restore_storage)
        .await
        .unwrap();

    // Wait for restored replicas to settle.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Create a client pointing directly to restored replicas (no discovery).
    let local_addr = alloc_addr();
    let dir = Arc::new(InMemoryShardDirectory::new());
    let transport: TestTransport = TcpTransport::with_directory(
        TcpAddress(local_addr),
        dir,
    );
    let shard_addrs: Vec<TcpAddress> =
        fresh_addrs[0].iter().map(|a| TcpAddress(*a)).collect();
    transport.set_shard_addresses(ShardNumber(0), tapirs::IrMembership::new(shard_addrs));
    let entries = build_shard_entries(1);
    let directory = Arc::new(RwLock::new(ShardDirectory::new(entries)));
    let router = Arc::new(DynamicRouter::new(directory));
    let tapir_client = Arc::new(TapirClient::new(
        tapirs::Rng::from_seed(thread_rng().r#gen()),
        transport,
    ));
    let restored_client = Arc::new(RoutingClient::new(tapir_client, router));

    // Verify all 4 keys.
    let val = rw_get(&restored_client, "alpha", "alpha_solo_incr_verify").await;
    assert_eq!(val, Some("1".to_string()));
    let val = rw_get(&restored_client, "bravo", "bravo_solo_incr_verify").await;
    assert_eq!(val, Some("2".to_string()));
    let val = rw_get(&restored_client, "charlie", "charlie_solo_incr_verify").await;
    assert_eq!(val, Some("3".to_string()));
    let val = rw_get(&restored_client, "delta_key", "delta_key_solo_incr_verify").await;
    assert_eq!(val, Some("4".to_string()));

    drop(restore_temp_dirs);
}
