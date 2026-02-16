use crate::discovery::HttpDiscoveryClient;
use crate::node::Node;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use tapirs::discovery::{DiscoveryClient as _, InMemoryShardDirectory};
use tapirs::{
    DynamicRouter, KeyRange, RoutingClient, ShardDirectory, ShardEntry, ShardNumber, TapirClient,
    TapirReplica, TcpAddress, TcpTransport,
};
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::time::Duration;

fn alloc_addr() -> SocketAddr {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let a = l.local_addr().unwrap();
    drop(l);
    a
}

fn env_or(var: &str, default: u32) -> u32 {
    std::env::var(var)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

struct TestCluster {
    discovery_addr: SocketAddr,
    nodes: Vec<Arc<Node>>,
    replica_addrs: Vec<Vec<SocketAddr>>, // [shard][replica_idx]
    _temp_dirs: Vec<TempDir>,
}

type K = String;
type V = String;
type TestTransport = TcpTransport<TapirReplica<K, V>>;

/// Mirrors the operator workflow:
///   tapi discovery -> tapi shard-manager -> tapi node -> tapi admin add-replica
async fn bootstrap_cluster(
    num_shards: u32,
    replicas_per_shard: u32,
    num_nodes: u32,
) -> TestCluster {
    // === tapi discovery ===
    let disc_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let discovery_addr = disc_listener.local_addr().unwrap();
    tokio::spawn(crate::discovery::serve(disc_listener));

    // === tapi shard-manager ===
    let mgr_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mgr_addr = mgr_listener.local_addr().unwrap();
    let discovery_url = format!("http://{discovery_addr}");
    tokio::spawn(crate::shard_manager_server::serve(
        mgr_listener,
        discovery_url.clone(),
    ));

    // === tapi node (one per num_nodes) ===
    let shard_manager_url = format!("http://{mgr_addr}");
    let mut nodes = Vec::new();
    let mut temp_dirs = Vec::new();
    for _ in 0..num_nodes {
        let td = TempDir::new().unwrap();
        let node = Arc::new(Node::with_discovery_and_shard_manager(
            td.path().to_str().unwrap().to_string(),
            &discovery_url,
            &shard_manager_url,
        ));
        nodes.push(node);
        temp_dirs.push(td);
    }

    // === tapi admin add-replica (one at a time per shard) ===
    // Ports are allocated just-in-time and retried on "already in use"
    // to avoid TOCTOU races between parallel test processes.
    let disc_client = HttpDiscoveryClient::new(&discovery_addr.to_string());
    let mut replica_addrs: Vec<Vec<SocketAddr>> = Vec::new();
    for shard_idx in 0..num_shards {
        let mut shard_addrs = Vec::new();
        for replica_idx in 0..replicas_per_shard {
            let node_idx = replica_idx as usize % num_nodes as usize;
            let shard = ShardNumber(shard_idx);

            // Retry with a new port on bind failure (TOCTOU race).
            let addr = loop {
                let candidate = alloc_addr();
                match nodes[node_idx].create_replica(shard, candidate).await {
                    Ok(()) => break candidate,
                    Err(e) if e.contains("already in use") => continue,
                    Err(e) => panic!("create_replica failed: {e}"),
                }
            };

            // Operator registers with discovery.
            shard_addrs.push(addr);
            let registered: Vec<String> = shard_addrs.iter().map(|a| a.to_string()).collect();
            disc_client
                .register_shard(shard_idx, registered)
                .await
                .unwrap();

            // Wait for view change to settle.
            let settle_time = if replica_idx == 0 {
                Duration::from_secs(3)
            } else {
                Duration::from_secs(5)
            };
            tokio::time::sleep(settle_time).await;
        }
        replica_addrs.push(shard_addrs);
    }

    TestCluster {
        discovery_addr,
        nodes,
        replica_addrs,
        _temp_dirs: temp_dirs,
    }
}

/// Create client — same setup as tapi client (src/bin/tapi/client.rs).
/// Uses DiscoveryShardDirectory for address resolution, NOT manual
/// transport.set_shard_addresses().
async fn create_test_client(
    cluster: &TestCluster,
) -> (
    Arc<RoutingClient<K, V, TestTransport, DynamicRouter<K>>>,
    TempDir,
    Arc<tapirs::discovery::DiscoveryShardDirectory<TcpAddress, HttpDiscoveryClient>>,
) {
    let local_addr = alloc_addr();
    let td = TempDir::new().unwrap();
    let dir = Arc::new(InMemoryShardDirectory::new());

    // Same as client.rs: DiscoveryShardDirectory auto-syncs shard->membership
    // from discovery, populating dir.
    let disc_client = Arc::new(HttpDiscoveryClient::new(
        &cluster.discovery_addr.to_string(),
    ));
    let discovery_dir = tapirs::discovery::DiscoveryShardDirectory::<TcpAddress, _>::new(
        Arc::clone(&dir),
        disc_client,
        std::time::Duration::from_millis(100), // fast sync for tests
    );

    // Wait for initial sync to populate dir from discovery.
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Same as client.rs: TcpTransport with shared directory.
    let transport: TestTransport = TcpTransport::with_directory(
        TcpAddress(local_addr),
        td.path().to_str().unwrap().to_string(),
        Arc::clone(&dir),
    );

    // Key routing — discovery doesn't store key ranges, so this is
    // still needed (same as client.rs).
    let entries = build_shard_entries(cluster.replica_addrs.len() as u32);
    let directory = Arc::new(RwLock::new(ShardDirectory::new(entries)));
    let router = Arc::new(DynamicRouter::new(directory));
    let tapir_client = Arc::new(TapirClient::new(transport));
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
        let val = txn.get(key.to_string()).await;
        txn.put(dest_key.to_string(), val.clone());
        if txn.commit().await.is_some() {
            return val;
        }
        // OCC abort — read was stale (Decide not yet propagated), retry.
        tracing::info!("rw_get({key}): OCC abort on attempt {attempt}, retrying");
        tokio::time::sleep(Duration::from_millis(200)).await;
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
    let (client, _td, _disc_dir) = create_test_client(&cluster).await;

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
    let val = rw_get(&client, "key1", "key1_read").await;
    assert_eq!(val, Some("value1".to_string()));

    // Overwrite + verify via rw dependency.
    let txn = client.begin();
    txn.put("key1".to_string(), Some("value2".to_string()));
    assert!(txn.commit().await.is_some(), "overwrite should commit");

    let val = rw_get(&client, "key1", "key1_read2").await;
    assert_eq!(val, Some("value2".to_string()));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_view_change_during_transactions() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

    let cluster = bootstrap_cluster(2, 3, 3).await;
    let (client, _td, _disc_dir) = create_test_client(&cluster).await;

    let txn = client.begin();
    txn.put("survive".to_string(), Some("yes".to_string()));
    assert!(txn.commit().await.is_some());

    // Same as `tapi admin view-change --shard 0`.
    cluster.nodes[0].force_view_change(ShardNumber(0));
    tokio::time::sleep(Duration::from_secs(5)).await;

    let val = rw_get(&client, "survive", "survive_read").await;
    assert_eq!(val, Some("yes".to_string()));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_remove_replica() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

    let cluster = bootstrap_cluster(1, 3, 3).await;
    let (client, _td, _disc_dir) = create_test_client(&cluster).await;

    let txn = client.begin();
    txn.put("before".to_string(), Some("ok".to_string()));
    assert!(txn.commit().await.is_some());

    // Same as `tapi admin remove-replica --shard 0`.
    assert!(cluster.nodes[0].remove_replica(ShardNumber(0)));
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Data should still be readable from remaining 2 replicas.
    let val = rw_get(&client, "before", "before_read").await;
    assert_eq!(val, Some("ok".to_string()));
}
