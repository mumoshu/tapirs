use crate::discovery::HttpDiscoveryClient;
use crate::node::Node;
use rand::{thread_rng, Rng as _};
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use tapirs::discovery::{InMemoryShardDirectory, RemoteShardDirectory as _};
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
    shard_manager_addr: SocketAddr,
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
                match nodes[node_idx].create_replica(shard, candidate, "memory").await {
                    Ok(()) => break candidate,
                    Err(e) if e.contains("already in use") => continue,
                    Err(e) => panic!("create_replica failed: {e}"),
                }
            };

            // Operator registers with discovery.
            shard_addrs.push(addr);
            let registered: Vec<String> = shard_addrs.iter().map(|a| a.to_string()).collect();
            let membership = tapirs::discovery::strings_to_membership::<TcpAddress>(&registered)
                .expect("parse membership");
            disc_client
                .put(ShardNumber(shard_idx), membership, 0)
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
        shard_manager_addr: mgr_addr,
        nodes,
        replica_addrs,
        _temp_dirs: temp_dirs,
    }
}

/// Create client — same setup as tapi client (src/bin/tapi/client.rs).
/// Uses CachingShardDirectory for address resolution, NOT manual
/// transport.set_shard_addresses().
async fn create_test_client(
    cluster: &TestCluster,
) -> (
    Arc<RoutingClient<K, V, TestTransport, DynamicRouter<K>>>,
    TempDir,
    Arc<tapirs::discovery::CachingShardDirectory<TcpAddress, HttpDiscoveryClient>>,
) {
    let local_addr = alloc_addr();
    let td = TempDir::new().unwrap();
    let dir = Arc::new(InMemoryShardDirectory::new());

    // Same as client.rs: CachingShardDirectory auto-syncs shard->membership
    // from discovery, populating dir.
    let disc_client = Arc::new(HttpDiscoveryClient::new(
        &cluster.discovery_addr.to_string(),
    ));
    let discovery_dir = tapirs::discovery::CachingShardDirectory::<TcpAddress, _>::new(
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
    let (client, _td, _disc_dir) = create_test_client(&cluster).await;
    let disc_client = HttpDiscoveryClient::new(&cluster.discovery_addr.to_string());
    let shard = ShardNumber(0);

    // Write initial data and verify R/W.
    let txn = client.begin();
    txn.put("data".to_string(), Some("initial".to_string()));
    assert!(txn.commit().await.is_some(), "initial put should commit");

    let val = rw_get(&client, "data", "data_check").await;
    assert_eq!(val, Some("initial".to_string()));

    let original_addrs = cluster.replica_addrs[0].clone();
    let mut live_addrs: Vec<SocketAddr> = original_addrs.clone();

    let discovery_url = format!("http://{}", cluster.discovery_addr);
    let shard_manager_url = format!("http://{}", cluster.shard_manager_addr);

    // Keep new nodes and temp dirs alive for the duration of the test.
    let mut new_nodes: Vec<Arc<Node>> = Vec::new();
    let mut _new_temp_dirs: Vec<TempDir> = Vec::new();

    // Rolling replacement: for each original replica, add a new one then
    // leave+remove the old one.
    for i in 0..3 {
        tracing::info!("--- round {i}: adding new replica ---");

        // === ADD new replica ===
        let td = TempDir::new().unwrap();
        let new_node = Arc::new(Node::with_discovery_and_shard_manager(
            td.path().to_str().unwrap().to_string(),
            &discovery_url,
            &shard_manager_url,
        ));

        let new_addr = loop {
            let candidate = alloc_addr();
            match new_node.create_replica(shard, candidate, "memory").await {
                Ok(()) => break candidate,
                Err(e) if e.contains("already in use") => continue,
                Err(e) => panic!("create_replica failed in round {i}: {e}"),
            }
        };

        // Register new address in discovery.
        live_addrs.push(new_addr);
        let addrs: Vec<TcpAddress> = live_addrs.iter().map(|a| TcpAddress(*a)).collect();
        disc_client
            .put(ShardNumber(0), tapirs::IrMembership::new(addrs), 0)
            .await
            .unwrap();

        // Wait for AddMember view change to settle.
        tokio::time::sleep(Duration::from_secs(10)).await;

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
        let val = rw_get(&client, "data", &format!("data_after_add_{i}")).await;
        assert_eq!(val, Some("initial".to_string()));

        new_nodes.push(new_node);
        _new_temp_dirs.push(td);

        // === LEAVE + REMOVE original replica ===
        tracing::info!("--- round {i}: leaving original replica ---");

        cluster.nodes[i]
            .leave_shard(shard)
            .await
            .unwrap_or_else(|e| panic!("leave_shard failed in round {i}: {e}"));

        // Wait for RemoveMember view change to settle.
        tokio::time::sleep(Duration::from_secs(10)).await;

        // Drop the local handle.
        assert!(
            cluster.nodes[i].remove_replica(shard),
            "remove_replica should succeed in round {i}"
        );

        // Update discovery: remove old address.
        live_addrs.retain(|a| *a != original_addrs[i]);
        let addrs: Vec<TcpAddress> = live_addrs.iter().map(|a| TcpAddress(*a)).collect();
        disc_client
            .put(ShardNumber(0), tapirs::IrMembership::new(addrs), 0)
            .await
            .unwrap();

        // Let discovery sync propagate to client.
        tokio::time::sleep(Duration::from_secs(2)).await;

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
        let val = rw_get(&client, "data", &format!("data_after_remove_{i}")).await;
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
    let val = rw_get(&client, "data", "data_final").await;
    assert_eq!(val, Some("initial".to_string()));
    for i in 0..3 {
        let val = rw_get(&client, &format!("after_add_{i}"), &format!("final_add_{i}")).await;
        assert_eq!(val, Some(format!("added_{i}")));
        let val = rw_get(
            &client,
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
    let (client, _td, _disc_dir) = create_test_client(&cluster).await;
    let disc_client = HttpDiscoveryClient::new(&cluster.discovery_addr.to_string());
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
        let val = rw_get(&client, key, &format!("{key}_pre")).await;
        assert_eq!(val, Some(value.to_string()), "pre-backup read of {key}");
    }

    // 3. Force view change to synchronize leader_record with current state.
    //    IR records diverge across replicas between view changes — this
    //    ensures the backup captures all committed operations.
    cluster.nodes[0].force_view_change(shard);
    tokio::time::sleep(Duration::from_secs(5)).await;

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
    tokio::time::sleep(Duration::from_secs(1)).await;

    // 7. Create new nodes and allocate addresses.
    let discovery_url = format!("http://{}", cluster.discovery_addr);
    let shard_manager_url = format!("http://{}", cluster.shard_manager_addr);

    let mut new_nodes: Vec<Arc<Node>> = Vec::new();
    let mut _new_temp_dirs: Vec<TempDir> = Vec::new();
    let mut new_addrs: Vec<SocketAddr> = Vec::new();

    for _ in 0..3 {
        new_addrs.push(alloc_addr());
        let td = TempDir::new().unwrap();
        let node = Arc::new(Node::with_discovery_and_shard_manager(
            td.path().to_str().unwrap().to_string(),
            &discovery_url,
            &shard_manager_url,
        ));
        new_nodes.push(node);
        _new_temp_dirs.push(td);
    }

    // 8. Restore each replica from the backup.
    for i in 0..3 {
        loop {
            match new_nodes[i]
                .restore_shard(shard, new_addrs[i], &backup, new_addrs.clone())
                .await
            {
                Ok(()) => break,
                Err(e) if e.contains("already in use") => {
                    new_addrs[i] = alloc_addr();
                    continue;
                }
                Err(e) => panic!("restore_shard failed for node {i}: {e}"),
            }
        }
    }

    // 9. Register new addresses in discovery.
    //
    //    Use the restore view number (backup.view + 10, same as restore_shard)
    //    so the monotonic-write check accepts the put — the discovery server
    //    already has a higher view from the original replicas' PUSH.
    //
    //    In a real deployment, the discovery server is part of the cluster
    //    state. If you destroy all replicas and restore from backup, you'd
    //    ideally restore the discovery server state too (so view numbers are
    //    consistent). Today the discovery server is stateless (in-memory
    //    only), so a restore scenario inherits whatever stale state the
    //    discovery server accumulated from the old cluster. Using the
    //    restore view number (backup.view + 10) works as a workaround
    //    because it's strictly higher than anything the old cluster used.
    //    If the discovery server later gains a persistent metadata store,
    //    backup/restore should snapshot and restore the discovery state too.
    let restore_view = backup.view.number.0 + 10;
    let addrs: Vec<TcpAddress> = new_addrs.iter().map(|a| TcpAddress(*a)).collect();
    disc_client
        .put(ShardNumber(0), tapirs::IrMembership::new(addrs), restore_view)
        .await
        .unwrap();

    // 10. Wait for StartView processing + discovery sync.
    tokio::time::sleep(Duration::from_secs(5)).await;

    // 11. Create a fresh client that discovers the new replicas.
    let (client2, _td2, _disc_dir2) = create_test_client(&cluster).await;

    // Verify ALL data is readable from restored shard.
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
    let (client, _td, _disc_dir) = create_test_client(&cluster).await;
    let disc_client = HttpDiscoveryClient::new(&cluster.discovery_addr.to_string());

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
        let val = rw_get(&client, key, &format!("{key}_pre")).await;
        assert_eq!(val, Some(value.to_string()), "pre-backup read of {key}");
    }

    // 3. Start admin servers on each node.
    let mut admin_addrs: Vec<SocketAddr> = Vec::new();
    for node in &cluster.nodes {
        let addr = alloc_addr();
        crate::admin_server::start(addr, Arc::clone(node)).await;
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
        let vc_req = format!(r#"{{"command":"view_change","shard":{shard_id}}}"#);
        let resp = send_admin_line(&admin_addr, &vc_req).await;
        let parsed: serde_json::Value = serde_json::from_str(&resp).unwrap();
        assert!(
            parsed["ok"].as_bool().unwrap(),
            "view_change shard {shard_id}"
        );
        tokio::time::sleep(Duration::from_secs(5)).await;

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
    tokio::time::sleep(Duration::from_secs(1)).await;

    // 6. Create new nodes and restore via admin protocol.
    let discovery_url = format!("http://{}", cluster.discovery_addr);
    let shard_manager_url = format!("http://{}", cluster.shard_manager_addr);

    let mut new_nodes: Vec<Arc<Node>> = Vec::new();
    let mut _new_temp_dirs: Vec<TempDir> = Vec::new();
    let mut new_admin_addrs: Vec<SocketAddr> = Vec::new();

    for _ in 0..3 {
        let td = TempDir::new().unwrap();
        let node = Arc::new(Node::with_discovery_and_shard_manager(
            td.path().to_str().unwrap().to_string(),
            &discovery_url,
            &shard_manager_url,
        ));
        let admin_addr = alloc_addr();
        crate::admin_server::start(admin_addr, Arc::clone(&node)).await;
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
        //
        //   Use restore view (backup.view + 10, same as Node::restore_shard)
        //   so the monotonic-write check accepts the put — the discovery server
        //   already has a higher view from the original replicas' PUSH.
        //
        //   See comment in test_disaster_recovery_backup_restore for the full
        //   rationale: discovery is part of cluster state and ideally should be
        //   restored alongside replicas. Today the discovery server is stateless
        //   (in-memory only), so we use the restore view as a workaround.
        let restore_view = backup_value["view"]["number"].as_u64().unwrap_or(0) + 10;
        let addrs: Vec<TcpAddress> = new_addrs.iter().map(|a| TcpAddress(*a)).collect();
        disc_client
            .put(ShardNumber(shard_id), tapirs::IrMembership::new(addrs), restore_view)
            .await
            .unwrap();
    }

    // 7. Wait for restoration to settle.
    tokio::time::sleep(Duration::from_secs(5)).await;

    // 8. Create fresh client and verify all data survived.
    let (client2, _td2, _disc_dir2) = create_test_client(&cluster).await;

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
