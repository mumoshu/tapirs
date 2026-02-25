use crate::config::ReplicaConfig;
use crate::helpers::wait_for_operational;
use crate::node::Node;
use rand::Rng as _;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use tapirs::{
    DynamicRouter, IrMembership, KeyRange, RoutingClient, ShardDirectory, ShardEntry, ShardNumber,
    TapirClient, TapirReplica, TcpAddress, TcpTransport,
};
use tempfile::TempDir;
use tokio::time::Duration;

type K = String;
type V = String;
type TestTransport = TcpTransport<TapirReplica<K, V>>;

/// Generate a self-signed CA + leaf certificate with IP SAN 127.0.0.1 for mTLS.
///
/// Both replicas and clients use the same cert in tests (all on localhost).
fn generate_test_certs(dir: &std::path::Path) -> tapirs::tls::TlsConfig {
    use std::io::Write;

    // CA
    let ca_key = rcgen::KeyPair::generate().unwrap();
    let ca_params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
    let ca_cert = ca_params.self_signed(&ca_key).unwrap();

    // Leaf cert with IP SAN for 127.0.0.1 (what TcpAddress resolves to in tests).
    let mut leaf_params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
    leaf_params.subject_alt_names = vec![
        rcgen::SanType::IpAddress(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)),
    ];
    leaf_params.is_ca = rcgen::IsCa::NoCa;
    let leaf_key = rcgen::KeyPair::generate().unwrap();
    let leaf_cert = leaf_params.signed_by(&leaf_key, &ca_cert, &ca_key).unwrap();

    let cert_path = dir.join("tls.crt");
    let key_path = dir.join("tls.key");
    let ca_path = dir.join("ca.crt");

    std::fs::File::create(&cert_path)
        .unwrap()
        .write_all(leaf_cert.pem().as_bytes())
        .unwrap();
    std::fs::File::create(&key_path)
        .unwrap()
        .write_all(leaf_key.serialize_pem().as_bytes())
        .unwrap();
    std::fs::File::create(&ca_path)
        .unwrap()
        .write_all(ca_cert.pem().as_bytes())
        .unwrap();

    tapirs::tls::TlsConfig {
        cert_path,
        key_path,
        ca_path,
        server_name: None,
    }
}

fn alloc_listener() -> (SocketAddr, std::net::TcpListener) {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let a = l.local_addr().unwrap();
    (a, l)
}

fn alloc_addr() -> SocketAddr {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let a = l.local_addr().unwrap();
    drop(l);
    a
}

/// Basic TLS integration: 3 replicas, 1 shard, static membership.
///
/// Verifies that replica-to-replica and client-to-replica communication
/// works correctly over mTLS. All components use the same test certificate
/// (all on 127.0.0.1).
#[tokio::test(flavor = "multi_thread")]
async fn test_tls_read_write() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

    let cert_dir = TempDir::new().unwrap();
    let tls_config = generate_test_certs(cert_dir.path());

    // Create 3 nodes, each hosting 1 replica of shard 0.
    let mut nodes = Vec::new();
    let mut temp_dirs = Vec::new();
    let mut listeners = Vec::new();

    for _ in 0..3 {
        listeners.push(alloc_listener());
    }
    let membership: Vec<String> = listeners.iter().map(|(a, _)| a.to_string()).collect();
    let addrs: Vec<SocketAddr> = listeners.iter().map(|(a, _)| *a).collect();

    for (addr, listener) in listeners {
        let td = TempDir::new().unwrap();
        let mut node = Node::new(td.path().to_str().unwrap().to_string());
        node.tls_config = Some(tls_config.clone());
        let node = Arc::new(node);

        node.add_replica_with_listener(
            &ReplicaConfig {
                shard: 0,
                listen_addr: addr.to_string(),
                membership: membership.clone(),
            },
            listener,
        )
        .await
        .unwrap();

        nodes.push(node);
        temp_dirs.push(td);
    }

    // Create TLS-enabled client transport.
    let client_addr = alloc_addr();
    let client_td = TempDir::new().unwrap();
    let client_dir = Arc::new(tapirs::discovery::InMemoryShardDirectory::new());
    let transport: TestTransport = TcpTransport::with_tls(
        TcpAddress(client_addr),
        client_td.path().to_str().unwrap().to_string(),
        Arc::clone(&client_dir),
        &tls_config,
    )
    .unwrap();

    // Set shard membership so the client can reach replicas.
    let tcp_addrs: Vec<TcpAddress> = addrs.iter().map(|a| TcpAddress(*a)).collect();
    transport.set_shard_addresses(ShardNumber(0), IrMembership::new(tcp_addrs));

    // Create routing client with a single-shard router.
    let entries = vec![ShardEntry {
        shard: ShardNumber(0),
        range: KeyRange {
            start: None,
            end: None,
        },
    }];
    let directory = Arc::new(RwLock::new(ShardDirectory::new(entries)));
    let router = Arc::new(DynamicRouter::new(directory));
    let rng = tapirs::Rng::from_seed(rand::thread_rng().r#gen());
    let tapir_client = Arc::new(TapirClient::new(rng, transport));
    let client = Arc::new(RoutingClient::new(tapir_client, router));

    // Wait for the shard to be operational.
    wait_for_operational(&client, "__tls_probe__").await;

    // PUT
    let txn = client.begin();
    txn.put("tls_key".to_string(), Some("tls_value".to_string()));
    assert!(txn.commit().await.is_some(), "TLS put should commit");

    // GET via read-write transaction (same rw_get pattern as main tests).
    for attempt in 0..10 {
        let txn = client.begin();
        let val = txn.get("tls_key".to_string()).await.unwrap();
        txn.put("tls_key_read".to_string(), val.clone());
        if txn.commit().await.is_some() {
            assert_eq!(val, Some("tls_value".to_string()));
            return;
        }
        tracing::info!("tls read attempt {attempt} aborted, retrying");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("TLS read failed after 10 retries");
}

/// Verify TLS admin server works end-to-end.
///
/// Creates a node with TLS, starts an admin server with TLS acceptor,
/// and sends a status query using the TLS admin client.
#[tokio::test(flavor = "multi_thread")]
async fn test_tls_admin_server() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

    let cert_dir = TempDir::new().unwrap();
    let tls_config = generate_test_certs(cert_dir.path());

    let td = TempDir::new().unwrap();
    let mut node = Node::new(td.path().to_str().unwrap().to_string());
    node.tls_config = Some(tls_config.clone());
    let node = Arc::new(node);

    // Create a replica with static membership.
    let (replica_addr, listener) = alloc_listener();
    node.add_replica_with_listener(
        &ReplicaConfig {
            shard: 0,
            listen_addr: replica_addr.to_string(),
            membership: vec![replica_addr.to_string()],
        },
        listener,
    )
    .await
    .unwrap();

    // Start admin server with TLS — use a pre-bound port to know the address,
    // then let start() re-bind (brief TOCTOU is acceptable in tests).
    let admin_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let admin_addr = admin_listener.local_addr().unwrap();
    drop(admin_listener);

    let tls_acceptor = tapirs::tls::ReloadableTlsAcceptor::new(&tls_config).unwrap();
    tokio::spawn(crate::node::admin_server::start(
        admin_addr,
        Arc::clone(&node),
        #[cfg(feature = "tls")]
        Some(tls_acceptor),
    ));

    // Give the admin server a moment to bind and start accepting.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Send status request via TLS.
    let tls_connector = tapirs::tls::ReloadableTlsConnector::new(&tls_config).unwrap();
    let resp = tapirs::node::admin_client::send_admin_request_tls(
        &admin_addr.to_string(),
        r#"{"command":"status"}"#,
        &tls_connector,
    )
    .await
    .unwrap();

    assert!(resp.ok);
    assert!(resp.shards.is_some());
    let shards = resp.shards.unwrap();
    assert_eq!(shards.len(), 1);
    assert_eq!(shards[0].shard, 0);
}
