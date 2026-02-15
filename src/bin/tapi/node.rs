use crate::config::{NodeConfig, ReplicaConfig};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tapirs::{
    IrMembership, IrReplica, ShardNumber, TapirReplica, TcpAddress, TcpTransport,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

type TapirIrReplica = IrReplica<TapirReplica<String, String>, TcpTransport<TapirReplica<String, String>>>;

pub struct ReplicaHandle {
    pub replica: Arc<TapirIrReplica>,
    pub listen_addr: std::net::SocketAddr,
}

pub struct Node {
    pub replicas: Mutex<HashMap<ShardNumber, ReplicaHandle>>,
    persist_dir: String,
}

impl Node {
    fn new(persist_dir: String) -> Self {
        Self {
            replicas: Mutex::new(HashMap::new()),
            persist_dir,
        }
    }

    pub async fn add_replica(&self, cfg: &ReplicaConfig) {
        let shard = ShardNumber(cfg.shard);
        let listen_addr: std::net::SocketAddr = cfg
            .listen_addr
            .parse()
            .unwrap_or_else(|e| panic!("invalid listen_addr '{}': {e}", cfg.listen_addr));

        let membership_addrs: Vec<TcpAddress> = cfg
            .membership
            .iter()
            .map(|a| {
                TcpAddress(
                    a.parse()
                        .unwrap_or_else(|e| panic!("invalid membership addr '{a}': {e}")),
                )
            })
            .collect();
        let membership = IrMembership::new(membership_addrs);

        let persist_dir = format!("{}/shard_{}", self.persist_dir, cfg.shard);
        let address = TcpAddress(listen_addr);
        let transport = TcpTransport::new(address, persist_dir);

        // Populate shard directory so TapirTransport::shard_addresses works.
        transport.set_shard_addresses(shard, membership.clone());

        // Start listener BEFORE creating replica (IrReplica::new starts tick tasks).
        transport
            .listen(listen_addr)
            .await
            .unwrap_or_else(|e| panic!("failed to listen on {listen_addr}: {e}"));

        let transport_for_replica = transport.clone();
        let replica = Arc::new_cyclic(|weak: &std::sync::Weak<TapirIrReplica>| {
            let weak = weak.clone();
            transport_for_replica.set_receive_callback(move |from, message| {
                weak.upgrade()?.receive(from, message)
            });
            let upcalls = TapirReplica::new(shard, false);
            IrReplica::new(membership, upcalls, transport_for_replica.clone(), Some(TapirReplica::tick))
        });

        tracing::info!(?shard, %listen_addr, "replica started");

        self.replicas.lock().unwrap().insert(
            shard,
            ReplicaHandle {
                replica,
                listen_addr,
            },
        );
    }

    pub fn remove_replica(&self, shard: ShardNumber) -> bool {
        let removed = self.replicas.lock().unwrap().remove(&shard);
        if removed.is_some() {
            tracing::info!(?shard, "replica removed");
            true
        } else {
            false
        }
    }

    pub fn force_view_change(&self, shard: ShardNumber) -> bool {
        let replicas = self.replicas.lock().unwrap();
        if let Some(handle) = replicas.get(&shard) {
            handle.replica.force_view_change();
            tracing::info!(?shard, "view change triggered");
            true
        } else {
            false
        }
    }

    pub fn shard_list(&self) -> Vec<(ShardNumber, std::net::SocketAddr)> {
        self.replicas
            .lock()
            .unwrap()
            .iter()
            .map(|(shard, handle)| (*shard, handle.listen_addr))
            .collect()
    }
}

async fn register_with_discovery(replicas: &[ReplicaConfig], discovery_url: &str) {
    // Parse "http://host:port" to get the socket address.
    let addr_str = discovery_url
        .strip_prefix("http://")
        .unwrap_or(discovery_url);
    let addr: std::net::SocketAddr = match addr_str.parse() {
        Ok(a) => a,
        Err(e) => {
            tracing::warn!("invalid discovery_url '{discovery_url}': {e}");
            return;
        }
    };

    for cfg in replicas {
        let body = serde_json::to_string(&serde_json::json!({
            "replicas": cfg.membership
        }))
        .unwrap();
        let request = format!(
            "POST /v1/shards/{} HTTP/1.1\r\nHost: {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            cfg.shard, addr_str, body.len(), body,
        );

        match tokio::net::TcpStream::connect(addr).await {
            Ok(mut stream) => {
                if stream.write_all(request.as_bytes()).await.is_err() {
                    tracing::warn!(shard = cfg.shard, "failed to write to discovery service");
                    continue;
                }
                let mut response = Vec::new();
                let _ = stream.read_to_end(&mut response).await;
                let resp_str = String::from_utf8_lossy(&response);
                if resp_str.contains("200 OK") {
                    tracing::info!(shard = cfg.shard, "registered with discovery service");
                } else {
                    tracing::warn!(shard = cfg.shard, response = %resp_str, "discovery registration returned non-success");
                }
            }
            Err(e) => {
                tracing::warn!(shard = cfg.shard, %e, "failed to connect to discovery service");
            }
        }
    }
}

pub async fn run(cfg: NodeConfig) {
    let persist_dir = cfg
        .persist_dir
        .unwrap_or_else(|| "/tmp/tapi".to_string());
    let admin_listen_addr = cfg
        .admin_listen_addr
        .unwrap_or_else(|| "127.0.0.1:9000".to_string());

    let node = Arc::new(Node::new(persist_dir));

    for replica_cfg in &cfg.replicas {
        node.add_replica(replica_cfg).await;
    }

    let admin_addr: std::net::SocketAddr = admin_listen_addr
        .parse()
        .unwrap_or_else(|e| panic!("invalid admin_listen_addr '{admin_listen_addr}': {e}"));

    crate::admin_server::start(admin_addr, Arc::clone(&node)).await;

    if let Some(ref discovery_url) = cfg.discovery_url {
        register_with_discovery(&cfg.replicas, discovery_url).await;
    }

    tracing::info!(%admin_listen_addr, "node ready, press Ctrl-C to stop");

    tokio::signal::ctrl_c()
        .await
        .expect("failed to listen for Ctrl-C");
    tracing::info!("shutting down");
}
