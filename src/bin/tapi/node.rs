use crate::config::{NodeConfig, ReplicaConfig};
use crate::discovery::HttpDiscoveryClient;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tapirs::discovery::{DiscoveryShardDirectory, InMemoryShardDirectory};
use tapirs::{
    IrMembership, IrReplica, ShardNumber, TapirReplica, TcpAddress, TcpTransport,
};

type TapirIrReplica = IrReplica<TapirReplica<String, String>, TcpTransport<TapirReplica<String, String>>>;

pub struct ReplicaHandle {
    pub replica: Arc<TapirIrReplica>,
    pub listen_addr: SocketAddr,
}

pub struct Node {
    pub replicas: Mutex<HashMap<ShardNumber, ReplicaHandle>>,
    persist_dir: String,
    directory: Arc<InMemoryShardDirectory<TcpAddress>>,
    // Holds the DiscoveryShardDirectory alive so its background sync task
    // continues running. When None, no discovery sync is active.
    _discovery_dir: Option<Arc<DiscoveryShardDirectory<TcpAddress, HttpDiscoveryClient>>>,
    shard_manager_url: Option<String>,
}

impl Node {
    pub(crate) fn new(persist_dir: String) -> Self {
        Self {
            replicas: Mutex::new(HashMap::new()),
            persist_dir,
            directory: Arc::new(InMemoryShardDirectory::new()),
            _discovery_dir: None,
            shard_manager_url: None,
        }
    }

    pub(crate) fn with_discovery(persist_dir: String, discovery_url: &str) -> Self {
        let directory = Arc::new(InMemoryShardDirectory::new());
        let client = Arc::new(HttpDiscoveryClient::new(discovery_url));
        let discovery_dir = DiscoveryShardDirectory::new(
            Arc::clone(&directory),
            client,
            std::time::Duration::from_secs(10),
        );
        Self {
            replicas: Mutex::new(HashMap::new()),
            persist_dir,
            directory,
            _discovery_dir: Some(discovery_dir),
            shard_manager_url: None,
        }
    }

    pub(crate) fn with_discovery_and_shard_manager(
        persist_dir: String,
        discovery_url: &str,
        shard_manager_url: &str,
    ) -> Self {
        let mut node = Self::with_discovery(persist_dir, discovery_url);
        node.shard_manager_url = Some(shard_manager_url.to_string());
        node
    }

    pub async fn add_replica(&self, cfg: &ReplicaConfig) {
        let shard = ShardNumber(cfg.shard);
        let listen_addr: SocketAddr = cfg
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
        let transport =
            TcpTransport::with_directory(address, persist_dir, Arc::clone(&self.directory));

        // Populate shard directory so TapirTransport::shard_addresses works.
        transport.set_shard_addresses(shard, membership.clone());

        // If discovery is configured, register this shard for push.
        if let Some(ref dir) = self._discovery_dir {
            dir.add_own_shard(shard);
        }

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

    /// Create a replica and coordinate bootstrap/join via the shard-manager.
    ///
    /// 1. Create a local replica with membership=[self].
    /// 2. POST /v1/join to the shard-manager, which decides whether to
    ///    bootstrap (first replica) or join (subsequent).
    pub async fn create_replica(
        &self,
        shard: ShardNumber,
        listen_addr: SocketAddr,
    ) -> Result<(), String> {
        let cfg = ReplicaConfig {
            shard: shard.0,
            listen_addr: listen_addr.to_string(),
            membership: vec![listen_addr.to_string()],
        };
        self.add_replica(&cfg).await;

        // Shard-manager handles both bootstrap (first replica) and join (subsequent).
        self.shard_manager_join(shard, listen_addr).await?;
        Ok(())
    }

    async fn shard_manager_join(
        &self,
        shard: ShardNumber,
        listen_addr: SocketAddr,
    ) -> Result<(), String> {
        let url = self
            .shard_manager_url
            .as_ref()
            .ok_or_else(|| "no shard-manager-url configured".to_string())?;

        let addr_str = url
            .strip_prefix("http://")
            .unwrap_or(url);
        let addr: SocketAddr = addr_str
            .parse()
            .map_err(|e| format!("invalid shard-manager-url '{url}': {e}"))?;

        let body = serde_json::to_string(&serde_json::json!({
            "shard": shard.0,
            "listen_addr": listen_addr.to_string(),
        }))
        .map_err(|e| format!("serialize join request: {e}"))?;

        let request = format!(
            "POST /v1/join HTTP/1.1\r\nHost: {addr_str}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len(),
        );

        let mut stream = tokio::net::TcpStream::connect(addr)
            .await
            .map_err(|e| format!("connect to shard-manager at {addr}: {e}"))?;

        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        stream
            .write_all(request.as_bytes())
            .await
            .map_err(|e| format!("send join request: {e}"))?;

        let mut response = Vec::new();
        stream
            .read_to_end(&mut response)
            .await
            .map_err(|e| format!("read join response: {e}"))?;

        let resp_str = String::from_utf8_lossy(&response);
        let resp_body = resp_str
            .split_once("\r\n\r\n")
            .map(|(_, b)| b)
            .unwrap_or("");

        // Parse HTTP status line for error detection.
        let status_ok = resp_str
            .lines()
            .next()
            .map(|line| line.contains("200"))
            .unwrap_or(false);

        if !status_ok {
            // Try to extract error message from JSON body.
            #[derive(serde::Deserialize)]
            struct ErrResp {
                error: String,
            }
            if let Ok(err) = serde_json::from_str::<ErrResp>(resp_body) {
                return Err(err.error);
            }
            return Err(format!("shard-manager returned error (body: {resp_body})"));
        }

        Ok(())
    }

    pub fn remove_replica(&self, shard: ShardNumber) -> bool {
        let removed = self.replicas.lock().unwrap().remove(&shard);
        if removed.is_some() {
            // Remove from discovery push list. Discovery deregistration
            // happens on the next background sync cycle.
            if let Some(ref dir) = self._discovery_dir {
                dir.remove_own_shard(shard);
            }
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

    pub fn shard_list(&self) -> Vec<(ShardNumber, SocketAddr)> {
        self.replicas
            .lock()
            .unwrap()
            .iter()
            .map(|(shard, handle)| (*shard, handle.listen_addr))
            .collect()
    }
}

pub async fn run(cfg: NodeConfig) {
    let persist_dir = cfg
        .persist_dir
        .unwrap_or_else(|| "/tmp/tapi".to_string());
    let admin_listen_addr = cfg
        .admin_listen_addr
        .unwrap_or_else(|| "127.0.0.1:9000".to_string());

    let node = match (&cfg.discovery_url, &cfg.shard_manager_url) {
        (Some(disc), Some(mgr)) => {
            Arc::new(Node::with_discovery_and_shard_manager(persist_dir, disc, mgr))
        }
        (Some(disc), None) => Arc::new(Node::with_discovery(persist_dir, disc)),
        _ => Arc::new(Node::new(persist_dir)),
    };

    for replica_cfg in &cfg.replicas {
        node.add_replica(replica_cfg).await;
    }

    let admin_addr: SocketAddr = admin_listen_addr
        .parse()
        .unwrap_or_else(|e| panic!("invalid admin_listen_addr '{admin_listen_addr}': {e}"));

    crate::admin_server::start(admin_addr, Arc::clone(&node)).await;

    tracing::info!(%admin_listen_addr, "node ready, press Ctrl-C to stop");

    tokio::signal::ctrl_c()
        .await
        .expect("failed to listen for Ctrl-C");
    tracing::info!("shutting down");
}
