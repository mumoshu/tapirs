use crate::config::{NodeConfig, ReplicaConfig};
use crate::discovery::HttpDiscoveryClient;
use rand::{thread_rng, Rng as _};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tapirs::discovery::{DiscoveryShardDirectory, InMemoryShardDirectory};
use tapirs::{
    IrClient, IrMembership, IrRecord, IrReplica, IrSharedView, IrView, IrViewNumber,
    ShardNumber, TapirReplica, TcpAddress, TcpTransport,
};

fn production_rng() -> tapirs::Rng {
    tapirs::Rng::from_seed(thread_rng().r#gen())
}

type TapirIrReplica = IrReplica<TapirReplica<String, String>, TcpTransport<TapirReplica<String, String>>>;

pub struct ReplicaHandle {
    pub replica: Arc<TapirIrReplica>,
    pub listen_addr: SocketAddr,
}

/// A shard backup containing the IR record and view.
///
/// The record holds all IR operations (consensus: Prepare with results;
/// inconsistent: Commit/Abort). When fed to a fresh replica via
/// IrClient::bootstrap_record() → BootstrapRecord → StartView → sync(),
/// TAPIR replays all operations to reconstruct OCC + MVCC state.
///
/// The backup reflects state as of the last completed view change.
/// In TAPIR's leaderless design, the IR record is NOT consistent
/// across intra-shard replicas until a view change merges their
/// divergent records. Operations committed after the last view change
/// may not be captured. Force a view change before backup to ensure
/// the most up-to-date state.
#[derive(Serialize, Deserialize)]
pub struct ShardBackup {
    pub record: IrRecord<TapirReplica<String, String>>,
    pub view: IrSharedView<TcpAddress>,
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

    pub async fn add_replica(&self, cfg: &ReplicaConfig) -> Result<(), String> {
        let shard = ShardNumber(cfg.shard);
        let listen_addr: SocketAddr = cfg
            .listen_addr
            .parse()
            .map_err(|e| format!("invalid listen_addr '{}': {e}", cfg.listen_addr))?;

        let membership_addrs: Vec<TcpAddress> = cfg
            .membership
            .iter()
            .map(|a| {
                a.parse()
                    .map(TcpAddress)
                    .map_err(|e| format!("invalid membership addr '{a}': {e}"))
            })
            .collect::<Result<Vec<_>, _>>()?;
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
            .map_err(|e| format!("failed to listen on {listen_addr}: {e}"))?;

        let transport_for_replica = transport.clone();
        let replica = Arc::new_cyclic(|weak: &std::sync::Weak<TapirIrReplica>| {
            let weak = weak.clone();
            transport_for_replica.set_receive_callback(move |from, message| {
                weak.upgrade()?.receive(from, message)
            });
            let upcalls = TapirReplica::new(shard, false);
            IrReplica::with_view_change_interval(
                production_rng(),
                membership,
                upcalls,
                transport_for_replica.clone(),
                Some(TapirReplica::tick),
                Some(Duration::from_secs(10)),
            )
        });

        tracing::info!(?shard, %listen_addr, "replica started");

        self.replicas.lock().unwrap().insert(
            shard,
            ReplicaHandle {
                replica,
                listen_addr,
            },
        );

        Ok(())
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
        storage: &str,
    ) -> Result<(), String> {
        if storage == "disk" {
            return Err(
                "disk storage backend is not yet available; use --storage memory (default)"
                    .to_string(),
            );
        }

        let cfg = ReplicaConfig {
            shard: shard.0,
            listen_addr: listen_addr.to_string(),
            membership: vec![listen_addr.to_string()],
        };
        self.add_replica(&cfg).await?;

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

    /// Coordinate removing this node's replica from a shard via the shard-manager.
    ///
    /// Protocol flow:
    ///   1. POST /v1/leave { shard, listen_addr } to shard manager
    ///   2. Shard manager queries discovery, verifies addr is in shard
    ///   3. ShardManager::leave creates ShardClient with discovery membership
    ///   4. Broadcasts RemoveMember(addr) to all replicas
    ///   5. Replicas remove addr from membership, enter ViewChanging, view += 3
    ///   6. DoViewChange → leader collects f+1 addenda → StartView
    ///   7. Remaining replicas transition to Normal with clean membership
    ///   8. Removed replica is orphaned — safe to drop via remove_replica()
    ///
    /// After this returns, call `remove_replica(shard)` to drop the local handle.
    pub async fn leave_shard(&self, shard: ShardNumber) -> Result<(), String> {
        let listen_addr = {
            let replicas = self.replicas.lock().unwrap();
            let handle = replicas
                .get(&shard)
                .ok_or_else(|| format!("shard {shard:?} not found on this node"))?;
            handle.listen_addr
        };
        self.shard_manager_leave(shard, listen_addr).await
    }

    async fn shard_manager_leave(
        &self,
        shard: ShardNumber,
        listen_addr: SocketAddr,
    ) -> Result<(), String> {
        let url = self
            .shard_manager_url
            .as_ref()
            .ok_or_else(|| "no shard-manager-url configured".to_string())?;

        let addr_str = url.strip_prefix("http://").unwrap_or(url);
        let addr: SocketAddr = addr_str
            .parse()
            .map_err(|e| format!("invalid shard-manager-url '{url}': {e}"))?;

        let body = serde_json::to_string(&serde_json::json!({
            "shard": shard.0,
            "listen_addr": listen_addr.to_string(),
        }))
        .map_err(|e| format!("serialize leave request: {e}"))?;

        let request = format!(
            "POST /v1/leave HTTP/1.1\r\nHost: {addr_str}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len(),
        );

        let mut stream = tokio::net::TcpStream::connect(addr)
            .await
            .map_err(|e| format!("connect to shard-manager at {addr}: {e}"))?;

        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        stream
            .write_all(request.as_bytes())
            .await
            .map_err(|e| format!("send leave request: {e}"))?;

        let mut response = Vec::new();
        stream
            .read_to_end(&mut response)
            .await
            .map_err(|e| format!("read leave response: {e}"))?;

        let resp_str = String::from_utf8_lossy(&response);
        let resp_body = resp_str
            .split_once("\r\n\r\n")
            .map(|(_, b)| b)
            .unwrap_or("");

        let status_ok = resp_str
            .lines()
            .next()
            .map(|line| line.contains("200"))
            .unwrap_or(false);

        if !status_ok {
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

    #[allow(clippy::disallowed_methods)] // output order unspecified; used for display only
    pub fn shard_list(&self) -> Vec<(ShardNumber, SocketAddr)> {
        self.replicas
            .lock()
            .unwrap()
            .iter()
            .map(|(shard, handle)| (*shard, handle.listen_addr))
            .collect()
    }

    /// Take a backup of a shard by fetching the leader_record from
    /// the local replica.
    ///
    /// Returns the IR record and view from the last completed view change.
    /// Force a view change before backup if the most up-to-date state is
    /// needed — view change is TAPIR's synchronization mechanism.
    pub async fn backup_shard(&self, shard: ShardNumber) -> Option<ShardBackup> {
        let (transport, addr) = {
            let replicas = self.replicas.lock().unwrap();
            let handle = replicas.get(&shard)?;
            (handle.replica.transport().clone(), handle.listen_addr)
        };
        let client = IrClient::new(
            production_rng(),
            IrMembership::new(vec![TcpAddress(addr)]),
            transport,
        );
        let (view, record) = client.fetch_leader_record().await?;
        Some(ShardBackup {
            record: (*record).clone(),
            view,
        })
    }

    /// Restore a shard from backup onto this node.
    ///
    /// Creates a fresh replica at listen_addr, then sends a BootstrapRecord
    /// with the backup data via IrClient::bootstrap_record(). The replica
    /// converts it to a self-directed StartView, and TAPIR's sync() replays
    /// all operations from the record to reconstruct the OCC+MVCC state.
    ///
    /// For multi-replica restore: call on each node with the same
    /// new_membership list, then register with discovery.
    pub async fn restore_shard(
        &self,
        shard: ShardNumber,
        listen_addr: SocketAddr,
        backup: &ShardBackup,
        new_membership: Vec<SocketAddr>,
    ) -> Result<(), String> {
        // Create fresh replica with membership=[self].
        let cfg = ReplicaConfig {
            shard: shard.0,
            listen_addr: listen_addr.to_string(),
            membership: vec![listen_addr.to_string()],
        };
        self.add_replica(&cfg).await?;

        // Build restore view: new membership, advanced view number,
        // preserved app_config (shard key ranges).
        let restore_view = IrSharedView::new(IrView {
            membership: IrMembership::new(
                new_membership.into_iter().map(TcpAddress).collect(),
            ),
            number: IrViewNumber(backup.view.number.0 + 10),
            app_config: backup.view.app_config.clone(),
        });

        // Clone the replica's transport and create a temporary IrClient
        // to send BootstrapRecord — same pattern as ShardManager::bootstrap()
        // in shard_manager_catchup.rs.
        let transport = {
            let replicas = self.replicas.lock().unwrap();
            let handle = replicas.get(&shard)
                .ok_or_else(|| format!("shard {shard:?} not found after creation"))?;
            handle.replica.transport().clone()
        };
        let client = IrClient::new(
            production_rng(),
            IrMembership::new(vec![TcpAddress(listen_addr)]),
            transport,
        );
        client.bootstrap_record(backup.record.clone(), restore_view);

        if let Some(ref dir) = self._discovery_dir {
            dir.add_own_shard(shard);
        }

        tracing::info!(?shard, %listen_addr, "shard restored from backup");
        Ok(())
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
        node.add_replica(replica_cfg).await.unwrap();
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
