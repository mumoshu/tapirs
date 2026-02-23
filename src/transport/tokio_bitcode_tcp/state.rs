use super::address::TcpAddress;
use super::wire::TcpIrMessage;
use crate::discovery::{InMemoryShardDirectory, ShardDirectory as _};
use crate::ir::ReplicaUpcalls;
use crate::{IrMembership, ShardNumber};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex, RwLock};
use tokio::sync::{mpsc, oneshot};

/// Tokio-based TCP transport with bitcode encoding.
pub struct TcpTransport<U: ReplicaUpcalls> {
    pub(super) address: TcpAddress,
    pub(super) inner: Arc<TransportInner<U>>,
}

impl<U: ReplicaUpcalls> Clone for TcpTransport<U> {
    fn clone(&self) -> Self {
        Self {
            address: self.address,
            inner: Arc::clone(&self.inner),
        }
    }
}

/// Shared transport state behind Arc.
pub(super) struct TransportInner<U: ReplicaUpcalls> {
    /// Pending request-reply oneshots, keyed by request_id.
    pub pending_replies: Mutex<HashMap<u64, oneshot::Sender<TcpIrMessage<U>>>>,
    /// Monotonically increasing request ID counter.
    pub next_request_id: AtomicU64,
    /// Outbound connection pool: one mpsc sender per peer.
    pub connections: Mutex<HashMap<SocketAddr, mpsc::Sender<Vec<u8>>>>,
    /// Receive callback for inbound messages (set when hosting a replica).
    #[allow(clippy::type_complexity)]
    pub receive_callback: Mutex<
        Option<
            Arc<dyn Fn(TcpAddress, TcpIrMessage<U>) -> Option<TcpIrMessage<U>> + Send + Sync>,
        >,
    >,
    /// Shard directory for TapirTransport::shard_addresses().
    pub directory: Arc<InMemoryShardDirectory<TcpAddress>>,
    /// Shard number for this transport's replica group, set by set_shard_addresses().
    pub shard: RwLock<Option<ShardNumber>>,
    /// Directory for persistent state files.
    pub persist_dir: String,
    /// Optional TLS acceptor for inbound connections.
    #[cfg(feature = "tls")]
    pub tls_acceptor: Option<crate::tls::ReloadableTlsAcceptor>,
    /// Optional TLS connector for outbound connections.
    #[cfg(feature = "tls")]
    pub tls_connector: Option<crate::tls::ReloadableTlsConnector>,
}

impl<U: ReplicaUpcalls> TcpTransport<U> {
    pub fn with_directory(
        address: TcpAddress,
        persist_dir: String,
        directory: Arc<InMemoryShardDirectory<TcpAddress>>,
    ) -> Self {
        Self {
            address,
            inner: Arc::new(TransportInner {
                pending_replies: Mutex::new(HashMap::new()),
                next_request_id: AtomicU64::new(0),
                connections: Mutex::new(HashMap::new()),
                receive_callback: Mutex::new(None),
                directory,
                shard: RwLock::new(None),
                persist_dir,
                #[cfg(feature = "tls")]
                tls_acceptor: None,
                #[cfg(feature = "tls")]
                tls_connector: None,
            }),
        }
    }

    /// Configure TLS for both inbound (acceptor) and outbound (connector) connections.
    #[cfg(feature = "tls")]
    pub fn with_tls(
        address: TcpAddress,
        persist_dir: String,
        directory: Arc<InMemoryShardDirectory<TcpAddress>>,
        tls_config: &crate::tls::TlsConfig,
    ) -> Result<Self, crate::tls::TlsError> {
        let acceptor = crate::tls::ReloadableTlsAcceptor::new(tls_config)?;
        let connector = crate::tls::ReloadableTlsConnector::new(tls_config)?;
        crate::tls::spawn_reload_task(
            tls_config.clone(),
            Some(acceptor.swap_target().clone()),
            Some(connector.swap_target().clone()),
        );
        Ok(Self {
            address,
            inner: Arc::new(TransportInner {
                pending_replies: Mutex::new(HashMap::new()),
                next_request_id: AtomicU64::new(0),
                connections: Mutex::new(HashMap::new()),
                receive_callback: Mutex::new(None),
                directory,
                shard: RwLock::new(None),
                persist_dir,
                tls_acceptor: Some(acceptor),
                tls_connector: Some(connector),
            }),
        })
    }

    pub fn directory(&self) -> &Arc<InMemoryShardDirectory<TcpAddress>> {
        &self.inner.directory
    }

    /// Register the replica's receive callback. Called before `listen()`.
    pub fn set_receive_callback(
        &self,
        cb: impl Fn(TcpAddress, TcpIrMessage<U>) -> Option<TcpIrMessage<U>> + Send + Sync + 'static,
    ) {
        *self.inner.receive_callback.lock().unwrap() = Some(Arc::new(cb));
    }

    /// Populate the shard directory with membership information.
    /// Also stores the shard number so on_membership_changed can update the right entry.
    pub fn set_shard_addresses(&self, shard: ShardNumber, membership: IrMembership<TcpAddress>) {
        *self.inner.shard.write().unwrap() = Some(shard);
        self.inner.directory.put(shard, membership, 0);
    }
}
