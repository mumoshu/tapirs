mod handle_apply_changes;
mod handle_connection;
mod handle_request;
mod handle_scan_changes;
pub mod run;
mod serve;

pub use run::run;
pub use serve::serve;

use crate::discovery::{InMemoryShardDirectory, RemoteShardDirectory};
use crate::{ShardManager, TapirReplica, TcpAddress, TcpTransport};
use serde::Deserialize;
use std::sync::Arc;

/// Concrete `ShardManager` for `String`/`String` KV over TCP, generic over discovery backend.
pub type TapirShardManager<RD> = ShardManager<
    String,
    String,
    TcpTransport<TapirReplica<String, String>>,
    RD,
>;

/// Shared state for the shard-manager HTTP server.
pub(crate) struct ShardManagerState<RD: RemoteShardDirectory<TcpAddress, String>> {
    pub(crate) manager: tokio::sync::Mutex<TapirShardManager<RD>>,
    pub(crate) remote: Arc<RD>,
}

impl<RD: RemoteShardDirectory<TcpAddress, String>> ShardManagerState<RD> {
    pub fn new(
        rng: crate::Rng,
        remote: Arc<RD>,
        #[cfg(feature = "tls")] tls_config: &Option<crate::tls::TlsConfig>,
    ) -> Self {
        let ephemeral_addr = {
            let l = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
            let a = l.local_addr().unwrap();
            drop(l);
            TcpAddress(a)
        };
        let directory = Arc::new(InMemoryShardDirectory::new());
        #[cfg(feature = "tls")]
        let transport = if let Some(tls) = tls_config {
            TcpTransport::with_tls(ephemeral_addr, Arc::clone(&directory), tls)
                .unwrap_or_else(|e| panic!("shard-manager transport TLS error: {e}"))
        } else {
            TcpTransport::with_directory(ephemeral_addr, Arc::clone(&directory))
        };
        #[cfg(not(feature = "tls"))]
        let transport = TcpTransport::with_directory(
            ephemeral_addr,
            Arc::clone(&directory),
        );

        let manager = ShardManager::new(
            rng,
            transport,
            Arc::clone(&remote),
        );
        Self {
            manager: tokio::sync::Mutex::new(manager),
            remote,
        }
    }
}

#[derive(Deserialize)]
pub(crate) struct JoinRequest {
    pub shard: u32,
    pub listen_addr: String,
}

pub(crate) fn status_text(code: u16) -> &'static str {
    match code {
        200 => "OK",
        400 => "Bad Request",
        404 => "Not Found",
        500 => "Internal Server Error",
        503 => "Service Unavailable",
        504 => "Gateway Timeout",
        _ => "Unknown",
    }
}
