mod compact;
mod exchange;
mod healthz;
mod join;
mod leave;
mod merge;
mod register;
mod scan_changes;
mod split;

use serde::Deserialize;

/// HTTP client for the tapi shard-manager service.
///
/// Uses raw-TCP HTTP/1.1 (no external HTTP libraries).
pub struct HttpShardManagerClient {
    addr: std::net::SocketAddr,
    #[cfg(feature = "tls")]
    tls_connector: Option<crate::tls::ReloadableTlsConnector>,
    #[cfg(feature = "tls")]
    tls_server_name: Option<String>,
}

#[derive(Deserialize)]
pub(super) struct Response {
    #[serde(default)]
    pub ok: Option<bool>,
    #[serde(default)]
    pub error: Option<String>,
}

impl HttpShardManagerClient {
    pub fn new(shard_manager_url: &str) -> Self {
        let addr_str = shard_manager_url
            .strip_prefix("http://")
            .or_else(|| shard_manager_url.strip_prefix("https://"))
            .unwrap_or(shard_manager_url);
        let addr: std::net::SocketAddr = addr_str
            .parse()
            .unwrap_or_else(|e| panic!("invalid shard_manager_url '{shard_manager_url}': {e}"));
        Self {
            addr,
            #[cfg(feature = "tls")]
            tls_connector: None,
            #[cfg(feature = "tls")]
            tls_server_name: None,
        }
    }

    #[cfg(feature = "tls")]
    pub fn with_tls(
        shard_manager_url: &str,
        tls_connector: crate::tls::ReloadableTlsConnector,
        server_name: Option<String>,
    ) -> Self {
        let mut client = Self::new(shard_manager_url);
        client.tls_connector = Some(tls_connector);
        client.tls_server_name = server_name;
        client
    }
}
