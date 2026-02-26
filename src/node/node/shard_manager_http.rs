use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

impl Node {
    /// Send an HTTP POST to the shard-manager with retry for transient errors.
    ///
    /// Retries up to 5 times with exponential backoff (100ms base, 2x) when
    /// the shard-manager returns a transient error such as "not found in
    /// discovery" (async FINALIZE propagation delay) or "discovery unavailable"
    /// (transient connection issue to the discovery tier).
    pub(crate) async fn shard_manager_http_post(
        &self,
        path: &str,
        shard: ShardNumber,
        listen_addr: SocketAddr,
    ) -> Result<(), String> {
        let mut backoff = std::time::Duration::from_millis(100);

        for attempt in 0..5u32 {
            match self.shard_manager_http_post_once(path, shard, listen_addr).await {
                Ok(()) => return Ok(()),
                Err(e) if Self::is_retryable_shard_manager_error(&e) && attempt < 4 => {
                    tracing::debug!(attempt, %e, "shard-manager {path} transient error, retrying");
                    tokio::time::sleep(backoff).await;
                    backoff *= 2;
                }
                Err(e) => return Err(e),
            }
        }
        unreachable!()
    }

    /// Single-attempt HTTP POST to the shard-manager.
    async fn shard_manager_http_post_once(
        &self,
        path: &str,
        shard: ShardNumber,
        listen_addr: SocketAddr,
    ) -> Result<(), String> {
        let url = self
            .shard_manager_url
            .as_ref()
            .ok_or_else(|| "no shard-manager-url configured".to_string())?;

        let (host_port, _is_https) = if let Some(hp) = url.strip_prefix("https://") {
            (hp, true)
        } else if let Some(hp) = url.strip_prefix("http://") {
            (hp, false)
        } else {
            (url.as_str(), false)
        };

        let body = serde_json::to_string(&serde_json::json!({
            "shard": shard.0,
            "listen_addr": listen_addr.to_string(),
        }))
        .map_err(|e| format!("serialize {path} request: {e}"))?;

        let request = format!(
            "POST {path} HTTP/1.1\r\nHost: {host_port}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len(),
        );

        // Use string-based connect to support both IP:port and hostname:port.
        let tcp_stream = tokio::net::TcpStream::connect(host_port)
            .await
            .map_err(|e| format!("connect to shard-manager at {host_port}: {e}"))?;

        #[cfg(feature = "tls")]
        let response = if _is_https {
            let tls_config = self.tls_config.as_ref()
                .ok_or_else(|| "https:// shard-manager URL requires TLS config (--tls-cert/--tls-key/--tls-ca)".to_string())?;
            let connector = crate::tls::ReloadableTlsConnector::new(tls_config)
                .map_err(|e| format!("TLS connector: {e}"))?;
            let host = host_port.rsplit_once(':').map(|(h, _)| h).unwrap_or(host_port);
            let server_name = rustls::pki_types::ServerName::try_from(host.to_string())
                .map_err(|e| format!("invalid TLS server name '{host}': {e}"))?;
            let mut tls_stream = connector.connector()
                .connect(server_name, tcp_stream).await
                .map_err(|e| format!("TLS handshake with shard-manager: {e}"))?;
            tls_stream.write_all(request.as_bytes()).await
                .map_err(|e| format!("send {path}: {e}"))?;
            let mut resp = Vec::new();
            tls_stream.read_to_end(&mut resp).await
                .map_err(|e| format!("read {path}: {e}"))?;
            resp
        } else {
            let mut stream = tcp_stream;
            stream.write_all(request.as_bytes()).await
                .map_err(|e| format!("send {path}: {e}"))?;
            let mut resp = Vec::new();
            stream.read_to_end(&mut resp).await
                .map_err(|e| format!("read {path}: {e}"))?;
            resp
        };

        #[cfg(not(feature = "tls"))]
        let response = {
            let _ = _is_https;
            let mut stream = tcp_stream;
            stream.write_all(request.as_bytes()).await
                .map_err(|e| format!("send {path}: {e}"))?;
            let mut resp = Vec::new();
            stream.read_to_end(&mut resp).await
                .map_err(|e| format!("read {path}: {e}"))?;
            resp
        };

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
            return Err(format!("shard-manager error on {path}: {resp_body}"));
        }

        Ok(())
    }

    /// Check if a shard-manager error is transient and worth retrying.
    fn is_retryable_shard_manager_error(err: &str) -> bool {
        err.contains("not found in discovery")
            || err.contains("discovery unavailable")
            || err.contains("PrepareConflict")
    }

    pub(crate) async fn shard_manager_join(
        &self,
        shard: ShardNumber,
        listen_addr: SocketAddr,
    ) -> Result<(), String> {
        self.shard_manager_http_post("/v1/join", shard, listen_addr).await
    }

    pub(crate) async fn shard_manager_leave(
        &self,
        shard: ShardNumber,
        listen_addr: SocketAddr,
    ) -> Result<(), String> {
        self.shard_manager_http_post("/v1/leave", shard, listen_addr).await
    }
}
