use serde::Deserialize;
use std::io::{Read, Write};
use std::net::TcpStream;

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
struct Response {
    #[serde(default)]
    ok: Option<bool>,
    #[serde(default)]
    error: Option<String>,
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

    /// Connect, optionally wrap with TLS, send request, and return the response body.
    fn exchange(&self, request: &str) -> Result<String, String> {
        let stream = TcpStream::connect(self.addr)
            .map_err(|e| format!("connect to {}: {e}", self.addr))?;
        stream
            .set_read_timeout(Some(std::time::Duration::from_secs(10)))
            .map_err(|e| format!("set timeout: {e}"))?;

        #[cfg(feature = "tls")]
        if let Some(ref connector) = self.tls_connector {
            let config = connector.client_config();
            let server_name = if let Some(ref name) = self.tls_server_name {
                rustls::pki_types::ServerName::try_from(name.as_str())
                    .map_err(|e| format!("invalid TLS server name '{name}': {e}"))?
                    .to_owned()
            } else {
                rustls::pki_types::ServerName::IpAddress(
                    rustls::pki_types::IpAddr::from(self.addr.ip()),
                )
            };
            let conn = rustls::ClientConnection::new(config, server_name)
                .map_err(|e| format!("TLS connect to {}: {e}", self.addr))?;
            let mut tls_stream = rustls::StreamOwned::new(conn, stream);
            return Self::http_exchange(&mut tls_stream, self.addr, request);
        }

        let mut stream = stream;
        Self::http_exchange(&mut stream, self.addr, request)
    }

    /// Perform the HTTP request/response exchange over a stream.
    fn http_exchange(
        stream: &mut (impl Read + Write),
        addr: std::net::SocketAddr,
        request: &str,
    ) -> Result<String, String> {
        stream
            .write_all(request.as_bytes())
            .map_err(|e| format!("send to {addr}: {e}"))?;
        let mut response = Vec::new();
        stream
            .read_to_end(&mut response)
            .map_err(|e| format!("read from {addr}: {e}"))?;
        let resp_str = String::from_utf8_lossy(&response).to_string();
        let resp_body = resp_str
            .split_once("\r\n\r\n")
            .map(|(_, b)| b.to_string())
            .unwrap_or_default();
        Ok(resp_body)
    }

    /// Send an HTTP POST and return the response body.
    fn http_post(&self, path: &str, body: &str) -> Result<String, String> {
        let request = format!(
            "POST {path} HTTP/1.1\r\nHost: {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            self.addr,
            body.len(),
        );
        self.exchange(&request)
    }

    /// Send an HTTP GET and return the response body.
    fn http_get(&self, path: &str) -> Result<String, String> {
        let request = format!(
            "GET {path} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n",
            self.addr,
        );
        self.exchange(&request)
    }

    /// Parse a standard `{"ok":true}` / `{"error":"..."}` response.
    fn parse_response(&self, body: &str) -> Result<(), String> {
        let resp: Response =
            serde_json::from_str(body).map_err(|e| format!("parse response: {e}: {body}"))?;
        if let Some(err) = resp.error {
            return Err(err);
        }
        if resp.ok == Some(true) {
            return Ok(());
        }
        Err(format!("unexpected response: {body}"))
    }

    /// Check shard-manager health via GET /healthz.
    pub fn healthz(&self) -> Result<(), String> {
        let resp = self.http_get("/healthz")?;
        self.parse_response(&resp)
    }

    #[allow(dead_code)]
    pub fn join(&self, shard: u32, listen_addr: &str) -> Result<(), String> {
        let body = serde_json::json!({
            "shard": shard,
            "listen_addr": listen_addr,
        })
        .to_string();
        let resp = self.http_post("/v1/join", &body)?;
        self.parse_response(&resp)
    }

    #[allow(dead_code)]
    pub fn leave(&self, shard: u32, listen_addr: &str) -> Result<(), String> {
        let body = serde_json::json!({
            "shard": shard,
            "listen_addr": listen_addr,
        })
        .to_string();
        let resp = self.http_post("/v1/leave", &body)?;
        self.parse_response(&resp)
    }

    #[allow(dead_code)]
    pub fn register(
        &self,
        shard: u32,
        key_range_start: Option<&str>,
        key_range_end: Option<&str>,
        replicas: Option<&[String]>,
    ) -> Result<(), String> {
        let mut json = serde_json::json!({
            "shard": shard,
            "key_range_start": key_range_start,
            "key_range_end": key_range_end,
        });
        if let Some(addrs) = replicas {
            json["replicas"] = serde_json::json!(addrs);
        }
        let body = json.to_string();
        let resp = self.http_post("/v1/register", &body)?;
        self.parse_response(&resp)
    }

    pub fn split(
        &self,
        source: u32,
        split_key: &str,
        new_shard: u32,
        new_replicas: &[String],
    ) -> Result<(), String> {
        let body = serde_json::json!({
            "source": source,
            "split_key": split_key,
            "new_shard": new_shard,
            "new_replicas": new_replicas,
        })
        .to_string();
        let resp = self.http_post("/v1/split", &body)?;
        self.parse_response(&resp)
    }

    pub fn merge(&self, absorbed: u32, surviving: u32) -> Result<(), String> {
        let body = serde_json::json!({
            "absorbed": absorbed,
            "surviving": surviving,
        })
        .to_string();
        let resp = self.http_post("/v1/merge", &body)?;
        self.parse_response(&resp)
    }

    pub fn compact(
        &self,
        source: u32,
        new_shard: u32,
        new_replicas: &[String],
    ) -> Result<(), String> {
        let body = serde_json::json!({
            "source": source,
            "new_shard": new_shard,
            "new_replicas": new_replicas,
        })
        .to_string();
        let resp = self.http_post("/v1/compact", &body)?;
        self.parse_response(&resp)
    }
}
