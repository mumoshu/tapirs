use serde::Deserialize;
use std::io::{Read, Write};
use std::net::TcpStream;

/// HTTP client for the tapi shard-manager service.
///
/// Follows the same raw-TCP HTTP/1.1 pattern as `HttpDiscoveryClient`
/// in `src/bin/tapi/discovery.rs`. No external HTTP libraries.
pub struct HttpShardManagerClient {
    addr: std::net::SocketAddr,
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
            .unwrap_or(shard_manager_url);
        let addr: std::net::SocketAddr = addr_str
            .parse()
            .unwrap_or_else(|e| panic!("invalid shard_manager_url '{shard_manager_url}': {e}"));
        Self { addr }
    }

    /// Send an HTTP POST and return the response body.
    fn http_post(&self, path: &str, body: &str) -> Result<String, String> {
        let mut stream = TcpStream::connect(self.addr)
            .map_err(|e| format!("connect to {}: {e}", self.addr))?;
        let request = format!(
            "POST {path} HTTP/1.1\r\nHost: {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            self.addr,
            body.len(),
        );
        stream
            .write_all(request.as_bytes())
            .map_err(|e| format!("send to {}: {e}", self.addr))?;
        let mut response = Vec::new();
        stream
            .read_to_end(&mut response)
            .map_err(|e| format!("read from {}: {e}", self.addr))?;
        let resp_str = String::from_utf8_lossy(&response).to_string();
        let body = resp_str
            .split_once("\r\n\r\n")
            .map(|(_, b)| b.to_string())
            .unwrap_or_default();
        Ok(body)
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

    pub fn join(&self, shard: u32, listen_addr: &str) -> Result<(), String> {
        let body = serde_json::json!({
            "shard": shard,
            "listen_addr": listen_addr,
        })
        .to_string();
        let resp = self.http_post("/v1/join", &body)?;
        self.parse_response(&resp)
    }

    pub fn leave(&self, shard: u32, listen_addr: &str) -> Result<(), String> {
        let body = serde_json::json!({
            "shard": shard,
            "listen_addr": listen_addr,
        })
        .to_string();
        let resp = self.http_post("/v1/leave", &body)?;
        self.parse_response(&resp)
    }

    pub fn register(
        &self,
        shard: u32,
        key_range_start: Option<&str>,
        key_range_end: Option<&str>,
    ) -> Result<(), String> {
        let body = serde_json::json!({
            "shard": shard,
            "key_range_start": key_range_start,
            "key_range_end": key_range_end,
        })
        .to_string();
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
