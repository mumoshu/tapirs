use std::io::{Read, Write};
use std::net::TcpStream;

use super::Response;

impl super::HttpShardManagerClient {
    /// Connect, optionally wrap with TLS, send request, and return the response body.
    pub(super) fn exchange(&self, request: &str) -> Result<String, String> {
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
    pub(super) fn http_exchange(
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
    pub(super) fn http_post(&self, path: &str, body: &str) -> Result<String, String> {
        let request = format!(
            "POST {path} HTTP/1.1\r\nHost: {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            self.addr,
            body.len(),
        );
        self.exchange(&request)
    }

    /// Send an HTTP GET and return the response body.
    pub(super) fn http_get(&self, path: &str) -> Result<String, String> {
        let request = format!(
            "GET {path} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n",
            self.addr,
        );
        self.exchange(&request)
    }

    /// Like [`exchange`] but returns raw body bytes instead of a String.
    ///
    /// Splits the HTTP response at the `\r\n\r\n` header/body boundary using
    /// byte search (not String conversion), preserving binary body content.
    pub(super) fn exchange_raw(&self, request: &str) -> Result<(String, Vec<u8>), String> {
        let stream = TcpStream::connect(self.addr)
            .map_err(|e| format!("connect to {}: {e}", self.addr))?;
        stream
            .set_read_timeout(Some(std::time::Duration::from_secs(30)))
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
            return Self::http_exchange_raw(&mut tls_stream, self.addr, request);
        }

        let mut stream = stream;
        Self::http_exchange_raw(&mut stream, self.addr, request)
    }

    /// Perform the HTTP request/response exchange over a stream, returning
    /// headers as String and body as raw bytes.
    fn http_exchange_raw(
        stream: &mut (impl Read + Write),
        addr: std::net::SocketAddr,
        request: &str,
    ) -> Result<(String, Vec<u8>), String> {
        stream
            .write_all(request.as_bytes())
            .map_err(|e| format!("send to {addr}: {e}"))?;
        let mut response = Vec::new();
        stream
            .read_to_end(&mut response)
            .map_err(|e| format!("read from {addr}: {e}"))?;

        // Split at \r\n\r\n boundary.
        let separator = b"\r\n\r\n";
        let split_pos = response
            .windows(separator.len())
            .position(|w| w == separator);

        match split_pos {
            Some(pos) => {
                let headers = String::from_utf8_lossy(&response[..pos]).to_string();
                let body = response[pos + separator.len()..].to_vec();
                Ok((headers, body))
            }
            None => {
                // No header/body separator found — return empty headers and all as body.
                Ok((String::new(), response))
            }
        }
    }

    /// Send an HTTP POST with custom headers and a raw binary body.
    pub(super) fn http_post_binary(
        &self,
        path: &str,
        extra_headers: &[(&str, &str)],
        body: &[u8],
    ) -> Result<String, String> {
        let mut request = format!(
            "POST {path} HTTP/1.1\r\nHost: {}\r\nContent-Length: {}\r\nConnection: close\r\n",
            self.addr,
            body.len(),
        );
        for (name, value) in extra_headers {
            request.push_str(&format!("{name}: {value}\r\n"));
        }
        request.push_str("\r\n");

        let stream = TcpStream::connect(self.addr)
            .map_err(|e| format!("connect to {}: {e}", self.addr))?;
        stream
            .set_read_timeout(Some(std::time::Duration::from_secs(30)))
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
            tls_stream
                .write_all(request.as_bytes())
                .map_err(|e| format!("send headers to {}: {e}", self.addr))?;
            tls_stream
                .write_all(body)
                .map_err(|e| format!("send body to {}: {e}", self.addr))?;
            let mut response = Vec::new();
            tls_stream
                .read_to_end(&mut response)
                .map_err(|e| format!("read from {}: {e}", self.addr))?;
            let resp_str = String::from_utf8_lossy(&response).to_string();
            let resp_body = resp_str
                .split_once("\r\n\r\n")
                .map(|(_, b)| b.to_string())
                .unwrap_or_default();
            return Ok(resp_body);
        }

        let mut stream = stream;
        stream
            .write_all(request.as_bytes())
            .map_err(|e| format!("send headers to {}: {e}", self.addr))?;
        stream
            .write_all(body)
            .map_err(|e| format!("send body to {}: {e}", self.addr))?;
        let mut response = Vec::new();
        stream
            .read_to_end(&mut response)
            .map_err(|e| format!("read from {}: {e}", self.addr))?;
        let resp_str = String::from_utf8_lossy(&response).to_string();
        let resp_body = resp_str
            .split_once("\r\n\r\n")
            .map(|(_, b)| b.to_string())
            .unwrap_or_default();
        Ok(resp_body)
    }

    /// Parse a standard `{"ok":true}` / `{"error":"..."}` response.
    pub(super) fn parse_response(&self, body: &str) -> Result<(), String> {
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
}
