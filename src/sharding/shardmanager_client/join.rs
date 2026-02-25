impl super::HttpShardManagerClient {
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
}
