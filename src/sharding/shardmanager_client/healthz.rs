impl super::HttpShardManagerClient {
    /// Check shard-manager health via GET /healthz.
    pub fn healthz(&self) -> Result<(), String> {
        let resp = self.http_get("/healthz")?;
        self.parse_response(&resp)
    }
}
