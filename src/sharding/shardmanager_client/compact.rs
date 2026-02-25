impl super::HttpShardManagerClient {
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
