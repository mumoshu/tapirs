impl super::HttpShardManagerClient {
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
}
