impl super::HttpShardManagerClient {
    /// Register and activate a shard with the given membership and key range.
    ///
    /// Sends `POST /v1/register` to the shard-manager server, which calls
    /// `ShardManager::register_active_shard()` → `strong_atomic_update_shards(ActivateShard)`
    /// to make the shard visible in discovery with its membership and key range.
    ///
    /// `replicas` is `Option` for historical reasons but should always be provided.
    /// Activating a shard without members makes it visible but unreachable. When
    /// `None`, the server falls back to querying discovery for existing membership,
    /// which only works if the shard was previously registered.
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
}
