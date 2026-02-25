impl super::HttpShardManagerClient {
    pub fn merge(&self, absorbed: u32, surviving: u32) -> Result<(), String> {
        let body = serde_json::json!({
            "absorbed": absorbed,
            "surviving": surviving,
        })
        .to_string();
        let resp = self.http_post("/v1/merge", &body)?;
        self.parse_response(&resp)
    }
}
