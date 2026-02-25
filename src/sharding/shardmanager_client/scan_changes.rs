use std::collections::HashMap;
use crate::sharding::shardmanager::scan_changes_types::ScanChangesResponse;

impl super::HttpShardManagerClient {
    /// Scan CDC changes from all active shards via the shard manager.
    ///
    /// `last_backup_views` maps shard number → view offset. Shards not in the
    /// map start from view 0 (full backup). Returns the bitcode-deserialized
    /// `ScanChangesResponse` with per-shard delta data.
    pub fn scan_changes(
        &self,
        last_backup_views: &HashMap<u32, u64>,
    ) -> Result<ScanChangesResponse, String> {
        let json_body = serde_json::to_string(&serde_json::json!({
            "last_backup_views": last_backup_views,
        }))
        .map_err(|e| format!("serialize request: {e}"))?;

        let request = format!(
            "POST /v1/scan-changes HTTP/1.1\r\nHost: {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            self.addr,
            json_body.len(),
            json_body,
        );

        let (headers, body_bytes) = self.exchange_raw(&request)?;

        // Check for HTTP error status.
        if let Some(status_line) = headers.lines().next()
            && let Some(code_str) = status_line.split_whitespace().nth(1)
            && let Ok(code) = code_str.parse::<u16>()
            && code != 200
        {
            let err_body = String::from_utf8_lossy(&body_bytes);
            return Err(format!("scan-changes HTTP {code}: {err_body}"));
        }

        bitcode::deserialize::<ScanChangesResponse>(&body_bytes)
            .map_err(|e| format!("deserialize ScanChangesResponse: {e}"))
    }

    /// Ship CDC deltas to a shard via the shard manager.
    ///
    /// `shard` is the target shard number, `replicas` are the shard's member
    /// addresses, and `delta_bytes` is the bitcode-serialized
    /// `Vec<LeaderRecordDelta<String, String>>`.
    pub fn apply_changes(
        &self,
        shard: u32,
        replicas: &[String],
        delta_bytes: &[u8],
    ) -> Result<(), String> {
        let replicas_str = replicas.join(",");
        let resp = self.http_post_binary(
            "/v1/apply-changes",
            &[
                ("X-Shard", &shard.to_string()),
                ("X-Replicas", &replicas_str),
                ("Content-Type", "application/octet-stream"),
            ],
            delta_bytes,
        )?;
        self.parse_response(&resp)
    }
}
