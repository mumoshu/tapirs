/// Configuration for connecting to an S3-compatible remote store.
///
/// Used by [`RemoteSegmentStore`] and [`RemoteManifestStore`] to
/// upload/download segment files and manifests.
#[derive(Clone)]
pub struct S3StorageConfig {
    pub bucket: String,
    pub prefix: String,
    pub endpoint_url: Option<String>,
    pub region: Option<String>,
}

impl S3StorageConfig {
    /// Shard-specific prefix within the bucket: `{prefix}shard_{id}/`.
    pub fn shard_prefix(&self, shard_id: u32) -> String {
        format!("{}shard_{shard_id}/", self.prefix)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shard_prefix_formatting() {
        let cfg = S3StorageConfig {
            bucket: "my-bucket".into(),
            prefix: "backups/".into(),
            endpoint_url: None,
            region: None,
        };
        assert_eq!(cfg.shard_prefix(0), "backups/shard_0/");
        assert_eq!(cfg.shard_prefix(42), "backups/shard_42/");
    }

    #[test]
    fn shard_prefix_empty_prefix() {
        let cfg = S3StorageConfig {
            bucket: "b".into(),
            prefix: String::new(),
            endpoint_url: None,
            region: None,
        };
        assert_eq!(cfg.shard_prefix(1), "shard_1/");
    }
}
