use crate::backup::storage::BackupStorage;

/// Manages versioned manifest snapshots on S3.
///
/// S3 key layout per shard:
///   `{shard}/manifests/v{view:08}.manifest`
///
/// Each manifest is a bitcode-serialized `UnifiedManifest` uploaded
/// after a successful flush. Manifests are immutable once written.
pub struct RemoteManifestStore<S> {
    storage: S,
}

impl<S: BackupStorage> RemoteManifestStore<S> {
    pub fn new(storage: S) -> Self {
        Self { storage }
    }

    fn manifest_key(view: u64) -> String {
        format!("v{view:08}.manifest")
    }

    async fn manifests_sub(&self, shard: &str) -> Result<S, String> {
        let sub = self.storage.sub(shard).sub("manifests");
        sub.init().await?;
        Ok(sub)
    }

    /// Upload a manifest snapshot for the given shard and view.
    pub async fn upload_manifest(
        &self,
        shard: &str,
        view: u64,
        manifest_bytes: &[u8],
    ) -> Result<(), String> {
        let sub = self.manifests_sub(shard).await?;
        sub.write(&Self::manifest_key(view), manifest_bytes).await
    }

    /// Download a specific view's manifest.
    pub async fn download_manifest(
        &self,
        shard: &str,
        view: u64,
    ) -> Result<Vec<u8>, String> {
        let sub = self.manifests_sub(shard).await?;
        sub.read(&Self::manifest_key(view)).await
    }

    /// Parse a manifest filename like "v00000003.manifest" into a view number.
    fn parse_manifest_view(filename: &str) -> Option<u64> {
        let stem = filename.strip_prefix('v')?.strip_suffix(".manifest")?;
        stem.parse().ok()
    }

    /// List all manifest versions available for a shard, sorted ascending.
    ///
    /// Enumerates `v*.manifest` files in the shard's manifests directory.
    /// For long-running clusters with many views, prefer `latest_manifest_view`
    /// when only the latest version is needed.
    pub async fn list_manifest_versions(
        &self,
        shard: &str,
    ) -> Result<Vec<u64>, String> {
        let sub = self.manifests_sub(shard).await?;
        let files = sub.list_files("v").await?;
        let mut versions: Vec<u64> = files
            .iter()
            .filter_map(|f| Self::parse_manifest_view(f))
            .collect();
        versions.sort();
        Ok(versions)
    }

    /// Get the latest manifest view number for a shard.
    ///
    /// Uses reverse listing to avoid enumerating all versions.
    /// Returns None if no manifests exist.
    pub async fn latest_manifest_view(
        &self,
        shard: &str,
    ) -> Result<Option<u64>, String> {
        let sub = self.manifests_sub(shard).await?;
        let files = sub.list_files_reverse("v", 1).await?;
        Ok(files.first().and_then(|f| Self::parse_manifest_view(f)))
    }

    /// Check whether a specific manifest view exists for a shard.
    pub async fn manifest_exists(
        &self,
        shard: &str,
        view: u64,
    ) -> Result<bool, String> {
        let sub = self.manifests_sub(shard).await?;
        sub.exists(&Self::manifest_key(view)).await
    }

    /// Download the latest manifest for a shard.
    /// Returns (view, manifest_bytes).
    pub async fn download_latest_manifest(
        &self,
        shard: &str,
    ) -> Result<(u64, Vec<u8>), String> {
        let view = self
            .latest_manifest_view(shard)
            .await?
            .ok_or_else(|| format!("no manifests found for {shard}"))?;
        let data = self.download_manifest(shard, view).await?;
        Ok((view, data))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backup::local::LocalBackupStorage;

    #[tokio::test(flavor = "current_thread")]
    async fn upload_download_manifest() {
        let dir = tempfile::tempdir().unwrap();
        let storage = LocalBackupStorage::new(dir.path().join("remote").to_str().unwrap());
        storage.init().await.unwrap();
        let store = RemoteManifestStore::new(storage);

        let manifest_bytes = b"manifest-v1-data";
        store.upload_manifest("shard_0", 1, manifest_bytes).await.unwrap();

        let data = store.download_manifest("shard_0", 1).await.unwrap();
        assert_eq!(data, manifest_bytes);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn list_versions_and_latest() {
        let dir = tempfile::tempdir().unwrap();
        let storage = LocalBackupStorage::new(dir.path().join("remote").to_str().unwrap());
        storage.init().await.unwrap();
        let store = RemoteManifestStore::new(storage);

        // No versions yet.
        assert!(store.list_manifest_versions("shard_0").await.unwrap().is_empty());
        assert!(store.latest_manifest_view("shard_0").await.unwrap().is_none());

        // Upload two versions.
        store.upload_manifest("shard_0", 1, b"v1").await.unwrap();
        store.upload_manifest("shard_0", 3, b"v3").await.unwrap();

        let versions = store.list_manifest_versions("shard_0").await.unwrap();
        assert_eq!(versions, vec![1, 3]);

        assert_eq!(store.latest_manifest_view("shard_0").await.unwrap(), Some(3));

        let (view, data) = store.download_latest_manifest("shard_0").await.unwrap();
        assert_eq!(view, 3);
        assert_eq!(data, b"v3");

        // manifest_exists
        assert!(store.manifest_exists("shard_0", 1).await.unwrap());
        assert!(store.manifest_exists("shard_0", 3).await.unwrap());
        assert!(!store.manifest_exists("shard_0", 2).await.unwrap());
    }
}
