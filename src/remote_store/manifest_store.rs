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

    /// List all manifest versions available for a shard, sorted ascending.
    pub async fn list_manifest_versions(
        &self,
        shard: &str,
    ) -> Result<Vec<u64>, String> {
        let sub = self.storage.sub(shard).sub("manifests");
        let entries = sub.list_subdirs().await;
        // list_subdirs returns prefixes; for manifest files we need to
        // list objects. Use a convention: write a marker in each manifest
        // "directory" or parse the flat listing.
        // Since BackupStorage only has list_subdirs, we need to work with
        // the flat file listing. For now, use exists() to probe known views.
        // TODO: add list_files() to BackupStorage for proper enumeration.
        //
        // Alternative: store a version index file. Simpler and more reliable.
        // Write "versions.json" listing all uploaded views.
        let _ = entries;
        let versions_key = "versions.json";
        let versions_sub = self.manifests_sub(shard).await?;
        match versions_sub.read(versions_key).await {
            Ok(data) => {
                let text = String::from_utf8(data)
                    .map_err(|e| format!("invalid UTF-8 in versions.json: {e}"))?;
                let versions: Vec<u64> = serde_json::from_str(&text)
                    .map_err(|e| format!("parse versions.json: {e}"))?;
                Ok(versions)
            }
            Err(_) => Ok(Vec::new()), // No versions yet.
        }
    }

    /// Download the latest manifest for a shard.
    /// Returns (view, manifest_bytes).
    pub async fn download_latest_manifest(
        &self,
        shard: &str,
    ) -> Result<(u64, Vec<u8>), String> {
        let versions = self.list_manifest_versions(shard).await?;
        let view = versions
            .last()
            .copied()
            .ok_or_else(|| format!("no manifests found for {shard}"))?;
        let data = self.download_manifest(shard, view).await?;
        Ok((view, data))
    }

    /// Update the version index after uploading a manifest.
    /// Appends the new view to the sorted list.
    pub async fn register_version(
        &self,
        shard: &str,
        view: u64,
    ) -> Result<(), String> {
        let mut versions = self.list_manifest_versions(shard).await?;
        if !versions.contains(&view) {
            versions.push(view);
            versions.sort();
        }
        let json = serde_json::to_string(&versions)
            .map_err(|e| format!("serialize versions: {e}"))?;
        let sub = self.manifests_sub(shard).await?;
        sub.write("versions.json", json.as_bytes()).await
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
        store.register_version("shard_0", 1).await.unwrap();

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

        // Upload two versions.
        store.upload_manifest("shard_0", 1, b"v1").await.unwrap();
        store.register_version("shard_0", 1).await.unwrap();
        store.upload_manifest("shard_0", 3, b"v3").await.unwrap();
        store.register_version("shard_0", 3).await.unwrap();

        let versions = store.list_manifest_versions("shard_0").await.unwrap();
        assert_eq!(versions, vec![1, 3]);

        let (view, data) = store.download_latest_manifest("shard_0").await.unwrap();
        assert_eq!(view, 3);
        assert_eq!(data, b"v3");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn register_version_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let storage = LocalBackupStorage::new(dir.path().join("remote").to_str().unwrap());
        storage.init().await.unwrap();
        let store = RemoteManifestStore::new(storage);

        store.register_version("shard_0", 5).await.unwrap();
        store.register_version("shard_0", 5).await.unwrap();
        let versions = store.list_manifest_versions("shard_0").await.unwrap();
        assert_eq!(versions, vec![5]);
    }
}
