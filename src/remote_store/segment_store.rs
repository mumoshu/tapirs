use std::path::Path;

use crate::backup::storage::BackupStorage;

/// Manages upload/download of VlogLsm segment files and SSTs between
/// local disk and an S3-compatible remote store.
///
/// S3 key layout per shard:
///   `{shard}/segments/{segment_filename}`
///
/// Segment filenames match the local names (e.g. `comb_mvcc_vlog_0001.dat`,
/// `comb_comm_sst_0000.db`).
pub struct RemoteSegmentStore<S> {
    storage: S,
}

impl<S: BackupStorage> RemoteSegmentStore<S> {
    pub fn new(storage: S) -> Self {
        Self { storage }
    }

    /// Upload a local segment file to the shard's segments prefix on S3.
    pub async fn upload_segment(
        &self,
        local_path: &Path,
        shard: &str,
        segment_name: &str,
    ) -> Result<(), String> {
        let data = std::fs::read(local_path)
            .map_err(|e| format!("read {}: {e}", local_path.display()))?;
        let sub = self.storage.sub(shard).sub("segments");
        sub.init().await?;
        sub.write(segment_name, &data).await
    }

    /// Atomically create a segment on S3 only if it doesn't exist.
    /// Returns true if created, false if already exists.
    pub async fn create_segment_if_absent(
        &self,
        local_path: &Path,
        shard: &str,
        segment_name: &str,
    ) -> Result<bool, String> {
        let data = std::fs::read(local_path)
            .map_err(|e| format!("read {}: {e}", local_path.display()))?;
        let sub = self.storage.sub(shard).sub("segments");
        sub.init().await?;
        sub.create_if_absent(segment_name, &data).await
    }

    /// Upload a segment only if the local copy is larger than the S3 copy.
    /// Returns true if uploaded, false if skipped.
    pub async fn upload_segment_if_larger(
        &self,
        local_path: &Path,
        shard: &str,
        segment_name: &str,
    ) -> Result<bool, String> {
        let data = std::fs::read(local_path)
            .map_err(|e| format!("read {}: {e}", local_path.display()))?;
        let sub = self.storage.sub(shard).sub("segments");
        sub.init().await?;
        sub.write_if_larger(segment_name, &data).await
    }

    /// Download a segment file from S3 to a local path.
    pub async fn download_segment(
        &self,
        shard: &str,
        segment_name: &str,
        local_path: &Path,
    ) -> Result<(), String> {
        let sub = self.storage.sub(shard).sub("segments");
        let data = sub.read(segment_name).await?;
        std::fs::write(local_path, &data)
            .map_err(|e| format!("write {}: {e}", local_path.display()))
    }

    /// Check if a segment exists on S3.
    pub async fn segment_exists(
        &self,
        shard: &str,
        segment_name: &str,
    ) -> Result<bool, String> {
        let sub = self.storage.sub(shard).sub("segments");
        sub.exists(segment_name).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backup::local::LocalBackupStorage;

    #[tokio::test(flavor = "current_thread")]
    async fn upload_download_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let storage = LocalBackupStorage::new(dir.path().join("remote").to_str().unwrap());
        storage.init().await.unwrap();
        let store = RemoteSegmentStore::new(storage);

        // Create a local "segment" file.
        let local = dir.path().join("seg.dat");
        std::fs::write(&local, b"segment-data-123").unwrap();

        // Upload.
        store.upload_segment(&local, "shard_0", "comb_mvcc_vlog_0001.dat").await.unwrap();

        // Check exists.
        assert!(store.segment_exists("shard_0", "comb_mvcc_vlog_0001.dat").await.unwrap());
        assert!(!store.segment_exists("shard_0", "nonexistent.dat").await.unwrap());

        // Download to a new path.
        let downloaded = dir.path().join("downloaded.dat");
        store.download_segment("shard_0", "comb_mvcc_vlog_0001.dat", &downloaded).await.unwrap();
        assert_eq!(std::fs::read(&downloaded).unwrap(), b"segment-data-123");
    }
}
