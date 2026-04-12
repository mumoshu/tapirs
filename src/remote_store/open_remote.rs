use std::path::Path;

use crate::backup::storage::BackupStorage;
use crate::storage::io::error::StorageError;
use crate::storage::io::s3_caching_io::{S3CacheConfig, register_s3_cache};
use crate::unified::wisckeylsm::manifest::UnifiedManifest;

use super::config::S3StorageConfig;
use super::download::download_all_files;
use super::manifest_store::RemoteManifestStore;
use super::segment_store::RemoteSegmentStore;

/// Download the latest manifest and all referenced segments from S3,
/// saving them to `base_dir`. After this, `CombinedStoreInner::open()`
/// will find the manifest and reopen from sealed segments.
///
/// Returns the downloaded manifest for the caller to inspect.
pub async fn prepare_local_from_remote<S: BackupStorage>(
    segment_store: &RemoteSegmentStore<S>,
    manifest_store: &RemoteManifestStore<S>,
    shard: &str,
    base_dir: &Path,
) -> Result<UnifiedManifest, String> {
    let (_view, bytes) = manifest_store.download_latest_manifest(shard).await?;
    prepare_from_manifest_bytes(segment_store, shard, base_dir, &bytes).await
}

/// Download a specific view's manifest and all referenced segments.
pub async fn prepare_local_from_remote_at_view<S: BackupStorage>(
    segment_store: &RemoteSegmentStore<S>,
    manifest_store: &RemoteManifestStore<S>,
    shard: &str,
    view: u64,
    base_dir: &Path,
) -> Result<UnifiedManifest, String> {
    let bytes = manifest_store.download_manifest(shard, view).await?;
    prepare_from_manifest_bytes(segment_store, shard, base_dir, &bytes).await
}

async fn prepare_from_manifest_bytes<S: BackupStorage>(
    segment_store: &RemoteSegmentStore<S>,
    shard: &str,
    base_dir: &Path,
    manifest_bytes: &[u8],
) -> Result<UnifiedManifest, String> {
    let mut manifest: UnifiedManifest = bitcode::deserialize(manifest_bytes)
        .map_err(|e| format!("deserialize manifest: {e}"))?;

    // Ensure base_dir exists.
    std::fs::create_dir_all(base_dir)
        .map_err(|e| format!("create_dir_all {}: {e}", base_dir.display()))?;

    // Download all segment and SST files.
    download_all_files(segment_store, shard, base_dir, &manifest).await?;

    // Rebase manifest paths to the new base_dir.
    rebase_manifest_paths(&mut manifest, base_dir);

    // Save manifest locally so CombinedStoreInner::open() finds it.
    manifest
        .save::<crate::storage::io::disk_io::BufferedIo>(base_dir)
        .map_err(|e: StorageError| format!("save manifest: {e}"))?;

    Ok(manifest)
}

/// Download only the manifest from S3 and register S3 cache config so that
/// `S3CachingIo` will lazy-download segments on first access.
///
/// This is the zero-copy open path: no segment data is transferred until
/// a read actually needs it. The caller should open `CombinedStoreInner`
/// with `S3CachingIo` as the DiskIo type after calling this.
///
/// Returns the manifest for the caller to inspect.
pub async fn prepare_local_lazy<S: BackupStorage>(
    manifest_store: &RemoteManifestStore<S>,
    s3_config: &S3StorageConfig,
    shard: &str,
    base_dir: &Path,
) -> Result<UnifiedManifest, String> {
    let (_view, bytes) = manifest_store.download_latest_manifest(shard).await?;
    prepare_lazy_from_bytes(s3_config, shard, base_dir, &bytes)
}

/// Download a specific view's manifest (lazy, no segments).
pub async fn prepare_local_lazy_at_view<S: BackupStorage>(
    manifest_store: &RemoteManifestStore<S>,
    s3_config: &S3StorageConfig,
    shard: &str,
    view: u64,
    base_dir: &Path,
) -> Result<UnifiedManifest, String> {
    let bytes = manifest_store.download_manifest(shard, view).await?;
    prepare_lazy_from_bytes(s3_config, shard, base_dir, &bytes)
}

fn prepare_lazy_from_bytes(
    s3_config: &S3StorageConfig,
    shard: &str,
    base_dir: &Path,
    manifest_bytes: &[u8],
) -> Result<UnifiedManifest, String> {
    let mut manifest: UnifiedManifest = bitcode::deserialize(manifest_bytes)
        .map_err(|e| format!("deserialize manifest: {e}"))?;

    std::fs::create_dir_all(base_dir)
        .map_err(|e| format!("create_dir_all {}: {e}", base_dir.display()))?;

    // Register S3 config so S3CachingIo::open() can download segments on demand.
    let endpoint = s3_config
        .endpoint_url
        .as_deref()
        .unwrap_or("")
        .to_string();
    register_s3_cache(
        base_dir,
        S3CacheConfig {
            endpoint,
            bucket: s3_config.bucket.clone(),
            shard_segments_prefix: format!("{shard}/segments/"),
        },
    );

    // Rebase manifest paths to the new base_dir. The source manifest has
    // absolute paths from the source machine; we need paths in our directory.
    rebase_manifest_paths(&mut manifest, base_dir);

    // Save manifest locally so CombinedStoreInner::open() finds it.
    manifest
        .save::<crate::storage::io::disk_io::BufferedIo>(base_dir)
        .map_err(|e: StorageError| format!("save manifest: {e}"))?;

    Ok(manifest)
}

/// Rewrite all paths in the manifest to point to the new base_dir.
/// Sealed segment and SST paths are stored as absolute paths from the
/// source machine. When opening in a different directory, they need
/// to be rebased to the new location.
pub(crate) fn rebase_manifest_paths(manifest: &mut UnifiedManifest, base_dir: &Path) {
    rebase_lsm_paths(&mut manifest.committed, base_dir);
    rebase_lsm_paths(&mut manifest.prepared, base_dir);
    rebase_lsm_paths(&mut manifest.mvcc, base_dir);
    rebase_lsm_paths(&mut manifest.ir_inc, base_dir);
    rebase_lsm_paths(&mut manifest.ir_con, base_dir);
}

fn rebase_lsm_paths(data: &mut crate::unified::wisckeylsm::manifest::LsmManifestData, base_dir: &Path) {
    for seg in &mut data.sealed_vlog_segments {
        if let Some(name) = seg.path.file_name() {
            seg.path = base_dir.join(name);
        }
    }
    for sst in &mut data.sst_metas {
        if let Some(name) = sst.path.file_name() {
            sst.path = base_dir.join(name);
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::backup::local::LocalBackupStorage;
    use crate::unified::wisckeylsm::types::VlogSegmentMeta;

    #[tokio::test(flavor = "current_thread")]
    async fn prepare_local_from_remote_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let remote_dir = dir.path().join("remote");
        let local_dir = dir.path().join("local");

        let storage = LocalBackupStorage::new(remote_dir.to_str().unwrap());
        storage.init().await.unwrap();
        let seg_store = RemoteSegmentStore::new(storage.sub(""));
        let man_store = RemoteManifestStore::new(storage.sub(""));

        // Create a manifest with one sealed segment.
        let mut manifest = UnifiedManifest::new();
        manifest.current_view = 1;
        manifest.committed.sealed_vlog_segments.push(VlogSegmentMeta {
            segment_id: 0,
            path: "comb_comm_vlog_0000.dat".into(),
            views: vec![],
            total_size: 42,
        });

        // Upload segment files and active segment stubs.
        let seg_sub = storage.sub("shard_0").sub("segments");
        seg_sub.init().await.unwrap();
        seg_sub.write("comb_comm_vlog_0000.dat", b"segment-data").await.unwrap();
        // Active segment files (empty, needed by open_from_manifest).
        for name in ["comb_comm_vlog_0000.dat", "comb_prep_vlog_0000.dat",
                      "comb_mvcc_vlog_0000.dat", "ir_inc_vlog_0000.dat",
                      "ir_con_vlog_0000.dat"] {
            if seg_sub.exists(name).await.unwrap() { continue; }
            seg_sub.write(name, b"").await.unwrap();
        }

        let manifest_bytes = bitcode::serialize(&manifest).unwrap();
        man_store.upload_manifest("shard_0", 1, &manifest_bytes).await.unwrap();

        // Download to local dir.
        let result = prepare_local_from_remote(
            &seg_store, &man_store, "shard_0", &local_dir,
        ).await.unwrap();

        assert_eq!(result.current_view, 1);
        assert!(local_dir.join("comb_comm_vlog_0000.dat").exists());
        assert!(local_dir.join("UNIFIED_MANIFEST").exists());
    }
}
