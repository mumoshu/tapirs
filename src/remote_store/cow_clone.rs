use std::path::Path;

use crate::backup::storage::BackupStorage;
use crate::mvcc::disk::s3_caching_io::{S3CacheConfig, register_s3_cache};
use crate::unified::wisckeylsm::manifest::{LsmManifestData, UnifiedManifest};

use super::config::S3StorageConfig;
use super::download::download_all_files;
use super::manifest_store::RemoteManifestStore;
use super::segment_store::RemoteSegmentStore;

/// Rewrite a manifest for a COW clone: keep sealed segments unchanged
/// (they reference the same S3 files), but assign fresh active segment
/// IDs and reset active write offsets to 0.
pub fn rewrite_manifest_for_clone(source: &UnifiedManifest) -> UnifiedManifest {
    let mut m = source.clone();
    rewrite_lsm_active("comb_comm", &mut m.committed);
    rewrite_lsm_active("comb_prep", &mut m.prepared);
    rewrite_lsm_active("comb_mvcc", &mut m.mvcc);
    rewrite_lsm_active("ir_inc", &mut m.ir_inc);
    rewrite_lsm_active("ir_con", &mut m.ir_con);
    m
}

fn rewrite_lsm_active(label: &str, data: &mut LsmManifestData) {
    // If the source's active segment has data, seal it as a sealed segment
    // in the clone manifest. Otherwise the clone can't find the data.
    if data.active_write_offset > 0 {
        use crate::unified::wisckeylsm::types::VlogSegmentMeta;
        data.sealed_vlog_segments.push(VlogSegmentMeta {
            segment_id: data.active_segment_id,
            path: super::download::active_vlog_name(label, data.active_segment_id).into(),
            views: vec![],
            total_size: data.active_write_offset,
        });
    }
    data.active_segment_id = data.next_segment_id;
    data.next_segment_id += 1;
    data.active_write_offset = 0;
}

/// Clone a shard from S3: download the source manifest and segments,
/// rewrite the manifest for COW (fresh active segments), create empty
/// active segment files, and save the manifest locally.
///
/// After this, `CombinedStoreInner::open()` will find the manifest and
/// open with shared sealed segments + empty active segments.
pub async fn clone_from_remote<S: BackupStorage>(
    segment_store: &RemoteSegmentStore<S>,
    manifest_store: &RemoteManifestStore<S>,
    shard: &str,
    view: u64,
    clone_base_dir: &Path,
) -> Result<UnifiedManifest, String> {
    // Download source manifest.
    let source_bytes = manifest_store.download_manifest(shard, view).await?;
    let source: UnifiedManifest = bitcode::deserialize(&source_bytes)
        .map_err(|e| format!("deserialize source manifest: {e}"))?;

    // Ensure clone directory exists.
    std::fs::create_dir_all(clone_base_dir)
        .map_err(|e| format!("create_dir_all {}: {e}", clone_base_dir.display()))?;

    // Download all sealed segments and SSTs from source.
    download_all_files(segment_store, shard, clone_base_dir, &source).await?;

    // Rewrite manifest: fresh active segments, reset offsets.
    let mut cloned = rewrite_manifest_for_clone(&source);
    super::open_remote::rebase_manifest_paths(&mut cloned, clone_base_dir);

    // Save clone manifest locally.
    cloned
        .save::<crate::mvcc::disk::disk_io::BufferedIo>(clone_base_dir)
        .map_err(|e| format!("save clone manifest: {e}"))?;

    Ok(cloned)
}

/// Zero-copy clone: download only the manifest from S3, rewrite it for
/// the clone, and register S3 cache config so segments are lazy-downloaded
/// on first access via S3CachingIo.
///
/// No segment data is transferred until the clone actually reads it. The
/// clone's new writes go to local active segments. Reads of existing data
/// fall through to S3-backed sealed segments, cached locally on first access.
/// `view` must come from a CrossShardSnapshot to ensure cross-shard
/// consistency. Never pass "latest" — each shard's latest manifest is
/// at a different point in time, producing inconsistent state.
pub async fn clone_from_remote_lazy<S: BackupStorage>(
    manifest_store: &RemoteManifestStore<S>,
    s3_config: &S3StorageConfig,
    shard: &str,
    view: u64,
    clone_base_dir: &Path,
) -> Result<UnifiedManifest, String> {
    let source_bytes = manifest_store.download_manifest(shard, view).await?;
    let source: UnifiedManifest = bitcode::deserialize(&source_bytes)
        .map_err(|e| format!("deserialize source manifest: {e}"))?;

    std::fs::create_dir_all(clone_base_dir)
        .map_err(|e| format!("create_dir_all {}: {e}", clone_base_dir.display()))?;

    // Register S3 config so S3CachingIo downloads segments on demand.
    let endpoint = s3_config
        .endpoint_url
        .as_deref()
        .unwrap_or("")
        .to_string();
    register_s3_cache(
        clone_base_dir,
        S3CacheConfig {
            endpoint,
            bucket: s3_config.bucket.clone(),
            shard_segments_prefix: format!("{shard}/segments/"),
        },
    );

    let mut cloned = rewrite_manifest_for_clone(&source);
    super::open_remote::rebase_manifest_paths(&mut cloned, clone_base_dir);

    cloned
        .save::<crate::mvcc::disk::disk_io::BufferedIo>(clone_base_dir)
        .map_err(|e| format!("save clone manifest: {e}"))?;

    // Active segment files for the clone are fresh (rewrite_manifest_for_clone
    // advanced the IDs, offset=0). Create them as empty local files since no data
    // has been written yet. Sealed segments stay on S3 (lazy-downloaded on read).
    for (label, data) in [
        ("comb_comm", &cloned.committed),
        ("comb_prep", &cloned.prepared),
        ("comb_mvcc", &cloned.mvcc),
        ("ir_inc", &cloned.ir_inc),
        ("ir_con", &cloned.ir_con),
    ] {
        let name = super::download::active_vlog_name(label, data.active_segment_id);
        let path = clone_base_dir.join(&name);
        if !path.exists() {
            std::fs::write(&path, b"")
                .map_err(|e| format!("create active segment {name}: {e}"))?;
        }
    }

    Ok(cloned)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::unified::wisckeylsm::types::VlogSegmentMeta;

    #[test]
    fn rewrite_advances_active_ids() {
        let mut source = UnifiedManifest::new();
        // committed has segments 0,1 sealed, active=2, next=3
        source.committed.sealed_vlog_segments.push(VlogSegmentMeta {
            segment_id: 0,
            path: "vlog_0000.dat".into(),
            views: vec![],
            total_size: 100,
        });
        source.committed.active_segment_id = 2;
        source.committed.next_segment_id = 3;
        source.committed.active_write_offset = 500;

        let cloned = rewrite_manifest_for_clone(&source);

        // Active should be 3 (was next_segment_id), next should be 4.
        assert_eq!(cloned.committed.active_segment_id, 3);
        assert_eq!(cloned.committed.next_segment_id, 4);
        assert_eq!(cloned.committed.active_write_offset, 0);
        // Sealed segments: original 1 + the old active (had data, offset=500).
        assert_eq!(cloned.committed.sealed_vlog_segments.len(), 2);
        assert_eq!(cloned.committed.sealed_vlog_segments[0].segment_id, 0);
        assert_eq!(cloned.committed.sealed_vlog_segments[1].segment_id, 2);
    }

    #[test]
    fn rewrite_all_lsms() {
        let mut source = UnifiedManifest::new();
        source.mvcc.next_segment_id = 5;
        source.ir_inc.next_segment_id = 2;

        let cloned = rewrite_manifest_for_clone(&source);

        assert_eq!(cloned.mvcc.active_segment_id, 5);
        assert_eq!(cloned.mvcc.next_segment_id, 6);
        assert_eq!(cloned.ir_inc.active_segment_id, 2);
        assert_eq!(cloned.ir_inc.next_segment_id, 3);
    }
}
