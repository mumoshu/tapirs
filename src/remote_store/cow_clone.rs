use std::path::Path;

use crate::backup::storage::BackupStorage;
use crate::unified::wisckeylsm::manifest::{LsmManifestData, UnifiedManifest};

use super::download::download_all_files;
use super::manifest_store::RemoteManifestStore;
use super::segment_store::RemoteSegmentStore;

/// Rewrite a manifest for a COW clone: keep sealed segments unchanged
/// (they reference the same S3 files), but assign fresh active segment
/// IDs and reset active write offsets to 0.
pub fn rewrite_manifest_for_clone(source: &UnifiedManifest) -> UnifiedManifest {
    let mut m = source.clone();
    rewrite_lsm_active(&mut m.committed);
    rewrite_lsm_active(&mut m.prepared);
    rewrite_lsm_active(&mut m.mvcc);
    rewrite_lsm_active(&mut m.ir_inc);
    rewrite_lsm_active(&mut m.ir_con);
    m
}

fn rewrite_lsm_active(data: &mut LsmManifestData) {
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
    view: Option<u64>,
    clone_base_dir: &Path,
) -> Result<UnifiedManifest, String> {
    // Download source manifest.
    let source_bytes = match view {
        Some(v) => manifest_store.download_manifest(shard, v).await?,
        None => manifest_store.download_latest_manifest(shard).await?.1,
    };
    let source: UnifiedManifest = bitcode::deserialize(&source_bytes)
        .map_err(|e| format!("deserialize source manifest: {e}"))?;

    // Ensure clone directory exists.
    std::fs::create_dir_all(clone_base_dir)
        .map_err(|e| format!("create_dir_all {}: {e}", clone_base_dir.display()))?;

    // Download all sealed segments and SSTs from source.
    download_all_files(segment_store, shard, clone_base_dir, &source).await?;

    // Rewrite manifest: fresh active segments, reset offsets.
    let cloned = rewrite_manifest_for_clone(&source);

    // Save clone manifest locally.
    cloned
        .save::<crate::mvcc::disk::disk_io::BufferedIo>(clone_base_dir)
        .map_err(|e| format!("save clone manifest: {e}"))?;

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
        // Sealed segments unchanged.
        assert_eq!(cloned.committed.sealed_vlog_segments.len(), 1);
        assert_eq!(cloned.committed.sealed_vlog_segments[0].segment_id, 0);
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
