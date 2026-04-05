use std::path::Path;

use crate::backup::storage::BackupStorage;
use crate::unified::wisckeylsm::manifest::UnifiedManifest;

use super::manifest_store::RemoteManifestStore;
use super::segment_store::RemoteSegmentStore;
use super::upload::{diff_manifests, upload_manifest_snapshot, upload_new_segments};

/// Sync local state to remote after a flush.
///
/// Diffs the manifest before and after seal to find newly-sealed segments,
/// uploads them, then uploads the manifest as a versioned snapshot.
///
/// Best-effort: logs errors but does not propagate them. S3 is never on
/// the critical path for view changes.
pub async fn sync_to_remote<S: BackupStorage>(
    segment_store: &RemoteSegmentStore<S>,
    manifest_store: &RemoteManifestStore<S>,
    shard: &str,
    base_dir: &Path,
    before: &UnifiedManifest,
    after: &UnifiedManifest,
) {
    let new_files = diff_manifests(before, after);
    if new_files.is_empty() && before.current_view == after.current_view {
        return;
    }

    match upload_new_segments(segment_store, shard, base_dir, &new_files).await {
        Ok(n) => {
            if n > 0 {
                tracing::debug!(shard, n, "uploaded {n} segments to S3");
            }
        }
        Err(e) => {
            tracing::warn!(shard, error = %e, "failed to upload segments to S3");
            return;
        }
    }

    let manifest_bytes = match bitcode::serialize(after) {
        Ok(b) => b,
        Err(e) => {
            tracing::warn!(shard, error = %e, "failed to serialize manifest for S3");
            return;
        }
    };

    if let Err(e) =
        upload_manifest_snapshot(manifest_store, shard, after.current_view, &manifest_bytes).await
    {
        tracing::warn!(shard, error = %e, "failed to upload manifest to S3");
    }
}
