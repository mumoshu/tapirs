use std::path::Path;

use crate::backup::storage::BackupStorage;
use crate::unified::wisckeylsm::manifest::UnifiedManifest;

use super::manifest_store::RemoteManifestStore;
use super::segment_store::RemoteSegmentStore;
use super::upload::{diff_manifests, upload_active_segments, upload_manifest_snapshot, upload_segments_force};

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

    // Force-upload newly sealed segments. A segment that was active in the
    // previous view may have been uploaded as empty/small; now that it's
    // sealed with data, we must overwrite the S3 copy.
    match upload_segments_force(segment_store, shard, base_dir, &new_files).await {
        Ok(n) => {
            if n > 0 {
                tracing::debug!(shard, n, "uploaded {n} sealed segments to S3");
            }
        }
        Err(e) => {
            tracing::warn!(shard, error = %e, "failed to upload segments to S3");
            return;
        }
    }

    // Upload active segment files unconditionally (they grow between seals).
    if let Err(e) = upload_active_segments(segment_store, shard, base_dir, after).await {
        tracing::warn!(shard, error = %e, "failed to upload active segments to S3");
        return;
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
