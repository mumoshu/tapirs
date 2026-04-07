use std::path::Path;

use crate::backup::storage::BackupStorage;
use crate::unified::wisckeylsm::manifest::{LsmManifestData, UnifiedManifest};

use super::manifest_store::RemoteManifestStore;
use super::segment_store::RemoteSegmentStore;

/// Collect file names that are new in `after` but not in `before`.
/// Compares sealed_vlog_segments and sst_metas by segment_id / sst id.
fn new_segment_paths(before: &LsmManifestData, after: &LsmManifestData) -> Vec<String> {
    let mut paths = Vec::new();
    let before_seg_ids: std::collections::BTreeSet<u64> = before
        .sealed_vlog_segments
        .iter()
        .map(|s| s.segment_id)
        .collect();
    for seg in &after.sealed_vlog_segments {
        if !before_seg_ids.contains(&seg.segment_id)
            && let Some(name) = seg.path.file_name().and_then(|n| n.to_str())
        {
            paths.push(name.to_string());
        }
    }
    let before_sst_ids: std::collections::BTreeSet<u64> =
        before.sst_metas.iter().map(|s| s.id).collect();
    for sst in &after.sst_metas {
        if !before_sst_ids.contains(&sst.id)
            && let Some(name) = sst.path.file_name().and_then(|n| n.to_str())
        {
            paths.push(name.to_string());
        }
    }
    paths
}

/// Collect all new segment/SST filenames across all LsmManifestData fields.
pub fn diff_manifests(before: &UnifiedManifest, after: &UnifiedManifest) -> Vec<String> {
    let mut files = Vec::new();
    files.extend(new_segment_paths(&before.committed, &after.committed));
    files.extend(new_segment_paths(&before.prepared, &after.prepared));
    files.extend(new_segment_paths(&before.mvcc, &after.mvcc));
    files.extend(new_segment_paths(&before.ir_inc, &after.ir_inc));
    files.extend(new_segment_paths(&before.ir_con, &after.ir_con));
    files
}

/// Collect ALL filenames referenced in the manifest (for full upload).
/// Reuses all_file_names from the download module.
pub fn all_manifest_files(manifest: &UnifiedManifest) -> Vec<String> {
    super::download::all_file_names(manifest)
}

/// Upload all new segment files to S3. Checks existence first (HEAD) to
/// skip already-uploaded segments (idempotent).
pub async fn upload_new_segments<S: BackupStorage>(
    segment_store: &RemoteSegmentStore<S>,
    shard: &str,
    base_dir: &Path,
    new_files: &[String],
) -> Result<usize, String> {
    let mut uploaded = 0;
    for name in new_files {
        if segment_store.segment_exists(shard, name).await? {
            continue;
        }
        let local_path = base_dir.join(name);
        if local_path.exists() {
            segment_store
                .upload_segment(&local_path, shard, name)
                .await?;
            uploaded += 1;
        }
    }
    Ok(uploaded)
}

/// Upload newly sealed segments and SSTs from the manifest diff.
///
/// - **Vlog segments**: use `write_if_larger` because S3 may have an
///   older smaller copy from when the segment was active.
/// - **SST files**: use `create_if_absent` because SSTs are new files
///   created at seal time and all replicas produce identical content.
pub async fn upload_segments_force<S: BackupStorage>(
    segment_store: &RemoteSegmentStore<S>,
    shard: &str,
    base_dir: &Path,
    files: &[String],
) -> Result<usize, String> {
    let mut uploaded = 0;
    for name in files {
        let local_path = base_dir.join(name);
        if !local_path.exists() {
            return Err(format!(
                "manifest references segment {} but file does not exist at {}",
                name,
                local_path.display()
            ));
        }
        let is_sst = name.ends_with(".db");
        let did_upload = if is_sst {
            segment_store
                .create_segment_if_absent(&local_path, shard, name)
                .await?
        } else {
            segment_store
                .upload_segment_if_larger(&local_path, shard, name)
                .await?
        };
        if did_upload {
            uploaded += 1;
        }
    }
    Ok(uploaded)
}

/// Collect active segment filenames from the manifest.
pub fn active_segment_files(manifest: &UnifiedManifest) -> Vec<String> {
    super::download::active_vlog_names(manifest)
}

/// Upload active segment files using atomic write-if-larger.
/// Multiple replicas may upload the same active segment concurrently.
/// Only the replica with the most data (largest file) wins — active
/// segments grow by append, so "larger" means "more up-to-date".
pub async fn upload_active_segments<S: BackupStorage>(
    segment_store: &RemoteSegmentStore<S>,
    shard: &str,
    base_dir: &Path,
    manifest: &UnifiedManifest,
) -> Result<usize, String> {
    let files = active_segment_files(manifest);
    let mut uploaded = 0;
    for name in &files {
        let local_path = base_dir.join(name);
        if !local_path.exists() {
            return Err(format!(
                "manifest references active segment {} but file does not exist at {}",
                name,
                local_path.display()
            ));
        }
        if segment_store
            .upload_segment_if_larger(&local_path, shard, name)
            .await?
        {
            uploaded += 1;
        }
    }
    Ok(uploaded)
}

/// Upload the manifest as a versioned snapshot.
///
/// The manifest file name encodes the view number (`v{view:08}.manifest`),
/// so no separate version index is needed — `list_manifest_versions` and
/// `latest_manifest_view` discover versions by listing manifest files.
pub async fn upload_manifest_snapshot<S: BackupStorage>(
    manifest_store: &RemoteManifestStore<S>,
    shard: &str,
    view: u64,
    manifest_bytes: &[u8],
) -> Result<(), String> {
    manifest_store
        .upload_manifest(shard, view, manifest_bytes)
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::unified::wisckeylsm::manifest::LsmManifestData;
    use crate::unified::wisckeylsm::types::VlogSegmentMeta;

    fn make_lsm(seg_ids: &[u64], sst_ids: &[u64]) -> LsmManifestData {
        let mut data = LsmManifestData::new();
        for &id in seg_ids {
            data.sealed_vlog_segments.push(VlogSegmentMeta {
                segment_id: id,
                path: format!("vlog_{id:04}.dat").into(),
                views: vec![],
                total_size: 100,
            });
        }
        for &id in sst_ids {
            data.sst_metas.push(crate::unified::wisckeylsm::sst::SstMeta {
                id,
                path: format!("sst_{id:04}.db").into(),
                num_entries: 10,
            });
        }
        data
    }

    #[test]
    fn diff_finds_new_segments() {
        let mut before = UnifiedManifest::new();
        before.committed = make_lsm(&[0], &[0]);
        let mut after = UnifiedManifest::new();
        after.committed = make_lsm(&[0, 1], &[0, 1]);

        let new_files = diff_manifests(&before, &after);
        assert_eq!(new_files, vec!["vlog_0001.dat", "sst_0001.db"]);
    }

    #[test]
    fn diff_empty_when_identical() {
        let m = UnifiedManifest::new();
        assert!(diff_manifests(&m, &m).is_empty());
    }
}
