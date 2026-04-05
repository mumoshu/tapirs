use std::path::Path;

use crate::backup::storage::BackupStorage;
use crate::unified::wisckeylsm::manifest::{LsmManifestData, UnifiedManifest};

use super::segment_store::RemoteSegmentStore;

/// Collect all segment and SST filenames referenced in an LsmManifestData.
fn lsm_file_names(data: &LsmManifestData) -> Vec<String> {
    let mut names = Vec::new();
    for seg in &data.sealed_vlog_segments {
        if let Some(name) = seg.path.file_name().and_then(|n| n.to_str()) {
            names.push(name.to_string());
        }
    }
    for sst in &data.sst_metas {
        if let Some(name) = sst.path.file_name().and_then(|n| n.to_str()) {
            names.push(name.to_string());
        }
    }
    names
}

/// Collect all segment and SST filenames across the entire manifest.
pub fn all_file_names(manifest: &UnifiedManifest) -> Vec<String> {
    let mut names = Vec::new();
    names.extend(lsm_file_names(&manifest.committed));
    names.extend(lsm_file_names(&manifest.prepared));
    names.extend(lsm_file_names(&manifest.mvcc));
    names.extend(lsm_file_names(&manifest.ir_inc));
    names.extend(lsm_file_names(&manifest.ir_con));
    names
}

/// Download all segment and SST files referenced in the manifest.
///
/// Skips files that already exist locally (idempotent for partial retries).
pub async fn download_all_files<S: BackupStorage>(
    segment_store: &RemoteSegmentStore<S>,
    shard: &str,
    base_dir: &Path,
    manifest: &UnifiedManifest,
) -> Result<usize, String> {
    let names = all_file_names(manifest);
    let mut downloaded = 0;
    for name in &names {
        let local_path = base_dir.join(name);
        if local_path.exists() {
            continue;
        }
        segment_store
            .download_segment(shard, name, &local_path)
            .await?;
        downloaded += 1;
    }
    Ok(downloaded)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::unified::wisckeylsm::types::VlogSegmentMeta;

    #[test]
    fn all_file_names_collects_segments_and_ssts() {
        let mut m = UnifiedManifest::new();
        m.committed.sealed_vlog_segments.push(VlogSegmentMeta {
            segment_id: 0,
            path: "/data/comb_comm_vlog_0000.dat".into(),
            views: vec![],
            total_size: 100,
        });
        m.mvcc.sst_metas.push(crate::unified::wisckeylsm::sst::SstMeta {
            id: 0,
            path: "/data/comb_mvcc_sst_0000.db".into(),
            num_entries: 10,
        });
        let names = all_file_names(&m);
        assert_eq!(names, vec!["comb_comm_vlog_0000.dat", "comb_mvcc_sst_0000.db"]);
    }
}
