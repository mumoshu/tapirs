use std::path::Path;

use crate::backup::storage::BackupStorage;
use crate::unified::wisckeylsm::manifest::{LsmManifestData, UnifiedManifest};

use super::segment_store::RemoteSegmentStore;

/// Compute the vlog filename for an active segment.
pub(crate) fn active_vlog_name(label: &str, id: u64) -> String {
    if label.is_empty() {
        format!("vlog_seg_{id:04}.dat")
    } else {
        format!("{label}_vlog_{id:04}.dat")
    }
}

/// Collect all segment and SST filenames referenced in an LsmManifestData,
/// including the active segment.
fn lsm_file_names(label: &str, data: &LsmManifestData) -> Vec<String> {
    let mut names = Vec::new();
    // Active segment (may have data from current view).
    names.push(active_vlog_name(label, data.active_segment_id));
    // Sealed segments.
    for seg in &data.sealed_vlog_segments {
        if let Some(name) = seg.path.file_name().and_then(|n| n.to_str()) {
            names.push(name.to_string());
        }
    }
    // SST files.
    for sst in &data.sst_metas {
        if let Some(name) = sst.path.file_name().and_then(|n| n.to_str()) {
            names.push(name.to_string());
        }
    }
    names
}

/// Collect only active segment filenames across the manifest.
pub(crate) fn active_vlog_names(manifest: &UnifiedManifest) -> Vec<String> {
    vec![
        active_vlog_name("comb_comm", manifest.committed.active_segment_id),
        active_vlog_name("comb_prep", manifest.prepared.active_segment_id),
        active_vlog_name("comb_mvcc", manifest.mvcc.active_segment_id),
        active_vlog_name("ir_inc", manifest.ir_inc.active_segment_id),
        active_vlog_name("ir_con", manifest.ir_con.active_segment_id),
    ]
}

/// Collect all segment and SST filenames across the entire manifest,
/// including active segments for each VlogLsm.
pub fn all_file_names(manifest: &UnifiedManifest) -> Vec<String> {
    let mut names = Vec::new();
    names.extend(lsm_file_names("comb_comm", &manifest.committed));
    names.extend(lsm_file_names("comb_prep", &manifest.prepared));
    names.extend(lsm_file_names("comb_mvcc", &manifest.mvcc));
    names.extend(lsm_file_names("ir_inc", &manifest.ir_inc));
    names.extend(lsm_file_names("ir_con", &manifest.ir_con));
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
        // Includes active segments for all 5 VlogLsms + the sealed segment + the SST.
        assert!(names.contains(&"comb_comm_vlog_0000.dat".to_string()));
        assert!(names.contains(&"comb_mvcc_sst_0000.db".to_string()));
        // Active segments for VlogLsms that have no sealed data:
        assert!(names.contains(&"comb_prep_vlog_0000.dat".to_string()));
        assert!(names.contains(&"ir_inc_vlog_0000.dat".to_string()));
        assert!(names.contains(&"ir_con_vlog_0000.dat".to_string()));
    }
}
