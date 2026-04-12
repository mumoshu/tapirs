use super::sst::SstMeta;
use super::types::VlogSegmentMeta;
use crate::storage::io::disk_io::DiskIo;
use crate::storage::io::error::StorageError;
use serde::{Deserialize, Serialize};
use std::path::Path;

/// Per-LSM vlog + SST metadata, stored inside UnifiedManifest.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LsmManifestData {
    pub active_segment_id: u64,
    pub active_write_offset: u64,
    pub sealed_vlog_segments: Vec<VlogSegmentMeta>,
    pub next_segment_id: u64,
    pub sst_metas: Vec<SstMeta>,
    pub next_sst_id: u64,
}

impl LsmManifestData {
    pub fn new() -> Self {
        Self {
            active_segment_id: 0,
            active_write_offset: 0,
            sealed_vlog_segments: Vec::new(),
            next_segment_id: 1,
            sst_metas: Vec::new(),
            next_sst_id: 0,
        }
    }
}

/// Persisted metadata for the unified storage engine.
///
/// Written atomically via write-temp-rename.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnifiedManifest {
    /// Current view number.
    pub current_view: u64,
    /// Committed transaction vlog metadata (also used by IR op log).
    pub committed: LsmManifestData,
    /// Prepared transaction vlog metadata.
    pub prepared: LsmManifestData,
    /// IR operation log vlog metadata.
    pub ir: LsmManifestData,
    /// MVCC VlogLsm metadata (key→timestamp→txn_id index).
    pub mvcc: LsmManifestData,
    /// Highest timestamp seen across all RO read operations.
    /// Used as a conservative global watermark on recovery: any prepare with
    /// commit_ts < max_read_time → Retry. Subsumes all lost range_reads.
    pub max_read_time: Option<u64>,
    /// Number of entries in committed VlogLsm (commits + aborts).
    /// Persisted so PersistentTapirStore can recover txn_log_len after restart.
    pub txn_log_count: u64,
    /// Reserved for future use (recovery replay).
    pub replay_start_offset: u64,
    /// IR inconsistent entries VlogLsm metadata (PersistentIrRecordStore).
    #[serde(default = "LsmManifestData::new")]
    pub ir_inc: LsmManifestData,
    /// IR consensus entries VlogLsm metadata (PersistentIrRecordStore).
    #[serde(default = "LsmManifestData::new")]
    pub ir_con: LsmManifestData,
    /// Cluster type: "data" for data shards, "discovery" for discovery shards.
    /// Prevents S3 prefix collisions — clone_from_remote_lazy validates this.
    pub cluster_type: String,
    /// CRC32 checksum.
    pub checksum: u32,
}

impl UnifiedManifest {
    pub fn new() -> Self {
        Self {
            current_view: 0,
            committed: LsmManifestData::new(),
            prepared: LsmManifestData::new(),
            ir: LsmManifestData::new(),
            mvcc: LsmManifestData::new(),
            max_read_time: None,
            txn_log_count: 0,
            replay_start_offset: 0,
            ir_inc: LsmManifestData::new(),
            ir_con: LsmManifestData::new(),
            cluster_type: "data".into(),
            checksum: 0,
        }
    }

    /// Write the manifest atomically to disk.
    pub fn save<IO: DiskIo>(&self, dir: &Path) -> Result<(), StorageError> {
        let manifest_path = dir.join("UNIFIED_MANIFEST");
        let tmp_path = dir.join("UNIFIED_MANIFEST.tmp");

        let mut m = self.clone();
        m.checksum = 0;
        let payload =
            bitcode::serialize(&m).map_err(|e| StorageError::Codec(e.to_string()))?;
        m.checksum = crc32fast::hash(&payload);

        let final_bytes =
            bitcode::serialize(&m).map_err(|e| StorageError::Codec(e.to_string()))?;

        IO::write_file(&tmp_path, &final_bytes)?;
        IO::sync_path(&tmp_path)?;
        IO::rename(&tmp_path, &manifest_path)?;
        IO::sync_path(dir)?;

        Ok(())
    }

    /// Load the manifest from disk. Returns None if no manifest exists.
    pub fn load<IO: DiskIo>(dir: &Path) -> Result<Option<Self>, StorageError> {
        let manifest_path = dir.join("UNIFIED_MANIFEST");
        if !IO::exists(&manifest_path) {
            return Ok(None);
        }

        let bytes = IO::read_file(&manifest_path)?;
        let manifest: UnifiedManifest =
            bitcode::deserialize(&bytes).map_err(|e| StorageError::Codec(e.to_string()))?;

        // Verify checksum.
        let mut check = manifest.clone();
        let stored_crc = check.checksum;
        check.checksum = 0;
        let payload =
            bitcode::serialize(&check).map_err(|e| StorageError::Codec(e.to_string()))?;
        let actual_crc = crc32fast::hash(&payload);

        if stored_crc != actual_crc {
            return Err(StorageError::Corruption {
                file: manifest_path.display().to_string(),
                offset: 0,
                expected_crc: stored_crc,
                actual_crc,
            });
        }

        Ok(Some(manifest))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_cluster_type_roundtrip() {
        let m = UnifiedManifest::new();
        assert_eq!(m.cluster_type, "data");

        let bytes = bitcode::serialize(&m).unwrap();
        let loaded: UnifiedManifest = bitcode::deserialize(&bytes).unwrap();
        assert_eq!(loaded.cluster_type, "data");
    }

    #[test]
    fn manifest_without_cluster_type_fails_to_deserialize() {
        // Serialize a manifest, then truncate to simulate an old format
        // that lacks the cluster_type field. bitcode is positional — the
        // shorter bytes will fail to deserialize.
        let m = UnifiedManifest::new();
        let bytes = bitcode::serialize(&m).unwrap();

        // Truncate by a few bytes to simulate missing field.
        let truncated = &bytes[..bytes.len() - 10];
        let result = bitcode::deserialize::<UnifiedManifest>(truncated);
        assert!(result.is_err(), "old manifest without cluster_type should fail");
    }
}
