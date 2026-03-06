use super::sst::SstMeta;
use super::types::VlogSegmentMeta;
use crate::mvcc::disk::disk_io::DiskIo;
use crate::mvcc::disk::error::StorageError;
use crate::mvcc::disk::lsm::SSTableMeta;
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
/// Written atomically via write-temp-rename (same strategy as
/// `src/mvcc/disk/manifest.rs`).
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
    /// MVCC SST metadata — L0 level.
    pub mvcc_l0_sstables: Vec<SSTableMeta>,
    /// MVCC SST metadata — L1 level.
    pub mvcc_l1_sstables: Vec<SSTableMeta>,
    /// Next MVCC SST file ID.
    pub next_sst_id: u64,
    /// Highest timestamp seen across all RO read operations.
    /// Used as a conservative global watermark on recovery: any prepare with
    /// commit_ts < max_read_time → Retry. Subsumes all lost range_reads.
    pub max_read_time: Option<u64>,
    /// Reserved for future use (recovery replay).
    pub replay_start_offset: u64,
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
            mvcc_l0_sstables: Vec::new(),
            mvcc_l1_sstables: Vec::new(),
            next_sst_id: 0,
            max_read_time: None,
            replay_start_offset: 0,
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
