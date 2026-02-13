use super::error::StorageError;
use super::lsm::SSTableMeta;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// Persistent manifest recording the state of the storage engine.
///
/// Updated atomically via write-temp-fsync-rename.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    /// L0 SSTable metadata.
    pub l0_sstables: Vec<SSTableMeta>,
    /// L1 SSTable metadata.
    pub l1_sstables: Vec<SSTableMeta>,
    /// Active vlog segment IDs.
    pub vlog_segment_ids: Vec<u64>,
    /// Write offset of the current (latest) vlog segment.
    pub vlog_write_offset: u64,
    /// Next SSTable ID to allocate.
    pub next_sst_id: u64,
    /// Next vlog segment ID to allocate.
    pub next_segment_id: u64,
    /// CRC32 over the manifest data (for integrity).
    pub checksum: u32,
}

impl Manifest {
    /// Create a new empty manifest.
    pub fn new() -> Self {
        Self {
            l0_sstables: Vec::new(),
            l1_sstables: Vec::new(),
            vlog_segment_ids: vec![0],
            vlog_write_offset: 0,
            next_sst_id: 0,
            next_segment_id: 1,
            checksum: 0,
        }
    }

    /// Write the manifest atomically to disk.
    ///
    /// Strategy: write to temp file, fsync, rename over the real file.
    pub fn save(&self, dir: &Path) -> Result<(), StorageError> {
        let manifest_path = dir.join("MANIFEST");
        let tmp_path = dir.join("MANIFEST.tmp");

        // Serialize without checksum first to compute it.
        let mut m = self.clone();
        m.checksum = 0;
        let payload =
            bitcode::serialize(&m).map_err(|e| StorageError::Codec(e.to_string()))?;
        m.checksum = crc32fast::hash(&payload);

        let final_bytes =
            bitcode::serialize(&m).map_err(|e| StorageError::Codec(e.to_string()))?;

        std::fs::write(&tmp_path, &final_bytes)?;
        // fsync the temp file.
        let file = std::fs::File::open(&tmp_path)?;
        file.sync_all()?;
        drop(file);

        // Atomic rename.
        std::fs::rename(&tmp_path, &manifest_path)?;

        // fsync the directory to persist the rename.
        let dir_file = std::fs::File::open(dir)?;
        dir_file.sync_all()?;

        Ok(())
    }

    /// Load the manifest from disk. Returns None if no manifest exists.
    pub fn load(dir: &Path) -> Result<Option<Self>, StorageError> {
        let manifest_path = dir.join("MANIFEST");
        if !manifest_path.exists() {
            return Ok(None);
        }

        let bytes = std::fs::read(&manifest_path)?;
        let manifest: Manifest =
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

    /// Path to the manifest file.
    pub fn path(dir: &Path) -> PathBuf {
        dir.join("MANIFEST")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn save_and_load_roundtrip() {
        let dir = TempDir::new().unwrap();
        let m = Manifest {
            l0_sstables: vec![SSTableMeta {
                id: 0,
                level: 0,
                path: PathBuf::from("sst-000000.db"),
                num_entries: 100,
            }],
            l1_sstables: Vec::new(),
            vlog_segment_ids: vec![0],
            vlog_write_offset: 4096,
            next_sst_id: 1,
            next_segment_id: 1,
            checksum: 0,
        };

        m.save(dir.path()).unwrap();

        let loaded = Manifest::load(dir.path()).unwrap().unwrap();
        assert_eq!(loaded.l0_sstables.len(), 1);
        assert_eq!(loaded.vlog_write_offset, 4096);
        assert_eq!(loaded.next_sst_id, 1);
    }

    #[test]
    fn load_nonexistent_returns_none() {
        let dir = TempDir::new().unwrap();
        let loaded = Manifest::load(dir.path()).unwrap();
        assert!(loaded.is_none());
    }

    #[test]
    fn corruption_detected() {
        let dir = TempDir::new().unwrap();
        let m = Manifest::new();
        m.save(dir.path()).unwrap();

        // Corrupt the manifest file.
        let path = Manifest::path(dir.path());
        let mut bytes = std::fs::read(&path).unwrap();
        if !bytes.is_empty() {
            bytes[0] ^= 0xFF;
        }
        std::fs::write(&path, &bytes).unwrap();

        let result = Manifest::load(dir.path());
        assert!(result.is_err() || matches!(result, Ok(None)));
    }
}
