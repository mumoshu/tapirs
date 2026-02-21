use super::aligned_buf::{AlignedBuf, round_up};
use super::disk_io::DiskIo;
use super::error::StorageError;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Bitcode-serialized u64 is always 8 bytes.
const TS_SIZE: usize = 8;

/// Fixed overhead per vlog entry: key_len(4) + value_len(4) + ts_bytes(8) + crc_kts(4) + crc_val(4).
const ENTRY_OVERHEAD: usize = 24;

/// Pointer into the value log, stored in the LSM index.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValuePointer {
    pub segment_id: u64,
    pub offset: u64,
    /// Total on-disk entry size (header + key + ts + crc_kts + value + crc_val).
    pub length: u32,
}

/// Deserialized value-log entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VlogEntry<K, V, TS> {
    pub key: K,
    pub timestamp: TS,
    pub value: Option<V>,
}

/// An append-only segment file in the value log.
pub struct VlogSegment<IO: DiskIo> {
    pub id: u64,
    io: IO,
    write_offset: u64,
    path: PathBuf,
}

impl<IO: DiskIo> VlogSegment<IO> {
    /// Open or create a segment file.
    pub fn open(id: u64, path: PathBuf, flags: super::disk_io::OpenFlags) -> Result<Self, StorageError> {
        let io = IO::open(&path, flags)?;
        Ok(Self {
            id,
            io,
            write_offset: 0,
            path,
        })
    }

    /// Open an existing segment at a known write offset (for recovery).
    pub fn open_at(
        id: u64,
        path: PathBuf,
        write_offset: u64,
        flags: super::disk_io::OpenFlags,
    ) -> Result<Self, StorageError> {
        let io = IO::open(&path, flags)?;
        Ok(Self {
            id,
            io,
            write_offset,
            path,
        })
    }

    pub fn write_offset(&self) -> u64 {
        self.write_offset
    }

    /// Set the write offset for the next append.
    ///
    /// Used during recovery to position the append cursor past recovered
    /// entries, preventing new appends from overwriting recovered vlog data
    /// whose ValuePointers are still referenced by SSTs.
    pub(crate) fn set_write_offset(&mut self, offset: u64) {
        self.write_offset = offset;
    }

    pub fn path(&self) -> &PathBuf {
        &self.path
    }

    /// Append an entry to the segment, returning a `ValuePointer`.
    ///
    /// On-disk entry layout (per WiscKey, Lu et al., FAST'16, Section 3.3.2, Figure 5):
    ///
    /// [key_len(4) | value_len(4) | key_bytes | ts_bytes(8) | crc_kts(4) | value_bytes | crc_val(4)]
    ///
    /// The paper specifies `(key size, value size, key, value)` — per-field sizes enabling
    /// field-addressable reads. We add a fixed-size timestamp (u64 = 8 bytes, always)
    /// between key and value, plus two CRCs for independent verification:
    ///
    /// - crc_kts covers key_bytes | ts_bytes: enables GC to verify key integrity
    ///   without reading value bytes. Without this, GC would need to read the entire
    ///   entry (including potentially large values) just to verify the key, negating
    ///   WiscKey's I/O separation benefit. A corrupt key that GC cannot detect could
    ///   cause it to classify a live entry as dead, freeing vLog space while the LSM
    ///   still holds a pointer to it — resulting in data loss on subsequent reads.
    ///
    /// - crc_val covers value_bytes: detects silent value corruption (bit-rot) that
    ///   the key-match check alone cannot catch. Without this, a corrupted value
    ///   with an intact key would be returned to the caller as valid data.
    ///
    /// No padding (unlike prior 4 KiB-aligned format). The paper uses tightly packed
    /// entries (Section 3.4.1 recommends a write buffer for batching small writes).
    /// With 4 KiB padding, a typical ~20-byte TAPIR entry (16-byte key + 8-byte ts
    /// + small value + 24 bytes overhead ≈ 68 bytes) would occupy 4096 bytes on disk,
    /// wasting ~98% of each block — write amplification that defeats WiscKey's core
    /// design goal of reducing I/O amplification.
    ///
    /// **Consumers of this format** (correctness of the format is defined by how
    /// these consumers parse and verify the data appended here):
    ///
    /// - `read()`: Reads `ptr.length` bytes, parses all fields, verifies both CRCs,
    ///   deserializes key + timestamp + value. Used internally by `read_verified()`.
    ///
    /// - `read_verified()`: Wraps `read()` with three verification checks (range,
    ///   CRC, key-match) per WiscKey Section 3.3.3. Called by `resolve_value()` in
    ///   DiskStore on every user-facing read. Returns specific error variants
    ///   (`VLogDataMissing`, `VLogKeyCrcMismatch`, `VLogValueCrcMismatch`,
    ///   `VLogKeyMismatch`) instead of the paper's blanket "not found" — see
    ///   `read_verified()` comment for the quorum-safety rationale.
    ///
    /// - `read_key_ts()`: Reads only `8 + key_len + 8 + 4` bytes (header + key +
    ///   ts + crc_kts), skipping value bytes entirely. Used by GC to check if an
    ///   entry is still live in the LSM without I/O for the value.
    ///
    /// - `recover_entries()`: Scans the vlog sequentially from a starting offset,
    ///   using key_len + value_len to compute entry boundaries and both CRCs to
    ///   verify integrity. Stops at the first invalid entry (file system prefix
    ///   property). Used by crash recovery and GC.
    pub async fn append<K, V, TS>(
        &mut self,
        entry: &VlogEntry<K, V, TS>,
    ) -> Result<ValuePointer, StorageError>
    where
        K: Serialize,
        V: Serialize,
        TS: Serialize,
    {
        let key_bytes =
            bitcode::serialize(&entry.key).map_err(|e| StorageError::Codec(e.to_string()))?;
        let ts_bytes = bitcode::serialize(&entry.timestamp)
            .map_err(|e| StorageError::Codec(e.to_string()))?;
        let value_bytes = bitcode::serialize(&entry.value)
            .map_err(|e| StorageError::Codec(e.to_string()))?;

        debug_assert_eq!(ts_bytes.len(), TS_SIZE);

        let key_len = key_bytes.len() as u32;
        let value_len = value_bytes.len() as u32;

        // CRC over key+ts region (for GC key-only verification).
        let mut kts_hasher = crc32fast::Hasher::new();
        kts_hasher.update(&key_bytes);
        kts_hasher.update(&ts_bytes);
        let crc_kts = kts_hasher.finalize();

        // CRC over value region.
        let crc_val = crc32fast::hash(&value_bytes);

        let total_len = ENTRY_OVERHEAD + key_bytes.len() + value_bytes.len();

        let mut buf = AlignedBuf::new(total_len);
        let slice = buf.as_full_slice_mut();
        let mut pos = 0;

        // Header: key_len(4) | value_len(4)
        slice[pos..pos + 4].copy_from_slice(&key_len.to_le_bytes());
        pos += 4;
        slice[pos..pos + 4].copy_from_slice(&value_len.to_le_bytes());
        pos += 4;

        // Key + timestamp + crc_kts
        slice[pos..pos + key_bytes.len()].copy_from_slice(&key_bytes);
        pos += key_bytes.len();
        slice[pos..pos + TS_SIZE].copy_from_slice(&ts_bytes);
        pos += TS_SIZE;
        slice[pos..pos + 4].copy_from_slice(&crc_kts.to_le_bytes());
        pos += 4;

        // Value + crc_val
        slice[pos..pos + value_bytes.len()].copy_from_slice(&value_bytes);
        pos += value_bytes.len();
        slice[pos..pos + 4].copy_from_slice(&crc_val.to_le_bytes());

        buf.set_len(total_len);

        let offset = self.write_offset;
        self.io.pwrite(&buf, offset).await?;
        self.write_offset += total_len as u64;

        Ok(ValuePointer {
            segment_id: self.id,
            offset,
            length: total_len as u32,
        })
    }

    /// Read and verify an entry at the given pointer.
    ///
    /// Parses the per-field format, verifies both CRCs (crc_kts and crc_val),
    /// and deserializes all fields. CRC failures return specific error variants
    /// (VLogKeyCrcMismatch, VLogValueCrcMismatch) rather than a generic Corruption.
    pub async fn read<K, V, TS>(
        &self,
        ptr: &ValuePointer,
    ) -> Result<VlogEntry<K, V, TS>, StorageError>
    where
        K: for<'de> Deserialize<'de>,
        V: for<'de> Deserialize<'de>,
        TS: for<'de> Deserialize<'de>,
    {
        let total = ptr.length as usize;
        if total < ENTRY_OVERHEAD {
            return Err(StorageError::Corruption {
                file: self.path.display().to_string(),
                offset: ptr.offset,
                expected_crc: 0,
                actual_crc: 0,
            });
        }

        let read_size = round_up(total);
        let mut buf = AlignedBuf::new(read_size);
        self.io.pread(&mut buf, ptr.offset).await?;

        let raw = buf.as_full_slice();

        // Parse header
        let key_len = u32::from_le_bytes([raw[0], raw[1], raw[2], raw[3]]) as usize;
        let value_len = u32::from_le_bytes([raw[4], raw[5], raw[6], raw[7]]) as usize;

        let expected_total = ENTRY_OVERHEAD + key_len + value_len;
        if expected_total != total {
            return Err(StorageError::Corruption {
                file: self.path.display().to_string(),
                offset: ptr.offset,
                expected_crc: 0,
                actual_crc: 0,
            });
        }

        // Extract regions
        let key_start = 8;
        let key_end = key_start + key_len;
        let ts_start = key_end;
        let ts_end = ts_start + TS_SIZE;
        let crc_kts_start = ts_end;
        let val_start = crc_kts_start + 4;
        let val_end = val_start + value_len;
        let crc_val_start = val_end;

        let key_bytes = &raw[key_start..key_end];
        let ts_bytes = &raw[ts_start..ts_end];
        let value_bytes = &raw[val_start..val_end];

        // Verify crc_kts
        let stored_crc_kts = u32::from_le_bytes([
            raw[crc_kts_start],
            raw[crc_kts_start + 1],
            raw[crc_kts_start + 2],
            raw[crc_kts_start + 3],
        ]);
        let mut kts_hasher = crc32fast::Hasher::new();
        kts_hasher.update(key_bytes);
        kts_hasher.update(ts_bytes);
        let actual_crc_kts = kts_hasher.finalize();

        if stored_crc_kts != actual_crc_kts {
            return Err(StorageError::VLogKeyCrcMismatch {
                file: self.path.display().to_string(),
                offset: ptr.offset,
                expected_crc: stored_crc_kts,
                actual_crc: actual_crc_kts,
            });
        }

        // Verify crc_val
        let stored_crc_val = u32::from_le_bytes([
            raw[crc_val_start],
            raw[crc_val_start + 1],
            raw[crc_val_start + 2],
            raw[crc_val_start + 3],
        ]);
        let actual_crc_val = crc32fast::hash(value_bytes);

        if stored_crc_val != actual_crc_val {
            return Err(StorageError::VLogValueCrcMismatch {
                file: self.path.display().to_string(),
                offset: ptr.offset,
                expected_crc: stored_crc_val,
                actual_crc: actual_crc_val,
            });
        }

        // Deserialize fields separately
        let key: K =
            bitcode::deserialize(key_bytes).map_err(|e| StorageError::Codec(e.to_string()))?;
        let timestamp: TS =
            bitcode::deserialize(ts_bytes).map_err(|e| StorageError::Codec(e.to_string()))?;
        let value: Option<V> =
            bitcode::deserialize(value_bytes).map_err(|e| StorageError::Codec(e.to_string()))?;

        Ok(VlogEntry {
            key,
            timestamp,
            value,
        })
    }

    /// Read only the key and timestamp from a vlog entry, skipping value bytes.
    ///
    /// Used by GC to check if an entry is still live in the LSM without reading
    /// potentially large value data. Per WiscKey (Lu et al., FAST'16, Section 3.3.2),
    /// the per-field format `(key size, value size, key, value)` enables reading just
    /// the key portion. The crc_kts covers key_bytes | ts_bytes and is verified
    /// independently of the value — a corrupt key would cause GC to misclassify
    /// a live entry as dead, freeing vLog space while the LSM still references it.
    pub async fn read_key_ts<K, TS>(
        &self,
        ptr: &ValuePointer,
    ) -> Result<(K, TS), StorageError>
    where
        K: for<'de> Deserialize<'de>,
        TS: for<'de> Deserialize<'de>,
    {
        // Read the 8-byte header to get key_len
        let header_size = round_up(8);
        let mut header_buf = AlignedBuf::new(header_size);
        self.io.pread(&mut header_buf, ptr.offset).await?;
        let header = header_buf.as_full_slice();
        let key_len = u32::from_le_bytes([header[0], header[1], header[2], header[3]]) as usize;

        // Read header + key + ts + crc_kts
        let kts_size = 8 + key_len + TS_SIZE + 4;
        let read_size = round_up(kts_size);
        let mut buf = AlignedBuf::new(read_size);
        self.io.pread(&mut buf, ptr.offset).await?;
        let raw = buf.as_full_slice();

        let key_start = 8;
        let key_end = key_start + key_len;
        let ts_start = key_end;
        let ts_end = ts_start + TS_SIZE;
        let crc_kts_start = ts_end;

        let key_bytes = &raw[key_start..key_end];
        let ts_bytes = &raw[ts_start..ts_end];

        // Verify crc_kts
        let stored_crc = u32::from_le_bytes([
            raw[crc_kts_start],
            raw[crc_kts_start + 1],
            raw[crc_kts_start + 2],
            raw[crc_kts_start + 3],
        ]);
        let mut kts_hasher = crc32fast::Hasher::new();
        kts_hasher.update(key_bytes);
        kts_hasher.update(ts_bytes);
        let actual_crc = kts_hasher.finalize();

        if stored_crc != actual_crc {
            return Err(StorageError::VLogKeyCrcMismatch {
                file: self.path.display().to_string(),
                offset: ptr.offset,
                expected_crc: stored_crc,
                actual_crc,
            });
        }

        let key: K =
            bitcode::deserialize(key_bytes).map_err(|e| StorageError::Codec(e.to_string()))?;
        let timestamp: TS =
            bitcode::deserialize(ts_bytes).map_err(|e| StorageError::Codec(e.to_string()))?;

        Ok((key, timestamp))
    }

    /// Scan and recover entries from a starting offset for crash recovery.
    ///
    /// Uses the key_len and value_len header fields to compute exact entry
    /// boundaries, enabling recovery of entries of any size. Without per-field
    /// length headers, recovery would need to guess entry boundaries — either
    /// by assuming a fixed block size (which silently loses entries that span
    /// multiple blocks, since the CRC lives past the assumed boundary) or by
    /// brute-force scanning every possible payload length for CRC matches
    /// (O(n²) per entry and susceptible to false CRC collisions at ~1/2³²
    /// per trial × ~4088 trials ≈ 1 in 10⁶ entries).
    ///
    /// Both CRCs (crc_kts and crc_val) must pass for an entry to be accepted.
    /// Recovery stops at the first invalid entry per the file system prefix
    /// property: if append X was lost in a crash, all appends after X were
    /// also lost (WiscKey, Lu et al., FAST'16, Section 3.3.3, paragraph
    /// starting "Since values are appended sequentially").
    pub async fn recover_entries<K, V, TS>(
        &self,
        from_offset: u64,
    ) -> Result<Vec<(VlogEntry<K, V, TS>, ValuePointer)>, StorageError>
    where
        K: for<'de> Deserialize<'de>,
        V: for<'de> Deserialize<'de>,
        TS: for<'de> Deserialize<'de>,
    {
        let mut recovered = Vec::new();
        let file_size = std::fs::metadata(&self.path)?.len();
        let mut current_offset = from_offset;

        while current_offset + 8 <= file_size {
            // Read header (8 bytes) to get key_len and value_len.
            let header_read_size = round_up(8);
            let mut header_buf = AlignedBuf::new(header_read_size);
            self.io.pread(&mut header_buf, current_offset).await?;
            let header = header_buf.as_full_slice();

            let key_len =
                u32::from_le_bytes([header[0], header[1], header[2], header[3]]) as usize;
            let value_len =
                u32::from_le_bytes([header[4], header[5], header[6], header[7]]) as usize;

            let total_len = ENTRY_OVERHEAD + key_len + value_len;

            // Partial write: not enough data for the full entry.
            if current_offset + total_len as u64 > file_size {
                break;
            }

            // Read the full entry.
            let ptr = ValuePointer {
                segment_id: self.id,
                offset: current_offset,
                length: total_len as u32,
            };

            match self.read::<K, V, TS>(&ptr).await {
                Ok(entry) => {
                    recovered.push((entry, ptr));
                    current_offset += total_len as u64;
                }
                Err(StorageError::VLogKeyCrcMismatch { .. })
                | Err(StorageError::VLogValueCrcMismatch { .. })
                | Err(StorageError::Corruption { .. })
                | Err(StorageError::Codec(_)) => {
                    // CRC failure or codec error — stop recovery (prefix property).
                    break;
                }
                Err(e) => return Err(e), // I/O errors propagate.
            }
        }

        Ok(recovered)
    }

    /// Sync the segment to disk.
    pub async fn sync(&self) -> Result<(), StorageError> {
        self.io.fsync().await
    }

    pub fn close(self) {
        self.io.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::disk_io::{BufferedIo, OpenFlags};
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn append_and_read_roundtrip() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();
        let flags = OpenFlags {
            create: true,
            direct: false,
        };

        let mut seg = VlogSegment::<BufferedIo>::open(1, path, flags).unwrap();

        let entry = VlogEntry::<String, String, u64> {
            key: "hello".into(),
            timestamp: 42,
            value: Some("world".into()),
        };

        let ptr = seg.append(&entry).await.unwrap();
        assert_eq!(ptr.segment_id, 1);
        assert_eq!(ptr.offset, 0);

        let read_back: VlogEntry<String, String, u64> = seg.read(&ptr).await.unwrap();
        assert_eq!(read_back.key, "hello");
        assert_eq!(read_back.timestamp, 42);
        assert_eq!(read_back.value, Some("world".into()));
    }

    #[tokio::test]
    async fn multiple_appends() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();
        let flags = OpenFlags {
            create: true,
            direct: false,
        };

        let mut seg = VlogSegment::<BufferedIo>::open(1, path, flags).unwrap();

        let mut ptrs = Vec::new();
        for i in 0u64..5 {
            let entry = VlogEntry::<u64, String, u64> {
                key: i,
                timestamp: i * 10,
                value: Some(format!("val-{i}")),
            };
            ptrs.push(seg.append(&entry).await.unwrap());
        }

        // Read them back in reverse order.
        for (i, ptr) in ptrs.iter().enumerate().rev() {
            let e: VlogEntry<u64, String, u64> = seg.read(ptr).await.unwrap();
            assert_eq!(e.key, i as u64);
            assert_eq!(e.value, Some(format!("val-{i}")));
        }
    }

    #[tokio::test]
    async fn corruption_detected() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();
        let flags = OpenFlags {
            create: true,
            direct: false,
        };

        let mut seg = VlogSegment::<BufferedIo>::open(1, path.clone(), flags).unwrap();
        let entry = VlogEntry::<String, String, u64> {
            key: "key".into(),
            timestamp: 1,
            value: Some("value".into()),
        };
        let ptr = seg.append(&entry).await.unwrap();
        seg.sync().await.unwrap();

        // Corrupt a byte in the file.
        use std::io::{Seek, Write, SeekFrom};
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap();
        file.seek(SeekFrom::Start(ptr.offset + 2)).unwrap();
        file.write_all(&[0xFF]).unwrap();
        file.flush().unwrap();
        drop(file);

        // Re-open and read.
        let seg2 = VlogSegment::<BufferedIo>::open(1, path, flags).unwrap();
        let result: Result<VlogEntry<String, String, u64>, _> = seg2.read(&ptr).await;
        assert!(matches!(result, Err(StorageError::Corruption { .. })));
    }

    // ========== Vlog Recovery Tests ==========

    #[tokio::test]
    async fn partial_write_recovery_stops_at_truncation() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();
        let flags = OpenFlags {
            create: true,
            direct: false,
        };

        // Write 5 entries (tightly packed, no padding).
        let mut seg = VlogSegment::<BufferedIo>::open(1, path.clone(), flags).unwrap();
        let mut offsets = Vec::new();
        for i in 0u64..5 {
            let entry = VlogEntry::<String, String, u64> {
                key: format!("key-{i}"),
                timestamp: i + 1,
                value: Some(format!("value-{i}")),
            };
            let ptr = seg.append(&entry).await.unwrap();
            offsets.push((ptr.offset, ptr.length));
        }
        seg.sync().await.unwrap();
        drop(seg);

        // Truncate mid-way through entry #2 — simulates crash.
        let truncate_at = offsets[2].0 + 4; // 4 bytes into entry #2
        let file = std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap();
        file.set_len(truncate_at).unwrap();
        drop(file);

        // Recovery should find exactly 2 entries (entries 0 and 1).
        let seg2 = VlogSegment::<BufferedIo>::open(1, path, flags).unwrap();
        let recovered = seg2.recover_entries::<String, String, u64>(0).await.unwrap();

        assert_eq!(recovered.len(), 2, "Should recover 2 entries before truncation point");
        assert_eq!(recovered[0].0.key, "key-0");
        assert_eq!(recovered[1].0.key, "key-1");
    }

    #[tokio::test]
    async fn partial_write_at_offset_0_recover_nothing() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();
        let flags = OpenFlags {
            create: true,
            direct: false,
        };

        // Write 3 entries.
        let mut seg = VlogSegment::<BufferedIo>::open(1, path.clone(), flags).unwrap();
        for i in 0u64..3 {
            let entry = VlogEntry::<String, String, u64> {
                key: format!("key-{i}"),
                timestamp: i + 1,
                value: Some(format!("value-{i}")),
            };
            seg.append(&entry).await.unwrap();
        }
        seg.sync().await.unwrap();
        drop(seg);

        // Truncate to 4 bytes — simulates crash during first entry write.
        let file = std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap();
        file.set_len(4).unwrap();
        drop(file);

        // Recovery should find 0 entries (not enough for header + data).
        let seg2 = VlogSegment::<BufferedIo>::open(1, path, flags).unwrap();
        let recovered = seg2.recover_entries::<String, String, u64>(0).await.unwrap();

        assert_eq!(recovered.len(), 0, "Should recover nothing when first entry is incomplete");
    }

    #[tokio::test]
    async fn short_write_crc_fails_recovery_stops() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();
        let flags = OpenFlags {
            create: true,
            direct: false,
        };

        // Write 3 complete entries
        let mut seg = VlogSegment::<BufferedIo>::open(1, path.clone(), flags).unwrap();
        let mut offsets = Vec::new();
        for i in 0u64..3 {
            let entry = VlogEntry::<String, String, u64> {
                key: format!("key-{i}"),
                timestamp: i + 1,
                value: Some(format!("value-{i}")),
            };
            let ptr = seg.append(&entry).await.unwrap();
            offsets.push((ptr.offset, ptr.length));
        }
        seg.sync().await.unwrap();
        drop(seg);

        // Truncate mid-way through entry #2.
        let truncate_at = offsets[2].0 + 4;
        std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(truncate_at)
            .unwrap();

        // Recover — should recover first 2 entries, stop at partial third.
        let seg2 = VlogSegment::<BufferedIo>::open(1, path, flags).unwrap();
        let recovered = seg2.recover_entries::<String, String, u64>(0).await.unwrap();

        assert_eq!(recovered.len(), 2, "Should recover 2 complete entries");

        for (i, (entry, _ptr)) in recovered.iter().enumerate() {
            assert_eq!(entry.key, format!("key-{i}"));
        }
    }

    #[tokio::test]
    async fn append_read_per_field_format() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();
        let flags = OpenFlags {
            create: true,
            direct: false,
        };

        let mut seg = VlogSegment::<BufferedIo>::open(1, path, flags).unwrap();
        let entry = VlogEntry::<String, String, u64> {
            key: "hello".into(),
            timestamp: 42,
            value: Some("world".into()),
        };

        let ptr = seg.append(&entry).await.unwrap();

        // Entry should be tightly packed (no 4 KiB padding).
        // key "hello" ≈ 6 bytes bitcode, value Some("world") ≈ 7 bytes bitcode,
        // ts = 8 bytes. Total = 24 + key_len + value_len.
        assert!(ptr.length < 100, "Entry should be tightly packed, got {} bytes", ptr.length);
        assert_eq!(ptr.offset, 0);

        let read_back: VlogEntry<String, String, u64> = seg.read(&ptr).await.unwrap();
        assert_eq!(read_back.key, "hello");
        assert_eq!(read_back.timestamp, 42);
        assert_eq!(read_back.value, Some("world".into()));
    }

    #[tokio::test]
    async fn recover_multi_block_entries() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();
        let flags = OpenFlags {
            create: true,
            direct: false,
        };

        let mut seg = VlogSegment::<BufferedIo>::open(1, path.clone(), flags).unwrap();

        // Small entry
        let e1 = VlogEntry::<String, String, u64> {
            key: "small".into(),
            timestamp: 1,
            value: Some("tiny".into()),
        };
        seg.append(&e1).await.unwrap();

        // Large entry (5 KB value — would have been lost with old format)
        let large_value = "x".repeat(5000);
        let e2 = VlogEntry::<String, String, u64> {
            key: "big".into(),
            timestamp: 2,
            value: Some(large_value.clone()),
        };
        seg.append(&e2).await.unwrap();

        // Another small entry after the large one
        let e3 = VlogEntry::<String, String, u64> {
            key: "after-big".into(),
            timestamp: 3,
            value: Some("also small".into()),
        };
        seg.append(&e3).await.unwrap();

        seg.sync().await.unwrap();
        drop(seg);

        // Recover all 3 — the old format would lose e2 and e3.
        let seg2 = VlogSegment::<BufferedIo>::open(1, path, flags).unwrap();
        let recovered = seg2.recover_entries::<String, String, u64>(0).await.unwrap();

        assert_eq!(recovered.len(), 3, "Should recover all 3 entries including large one");
        assert_eq!(recovered[0].0.key, "small");
        assert_eq!(recovered[1].0.key, "big");
        assert_eq!(recovered[1].0.value, Some(large_value));
        assert_eq!(recovered[2].0.key, "after-big");
    }

    #[tokio::test]
    async fn recover_partial_write() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();
        let flags = OpenFlags {
            create: true,
            direct: false,
        };

        let mut seg = VlogSegment::<BufferedIo>::open(1, path.clone(), flags).unwrap();

        let e1 = VlogEntry::<String, String, u64> {
            key: "a".into(),
            timestamp: 1,
            value: Some("val-a".into()),
        };
        let ptr1 = seg.append(&e1).await.unwrap();

        let e2 = VlogEntry::<String, String, u64> {
            key: "b".into(),
            timestamp: 2,
            value: Some("val-b".into()),
        };
        let ptr2 = seg.append(&e2).await.unwrap();

        seg.sync().await.unwrap();
        drop(seg);

        // Truncate mid-way through entry 2
        let truncate_at = ptr2.offset + 8; // past the header, into key bytes
        std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(truncate_at)
            .unwrap();

        let seg2 = VlogSegment::<BufferedIo>::open(1, path, flags).unwrap();
        let recovered = seg2.recover_entries::<String, String, u64>(0).await.unwrap();

        assert_eq!(recovered.len(), 1, "Should recover only entry before truncation");
        assert_eq!(recovered[0].0.key, "a");
    }

    #[tokio::test]
    async fn read_key_ts_skips_value() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();
        let flags = OpenFlags {
            create: true,
            direct: false,
        };

        let mut seg = VlogSegment::<BufferedIo>::open(1, path, flags).unwrap();
        let entry = VlogEntry::<String, String, u64> {
            key: "mykey".into(),
            timestamp: 99,
            value: Some("large-value-not-read".into()),
        };

        let ptr = seg.append(&entry).await.unwrap();

        // read_key_ts should return key and timestamp, skipping value.
        let (key, ts): (String, u64) = seg.read_key_ts(&ptr).await.unwrap();
        assert_eq!(key, "mykey");
        assert_eq!(ts, 99);
    }
}
