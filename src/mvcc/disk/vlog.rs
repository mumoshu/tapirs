use super::aligned_buf::{AlignedBuf, round_up};
use super::disk_io::DiskIo;
use super::error::StorageError;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Pointer into the value log, stored in the LSM index.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValuePointer {
    pub segment_id: u64,
    pub offset: u64,
    /// Length of the serialized payload (before padding).
    pub length: u32,
}

/// On-disk value-log entry (serialized with bitcode).
///
/// The checksum covers the `payload` bytes (key + ts + value),
/// and is stored as the last 4 bytes of the serialized block.
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
    /// Layout on disk: `[payload bytes | 4-byte CRC32 | zero-padding to 4 KiB]`
    pub async fn append<K, V, TS>(
        &mut self,
        entry: &VlogEntry<K, V, TS>,
    ) -> Result<ValuePointer, StorageError>
    where
        K: Serialize,
        V: Serialize,
        TS: Serialize,
    {
        // Serialize the entry (without checksum).
        let payload =
            bitcode::serialize(entry).map_err(|e| StorageError::Codec(e.to_string()))?;

        // CRC32 over the payload.
        let crc = crc32fast::hash(&payload);
        let crc_bytes = crc.to_le_bytes();

        // Total on-disk bytes = payload + 4 bytes CRC.
        let total_len = payload.len() + 4;
        let padded = round_up(total_len);

        let mut buf = AlignedBuf::new(padded);
        // Copy payload + CRC into aligned buffer (rest is zero-padded).
        let slice = buf.as_full_slice_mut();
        slice[..payload.len()].copy_from_slice(&payload);
        slice[payload.len()..payload.len() + 4].copy_from_slice(&crc_bytes);
        buf.set_len(total_len);

        let offset = self.write_offset;
        self.io.pwrite(&buf, offset).await?;
        self.write_offset += padded as u64;

        Ok(ValuePointer {
            segment_id: self.id,
            offset,
            length: total_len as u32,
        })
    }

    /// Read and verify an entry at the given pointer.
    pub async fn read<K, V, TS>(
        &self,
        ptr: &ValuePointer,
    ) -> Result<VlogEntry<K, V, TS>, StorageError>
    where
        K: for<'de> Deserialize<'de>,
        V: for<'de> Deserialize<'de>,
        TS: for<'de> Deserialize<'de>,
    {
        let read_size = round_up(ptr.length as usize);
        let mut buf = AlignedBuf::new(read_size);
        self.io.pread(&mut buf, ptr.offset).await?;

        let total = ptr.length as usize;
        if total < 4 {
            return Err(StorageError::Corruption {
                file: self.path.display().to_string(),
                offset: ptr.offset,
                expected_crc: 0,
                actual_crc: 0,
            });
        }

        let raw = buf.as_full_slice();
        let payload = &raw[..total - 4];
        let stored_crc = u32::from_le_bytes([
            raw[total - 4],
            raw[total - 3],
            raw[total - 2],
            raw[total - 1],
        ]);
        let actual_crc = crc32fast::hash(payload);

        if stored_crc != actual_crc {
            return Err(StorageError::Corruption {
                file: self.path.display().to_string(),
                offset: ptr.offset,
                expected_crc: stored_crc,
                actual_crc,
            });
        }

        bitcode::deserialize(payload).map_err(|e| StorageError::Codec(e.to_string()))
    }

    /// Scan and recover entries from a starting offset for crash recovery.
    ///
    /// Returns a vector of (VlogEntry, ValuePointer) tuples for all valid
    /// entries found. Stops at the first CRC mismatch or codec error,
    /// which likely indicates a partial write during crash.
    pub async fn recover_entries<K, V, TS>(
        &self,
        from_offset: u64,
    ) -> Result<Vec<(VlogEntry<K, V, TS>, ValuePointer)>, StorageError>
    where
        K: Serialize + for<'de> Deserialize<'de>,
        V: Serialize + for<'de> Deserialize<'de>,
        TS: Serialize + for<'de> Deserialize<'de>,
    {
        let mut recovered = Vec::new();
        let file_size = std::fs::metadata(&self.path)?.len();
        let mut current_offset = from_offset;

        while current_offset + 4096 <= file_size {
            // Read one 4 KiB block (vlog entries are padded to this size)
            let mut buf = AlignedBuf::new(4096);
            self.io.pread(&mut buf, current_offset).await?;

            let buf_data = buf.as_full_slice();
            if buf_data.len() < 8 {
                // Not enough for minimal entry (bitcode header + CRC)
                break;
            }

            // Find the payload length by scanning for valid CRC.
            // The block format is: [payload | 4-byte CRC | padding to 4 KiB]
            // We don't know payload length in advance, so try each possible length.
            let mut found = false;
            for payload_len in 4..=(4096-4) {
                if payload_len + 4 > buf_data.len() {
                    break;
                }

                let payload = &buf_data[0..payload_len];
                let stored_crc = u32::from_le_bytes([
                    buf_data[payload_len],
                    buf_data[payload_len + 1],
                    buf_data[payload_len + 2],
                    buf_data[payload_len + 3],
                ]);
                let actual_crc = crc32fast::hash(payload);

                if stored_crc == actual_crc {
                    // Found matching CRC, try to deserialize
                    match bitcode::deserialize::<VlogEntry<K, V, TS>>(payload) {
                        Ok(entry) => {
                            // Success! Create ValuePointer and add to recovered list
                            let ptr = ValuePointer {
                                segment_id: self.id,
                                offset: current_offset,
                                length: (payload_len + 4) as u32,
                            };
                            recovered.push((entry, ptr));
                            found = true;
                            break;
                        }
                        Err(_) => {
                            // CRC matched but deserialize failed, keep trying
                            continue;
                        }
                    }
                }
            }

            if !found {
                // No valid entry found in this block, stop recovery
                break;
            }

            current_offset += 4096;
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

    // ========== Vlog Corruption Scenarios Tests ==========

    #[tokio::test]
    async fn partial_write_recovery_stops_at_truncation() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();
        let flags = OpenFlags {
            create: true,
            direct: false,
        };

        // Write 5 entries (each is one 4 KiB block).
        let mut seg = VlogSegment::<BufferedIo>::open(1, path.clone(), flags).unwrap();
        for i in 0u64..5 {
            let entry = VlogEntry::<String, String, u64> {
                key: format!("key-{i}"),
                timestamp: i + 1,
                value: Some(format!("value-{i}")),
            };
            seg.append(&entry).await.unwrap();
        }
        seg.sync().await.unwrap();
        drop(seg);

        // Truncate to 2 blocks (8192 bytes) — simulates crash mid-write of entry #2.
        let file = std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap();
        file.set_len(8192).unwrap();
        drop(file);

        // Recovery should find exactly 2 entries (blocks 0 and 1).
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

        // Truncate to less than one block — simulates crash during first entry write.
        let file = std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap();
        file.set_len(2048).unwrap();
        drop(file);

        // Recovery should find 0 entries (no complete block).
        let seg2 = VlogSegment::<BufferedIo>::open(1, path, flags).unwrap();
        let recovered = seg2.recover_entries::<String, String, u64>(0).await.unwrap();

        assert_eq!(recovered.len(), 0, "Should recover nothing when first block is incomplete");
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

        // Truncate file to simulate short write (less than 4 KiB)
        // Get file size and truncate to an incomplete block
        let metadata = std::fs::metadata(&path).unwrap();
        let file_size = metadata.len();

        // Truncate to middle of third block (simulating partial write)
        if file_size > 8192 + 2000 {
            let truncated_size = 8192 + 2000; // 2 complete blocks + partial third
            std::fs::OpenOptions::new()
                .write(true)
                .open(&path)
                .unwrap()
                .set_len(truncated_size)
                .unwrap();
        }

        // Recover - should recover first 2 entries, stop at partial third
        let seg2 = VlogSegment::<BufferedIo>::open(1, path, flags).unwrap();
        let recovered = seg2.recover_entries::<String, String, u64>(0).await.unwrap();

        assert!(recovered.len() <= 2, "Should recover at most 2 complete entries, got {}", recovered.len());

        // Verify recovered entries
        for (i, (entry, _ptr)) in recovered.iter().enumerate() {
            assert_eq!(entry.key, format!("key-{i}"));
        }
    }
}
