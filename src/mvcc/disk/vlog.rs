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
}
