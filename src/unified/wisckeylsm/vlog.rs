use super::types::{ViewRange, VlogPtr};
use crate::mvcc::disk::aligned_buf::{AlignedBuf, round_up};
use crate::mvcc::disk::disk_io::{DiskIo, OpenFlags};
use crate::mvcc::disk::error::StorageError;
use std::path::{Path, PathBuf};

/// Fixed header size: entry_type(1) + entry_len(4) + op_id.client_id(8) + op_id.number(8) = 21.
const HEADER_SIZE: usize = 21;

/// CRC trailer size.
const CRC_SIZE: usize = 4;

/// Minimum entry size: header + crc (no payload).
const MIN_ENTRY_SIZE: usize = HEADER_SIZE + CRC_SIZE;

/// Per-entry overhead in bytes: header (21) + CRC (4) = 25.
/// Used by callers to compute VlogPtr.length = OVERHEAD + payload_len.
pub(crate) const VLOG_RAW_ENTRY_OVERHEAD: usize = MIN_ENTRY_SIZE;

/// Protocol-agnostic decoded VLog entry.
///
/// `entry_type` and the `(id_client, id_number)` header fields are opaque to
/// the storage layer. Protocol modules interpret them according to their own
/// semantics.
#[derive(Debug, Clone)]
pub struct RawVlogEntry {
    pub entry_type: u8,
    pub id_client: u64,
    pub id_number: u64,
    pub payload: Vec<u8>,
}

/// A physical VLog segment file (either active or sealed).
///
/// The struct itself has no `K, V` type parameters because it stores raw
/// bytes on disk.  Methods like `append_entry`, `read_entry`, and
/// `read_prepare` are generic over `K, V` — they serialize typed data to
/// bytes on write and deserialize bytes back to typed data on read.
///
/// This keeps the on-disk format (`PreparePayloadSer`) stable regardless
/// of the Rust types used in memory, and avoids propagating K, V through
/// the segment management code (open, seal, rotate, manifest).
pub(crate) struct VlogSegment<IO: DiskIo> {
    pub(crate) id: u64,
    io: Option<IO>,
    write_offset: u64,
    path: PathBuf,
    pub(crate) views: Vec<ViewRange>,
}

impl<IO: DiskIo> VlogSegment<IO> {
    pub(crate) fn encode_raw_entry(
        entry_type: u8,
        id_client: u64,
        id_number: u64,
        payload_bytes: &[u8],
    ) -> Vec<u8> {
        let total_len = HEADER_SIZE + payload_bytes.len() + CRC_SIZE;
        let mut raw = Vec::with_capacity(total_len);

        raw.push(entry_type);
        raw.extend_from_slice(&(total_len as u32).to_le_bytes());
        raw.extend_from_slice(&id_client.to_le_bytes());
        raw.extend_from_slice(&id_number.to_le_bytes());
        raw.extend_from_slice(payload_bytes);

        let crc = crc32fast::hash(&raw);
        raw.extend_from_slice(&crc.to_le_bytes());
        raw
    }

    fn decode_raw_entry(
        path: &Path,
        offset: u64,
        raw: &[u8],
    ) -> Result<RawVlogEntry, StorageError> {
        let total = raw.len();
        if total < MIN_ENTRY_SIZE {
            return Err(StorageError::Corruption {
                file: path.display().to_string(),
                offset,
                expected_crc: 0,
                actual_crc: 0,
            });
        }

        let entry_len = u32::from_le_bytes([raw[1], raw[2], raw[3], raw[4]]) as usize;
        if entry_len != total {
            return Err(StorageError::Codec(format!(
                "entry length mismatch: header={entry_len} actual={total}"
            )));
        }

        let stored_crc = u32::from_le_bytes([
            raw[total - 4],
            raw[total - 3],
            raw[total - 2],
            raw[total - 1],
        ]);
        let computed_crc = crc32fast::hash(&raw[..total - 4]);
        if stored_crc != computed_crc {
            return Err(StorageError::Corruption {
                file: path.display().to_string(),
                offset,
                expected_crc: stored_crc,
                actual_crc: computed_crc,
            });
        }

        let id_client = u64::from_le_bytes([
            raw[5], raw[6], raw[7], raw[8], raw[9], raw[10], raw[11], raw[12],
        ]);
        let id_number = u64::from_le_bytes([
            raw[13], raw[14], raw[15], raw[16], raw[17], raw[18], raw[19], raw[20],
        ]);
        let payload = raw[HEADER_SIZE..total - CRC_SIZE].to_vec();

        Ok(RawVlogEntry {
            entry_type: raw[0],
            id_client,
            id_number,
            payload,
        })
    }

    /// Append a protocol-agnostic batch to the segment in one write.
    pub fn append_raw_batch(
        &mut self,
        entries: &[(u8, u64, u64, &[u8])],
    ) -> Result<Vec<VlogPtr>, StorageError> {
        if entries.is_empty() {
            return Ok(Vec::new());
        }

        let mut all_bytes = Vec::new();
        let mut entry_offsets = Vec::with_capacity(entries.len());

        for (entry_type, id_client, id_number, payload) in entries {
            let raw = Self::encode_raw_entry(*entry_type, *id_client, *id_number, payload);
            let entry_offset = self.write_offset + all_bytes.len() as u64;
            entry_offsets.push((entry_offset, raw.len() as u32));
            all_bytes.extend_from_slice(&raw);
        }

        let mut buf = AlignedBuf::new(all_bytes.len());
        buf.as_full_slice_mut()[..all_bytes.len()].copy_from_slice(&all_bytes);
        buf.set_len(all_bytes.len());

        IO::block_on(self.io.as_ref().unwrap().pwrite(&buf, self.write_offset))?;
        self.write_offset += all_bytes.len() as u64;

        Ok(entry_offsets
            .into_iter()
            .map(|(offset, length)| VlogPtr {
                segment_id: self.id,
                offset,
                length,
            })
            .collect())
    }

    /// Read and verify a protocol-agnostic entry at the given pointer.
    pub fn read_raw_entry(&self, ptr: &VlogPtr) -> Result<RawVlogEntry, StorageError> {
        let total = ptr.length as usize;
        let read_size = round_up(total);
        let mut buf = AlignedBuf::new(read_size);
        IO::block_on(self.io.as_ref().unwrap().pread(&mut buf, ptr.offset))?;
        let raw = &buf.as_full_slice()[..total];
        Self::decode_raw_entry(&self.path, ptr.offset, raw)
    }

    /// Iterate all raw entries in the segment.
    pub fn iter_raw_entries(&self) -> Result<Vec<(u64, RawVlogEntry)>, StorageError> {
        let mut results = Vec::new();
        let mut offset = 0u64;
        let end = self.write_offset;

        while offset + HEADER_SIZE as u64 <= end {
            let header_read_size = round_up(HEADER_SIZE);
            let mut header_buf = AlignedBuf::new(header_read_size);
            IO::block_on(self.io.as_ref().unwrap().pread(&mut header_buf, offset))?;

            let raw = header_buf.as_full_slice();
            let entry_len = u32::from_le_bytes([raw[1], raw[2], raw[3], raw[4]]) as u64;
            if entry_len < MIN_ENTRY_SIZE as u64 || offset + entry_len > end {
                break;
            }

            let ptr = VlogPtr {
                segment_id: self.id,
                offset,
                length: entry_len as u32,
            };
            let entry = self.read_raw_entry(&ptr)?;
            results.push((offset, entry));
            offset += entry_len;
        }

        Ok(results)
    }

    /// Open or create a segment file.
    pub fn open(id: u64, path: PathBuf, flags: OpenFlags) -> Result<Self, StorageError> {
        let io = IO::open(&path, flags, crate::mvcc::disk::disk_io::OpenMode::CreateNew)?;
        Ok(Self {
            id,
            io: Some(io),
            write_offset: 0,
            path,
            views: Vec::new(),
        })
    }

    /// Open an existing segment at a known write offset (for recovery).
    pub fn open_at(
        id: u64,
        path: PathBuf,
        write_offset: u64,
        views: Vec<ViewRange>,
        flags: OpenFlags,
    ) -> Result<Self, StorageError> {
        let io = IO::open(&path, flags, crate::mvcc::disk::disk_io::OpenMode::Segment { write_offset })?;
        Ok(Self {
            id,
            io: Some(io),
            write_offset,
            path,
            views,
        })
    }

    pub fn write_offset(&self) -> u64 {
        self.write_offset
    }

    pub fn path(&self) -> &PathBuf {
        &self.path
    }

    /// Read all valid bytes from this segment (0..write_offset).
    pub(crate) fn read_all_bytes(&self) -> Result<Vec<u8>, StorageError> {
        let len = self.write_offset as usize;
        if len == 0 {
            return Ok(Vec::new());
        }
        let read_size = round_up(len);
        let mut buf = AlignedBuf::new(read_size);
        IO::block_on(self.io.as_ref().unwrap().pread(&mut buf, 0))?;
        Ok(buf.as_full_slice()[..len].to_vec())
    }

    /// Fsync the segment.
    pub fn sync(&self) -> Result<(), StorageError> {
        IO::block_on(self.io.as_ref().unwrap().fsync())
    }

    /// Close the segment.
    pub fn close(mut self) {
        if let Some(io) = self.io.take() {
            io.close();
        }
    }

    /// Start a new view within this segment.
    /// Records the current write offset as the start of the new view's entries.
    pub fn start_view(&mut self, view: u64) {
        self.views.push(ViewRange {
            view,
            start_offset: self.write_offset,
            end_offset: self.write_offset,
            num_entries: 0,
        });
    }

    /// Finish the current view within this segment.
    /// Updates the end offset and entry count.
    pub fn finish_view(&mut self, num_entries: u32) {
        if let Some(vr) = self.views.last_mut() {
            vr.end_offset = self.write_offset;
            vr.num_entries = num_entries;
        }
    }
}

