use super::ir_record::{IrPayloadInline, VlogEntryType};
use super::types::{UnifiedVlogPtr, ViewRange};
use crate::ir::OpId;
use crate::mvcc::disk::aligned_buf::{AlignedBuf, round_up};
use crate::mvcc::disk::disk_io::{DiskIo, OpenFlags};
use crate::mvcc::disk::error::StorageError;
use crate::occ::TransactionId as OccTransactionId;
use crate::tapir::Timestamp;
use crate::unified::tapir::CachedPrepare;
use crate::IrClientId;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::path::{Path, PathBuf};

/// Fixed header size: entry_type(1) + entry_len(4) + op_id.client_id(8) + op_id.number(8) = 21.
const HEADER_SIZE: usize = 21;

/// CRC trailer size.
const CRC_SIZE: usize = 4;

/// Minimum entry size: header + crc (no payload).
const MIN_ENTRY_SIZE: usize = HEADER_SIZE + CRC_SIZE;

/// VLog entry type byte for TAPIR committed transactions.
const TAPIR_COMMITTED_TXN_ENTRY_TYPE: u8 = 0x80;

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
pub struct UnifiedVlogSegment<IO: DiskIo> {
    pub id: u64,
    io: Option<IO>,
    write_offset: u64,
    path: PathBuf,
    pub views: Vec<ViewRange>,
}

impl<IO: DiskIo> UnifiedVlogSegment<IO> {
    fn encode_raw_entry(
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

    /// Append a protocol-agnostic entry to the segment.
    pub fn append_raw_entry(
        &mut self,
        entry_type: u8,
        id_client: u64,
        id_number: u64,
        payload_bytes: &[u8],
    ) -> Result<UnifiedVlogPtr, StorageError> {
        let raw = Self::encode_raw_entry(entry_type, id_client, id_number, payload_bytes);
        let total_len = raw.len();

        let mut buf = AlignedBuf::new(total_len);
        buf.as_full_slice_mut()[..total_len].copy_from_slice(&raw);
        buf.set_len(total_len);

        let offset = self.write_offset;
        IO::block_on(self.io.as_ref().unwrap().pwrite(&buf, offset))?;
        self.write_offset += total_len as u64;

        Ok(UnifiedVlogPtr {
            segment_id: self.id,
            offset,
            length: total_len as u32,
        })
    }

    /// Append a protocol-agnostic batch to the segment in one write.
    pub fn append_raw_batch(
        &mut self,
        entries: &[(u8, u64, u64, &[u8])],
    ) -> Result<Vec<UnifiedVlogPtr>, StorageError> {
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
            .map(|(offset, length)| UnifiedVlogPtr {
                segment_id: self.id,
                offset,
                length,
            })
            .collect())
    }

    /// Read and verify a protocol-agnostic entry at the given pointer.
    pub fn read_raw_entry(&self, ptr: &UnifiedVlogPtr) -> Result<RawVlogEntry, StorageError> {
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

            let ptr = UnifiedVlogPtr {
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

    fn serialize_committed_txn_payload<K: Serialize, V: Serialize>(
        transaction_id: OccTransactionId,
        commit_ts: Timestamp,
        read_set: &[(K, Timestamp)],
        write_set: &[(K, Option<V>)],
        scan_set: &[(K, K, Timestamp)],
    ) -> Result<Vec<u8>, StorageError> {
        crate::unified::tapir::vlog_codec::serialize_committed_txn_payload(
            transaction_id,
            commit_ts,
            read_set,
            write_set,
            scan_set,
        )
    }

    fn deserialize_committed_txn_payload<K: DeserializeOwned, V: DeserializeOwned>(
        bytes: &[u8],
    ) -> Result<CachedPrepare<K, V>, StorageError> {
        crate::unified::tapir::vlog_codec::deserialize_committed_txn_payload(bytes)
    }

    /// Append a TAPIR committed transaction entry to the segment.
    pub fn append_committed_txn<K: Serialize, V: Serialize>(
        &mut self,
        transaction_id: OccTransactionId,
        commit_ts: Timestamp,
        read_set: &[(K, Timestamp)],
        write_set: &[(K, Option<V>)],
        scan_set: &[(K, K, Timestamp)],
    ) -> Result<UnifiedVlogPtr, StorageError> {
        let payload_bytes = Self::serialize_committed_txn_payload(
            transaction_id,
            commit_ts,
            read_set,
            write_set,
            scan_set,
        )?;
        self.append_raw_entry(
            TAPIR_COMMITTED_TXN_ENTRY_TYPE,
            transaction_id.client_id.0,
            transaction_id.number,
            &payload_bytes,
        )
    }
    /// Open or create a segment file.
    pub fn open(id: u64, path: PathBuf, flags: OpenFlags) -> Result<Self, StorageError> {
        let io = IO::open(&path, flags)?;
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
        let io = IO::open(&path, flags)?;
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

    /// Serialize a typed IR payload to bytes for on-disk storage.
    ///
    /// Each key/value is individually bitcode-serialized into `Vec<u8>`
    /// within `PreparePayloadSer`.  A `None` value (tombstone) is
    /// represented as an empty byte vector, distinguishing it from a
    /// zero-length serialized value.
    fn serialize_payload<K: Serialize, V: Serialize>(
        payload: &IrPayloadInline<K, V>,
    ) -> Result<Vec<u8>, StorageError> {
        crate::unified::ir::vlog_codec::serialize_payload(payload)
    }

    /// Deserialize an on-disk payload back to typed `IrPayloadInline<K, V>`.
    ///
    /// Inverse of `serialize_payload`.  Empty byte vectors in the
    /// write_set are interpreted as `None` (tombstone).
    fn deserialize_payload<K: DeserializeOwned, V: DeserializeOwned>(
        entry_type: VlogEntryType,
        bytes: &[u8],
    ) -> Result<IrPayloadInline<K, V>, StorageError> {
        crate::unified::ir::vlog_codec::deserialize_payload(entry_type, bytes)
    }

    /// Append a single IR entry to the segment.
    ///
    /// On-disk entry layout:
    /// ```text
    /// [entry_type(1) | entry_len(4) | op_id.client_id(8) | op_id.number(8) | payload(N) | crc32(4)]
    /// ```
    pub fn append_entry<K: Serialize, V: Serialize>(
        &mut self,
        op_id: OpId,
        entry_type: VlogEntryType,
        payload: &IrPayloadInline<K, V>,
    ) -> Result<UnifiedVlogPtr, StorageError> {
        let payload_bytes = Self::serialize_payload(payload)?;
        self.append_raw_entry(
            entry_type as u8,
            op_id.client_id.0,
            op_id.number,
            &payload_bytes,
        )
    }

    /// Append a batch of IR entries to the segment.
    /// Returns a vector of VLog pointers, one per entry.
    pub fn append_batch<K: Serialize, V: Serialize>(
        &mut self,
        entries: &[(OpId, VlogEntryType, &IrPayloadInline<K, V>)],
    ) -> Result<Vec<UnifiedVlogPtr>, StorageError> {
        if entries.is_empty() {
            return Ok(Vec::new());
        }

        let mut serialized = Vec::with_capacity(entries.len());
        for (op_id, entry_type, payload) in entries {
            let payload_bytes = Self::serialize_payload(payload)?;
            serialized.push((*entry_type as u8, op_id.client_id.0, op_id.number, payload_bytes));
        }

        let raw_entries: Vec<(u8, u64, u64, &[u8])> = serialized
            .iter()
            .map(|(et, cid, num, bytes)| (*et, *cid, *num, bytes.as_slice()))
            .collect();
        self.append_raw_batch(&raw_entries)
    }

    /// Read and verify an entry at the given pointer.
    /// Returns the OpId, entry type, and deserialized payload.
    pub fn read_entry<K: DeserializeOwned, V: DeserializeOwned>(
        &self,
        ptr: &UnifiedVlogPtr,
    ) -> Result<(OpId, VlogEntryType, IrPayloadInline<K, V>), StorageError> {
        let raw = self.read_raw_entry(ptr)?;

        let entry_type = crate::unified::ir::vlog_codec::entry_type_from_byte(raw.entry_type)?;
        let op_id = OpId {
            client_id: IrClientId(raw.id_client),
            number: raw.id_number,
        };

        let payload = Self::deserialize_payload(entry_type, &raw.payload)?;

        Ok((op_id, entry_type, payload))
    }

    /// Read a CO::Prepare entry and return it as a CachedPrepare.
    pub fn read_prepare<K: DeserializeOwned, V: DeserializeOwned>(
        &self,
        ptr: &UnifiedVlogPtr,
    ) -> Result<CachedPrepare<K, V>, StorageError> {
        let (_, entry_type, payload) = self.read_entry::<K, V>(ptr)?;
        if entry_type != VlogEntryType::Prepare {
            return Err(StorageError::Codec(format!(
                "expected Prepare entry, got {entry_type:?}"
            )));
        }
        match payload {
            IrPayloadInline::Prepare {
                transaction_id,
                commit_ts,
                read_set,
                write_set,
                scan_set,
            } => Ok(CachedPrepare {
                transaction_id,
                commit_ts,
                read_set,
                write_set,
                scan_set,
            }),
            _ => unreachable!("entry_type is Prepare but payload is not"),
        }
    }

    /// Read a TAPIR committed transaction entry from VLog.
    pub fn read_committed_txn<K: DeserializeOwned, V: DeserializeOwned>(
        &self,
        ptr: &UnifiedVlogPtr,
    ) -> Result<CachedPrepare<K, V>, StorageError> {
        let raw = self.read_raw_entry(ptr)?;
        if raw.entry_type != TAPIR_COMMITTED_TXN_ENTRY_TYPE {
            return Err(StorageError::Codec(format!(
                "expected committed transaction entry, got type byte {:#04x}",
                raw.entry_type
            )));
        }

        Self::deserialize_committed_txn_payload(&raw.payload)
    }

    /// Iterate all entries in the segment from offset 0 up to write_offset.
    /// Returns (offset, OpId, entry_type, payload) for each entry.
    pub fn iter_entries<K: DeserializeOwned, V: DeserializeOwned>(
        &self,
    ) -> Result<Vec<(u64, OpId, VlogEntryType, IrPayloadInline<K, V>)>, StorageError> {
        let mut results = Vec::new();
        let mut offset = 0u64;
        let end = self.write_offset;

        while offset + HEADER_SIZE as u64 <= end {
            // Read just the header to get entry_len
            let header_read_size = round_up(HEADER_SIZE);
            let mut header_buf = AlignedBuf::new(header_read_size);
            IO::block_on(self.io.as_ref().unwrap().pread(&mut header_buf, offset))?;

            let raw = header_buf.as_full_slice();
            let entry_len = u32::from_le_bytes([raw[1], raw[2], raw[3], raw[4]]) as u64;
            if entry_len < MIN_ENTRY_SIZE as u64 || offset + entry_len > end {
                break;
            }

            let entry_type_byte = raw[0];
            if entry_type_byte == TAPIR_COMMITTED_TXN_ENTRY_TYPE {
                offset += entry_len;
                continue;
            }

            let ptr = UnifiedVlogPtr {
                segment_id: self.id,
                offset,
                length: entry_len as u32,
            };
            let (op_id, entry_type, payload) = self.read_entry(&ptr)?;
            results.push((offset, op_id, entry_type, payload));

            offset += entry_len;
        }

        Ok(results)
    }

    /// Iterate TAPIR committed transaction entries in the segment.
    /// Returns `(offset, prepared)` tuples.
    pub fn iter_committed_txn_entries<K: DeserializeOwned, V: DeserializeOwned>(
        &self,
    ) -> Result<Vec<(u64, CachedPrepare<K, V>)>, StorageError> {
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

            if raw[0] == TAPIR_COMMITTED_TXN_ENTRY_TYPE {
                let ptr = UnifiedVlogPtr {
                    segment_id: self.id,
                    offset,
                    length: entry_len as u32,
                };
                let prepared = self.read_committed_txn(&ptr)?;
                results.push((offset, prepared));
            }

            offset += entry_len;
        }

        Ok(results)
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

