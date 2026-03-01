use super::types::{
    CachedPrepare, IrPayloadInline, UnifiedVlogPtr, ViewRange, VlogEntryType,
};
use crate::ir::OpId;
use crate::mvcc::disk::aligned_buf::{AlignedBuf, round_up};
use crate::mvcc::disk::disk_io::{DiskIo, OpenFlags};
use crate::mvcc::disk::error::StorageError;
use crate::occ::TransactionId as OccTransactionId;
use crate::tapir::Timestamp;
use crate::IrClientId;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::path::PathBuf;

/// Fixed header size: entry_type(1) + entry_len(4) + op_id.client_id(8) + op_id.number(8) = 21.
const HEADER_SIZE: usize = 21;

/// CRC trailer size.
const CRC_SIZE: usize = 4;

/// Minimum entry size: header + crc (no payload).
const MIN_ENTRY_SIZE: usize = HEADER_SIZE + CRC_SIZE;

/// A physical VLog segment file (either active or sealed).
pub struct UnifiedVlogSegment<IO: DiskIo> {
    pub id: u64,
    io: Option<IO>,
    write_offset: u64,
    path: PathBuf,
    pub views: Vec<ViewRange>,
}

impl<IO: DiskIo> UnifiedVlogSegment<IO> {
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

    /// Serialize an IR entry payload to bytes (bitcode).
    /// Converts typed K, V fields to byte vectors for on-disk storage.
    fn serialize_payload<K: Serialize, V: Serialize>(
        payload: &IrPayloadInline<K, V>,
    ) -> Result<Vec<u8>, StorageError> {
        let bytes = match payload {
            IrPayloadInline::Prepare {
                transaction_id,
                commit_ts,
                read_set,
                write_set,
                scan_set,
            } => {
                let ser_read_set: Vec<(Vec<u8>, Timestamp)> = read_set
                    .iter()
                    .map(|(k, ts)| {
                        let kb = bitcode::serialize(k)
                            .map_err(|e| StorageError::Codec(e.to_string()))?;
                        Ok((kb, *ts))
                    })
                    .collect::<Result<_, StorageError>>()?;
                let ser_write_set: Vec<(Vec<u8>, Vec<u8>)> = write_set
                    .iter()
                    .map(|(k, v)| {
                        let kb = bitcode::serialize(k)
                            .map_err(|e| StorageError::Codec(e.to_string()))?;
                        let vb = v
                            .as_ref()
                            .map(|val| bitcode::serialize(val))
                            .transpose()
                            .map_err(|e| StorageError::Codec(e.to_string()))?
                            .unwrap_or_default();
                        Ok((kb, vb))
                    })
                    .collect::<Result<_, StorageError>>()?;
                let ser_scan_set: Vec<(Vec<u8>, Vec<u8>, Timestamp)> = scan_set
                    .iter()
                    .map(|(sk, ek, ts)| {
                        let skb = bitcode::serialize(sk)
                            .map_err(|e| StorageError::Codec(e.to_string()))?;
                        let ekb = bitcode::serialize(ek)
                            .map_err(|e| StorageError::Codec(e.to_string()))?;
                        Ok((skb, ekb, *ts))
                    })
                    .collect::<Result<_, StorageError>>()?;
                let encodable = PreparePayloadSer {
                    txn_client_id: transaction_id.client_id.0,
                    txn_number: transaction_id.number,
                    commit_time: commit_ts.time,
                    commit_client_id: commit_ts.client_id.0,
                    read_set: ser_read_set,
                    write_set: ser_write_set,
                    scan_set: ser_scan_set,
                };
                bitcode::serialize(&encodable)
                    .map_err(|e| StorageError::Codec(e.to_string()))?
            }
            IrPayloadInline::Commit {
                transaction_id,
                commit_ts,
                prepare_ref,
            } => {
                let encodable = CommitPayloadSer {
                    txn_client_id: transaction_id.client_id.0,
                    txn_number: transaction_id.number,
                    commit_time: commit_ts.time,
                    commit_client_id: commit_ts.client_id.0,
                    prepare_ref: prepare_ref.clone(),
                };
                bitcode::serialize(&encodable)
                    .map_err(|e| StorageError::Codec(e.to_string()))?
            }
            IrPayloadInline::Abort {
                transaction_id,
                commit_ts,
            } => {
                let encodable = AbortPayloadSer {
                    txn_client_id: transaction_id.client_id.0,
                    txn_number: transaction_id.number,
                    commit_ts: commit_ts.map(|ts| (ts.time, ts.client_id.0)),
                };
                bitcode::serialize(&encodable)
                    .map_err(|e| StorageError::Codec(e.to_string()))?
            }
            IrPayloadInline::QuorumRead { key, timestamp } => {
                let key_bytes = bitcode::serialize(key)
                    .map_err(|e| StorageError::Codec(e.to_string()))?;
                let encodable = QuorumReadPayloadSer {
                    key: key_bytes,
                    time: timestamp.time,
                    client_id: timestamp.client_id.0,
                };
                bitcode::serialize(&encodable)
                    .map_err(|e| StorageError::Codec(e.to_string()))?
            }
            IrPayloadInline::QuorumScan {
                start_key,
                end_key,
                snapshot_ts,
            } => {
                let start_bytes = bitcode::serialize(start_key)
                    .map_err(|e| StorageError::Codec(e.to_string()))?;
                let end_bytes = bitcode::serialize(end_key)
                    .map_err(|e| StorageError::Codec(e.to_string()))?;
                let encodable = QuorumScanPayloadSer {
                    start_key: start_bytes,
                    end_key: end_bytes,
                    time: snapshot_ts.time,
                    client_id: snapshot_ts.client_id.0,
                };
                bitcode::serialize(&encodable)
                    .map_err(|e| StorageError::Codec(e.to_string()))?
            }
            IrPayloadInline::RaiseMinPrepareTime { time } => {
                bitcode::serialize(time)
                    .map_err(|e| StorageError::Codec(e.to_string()))?
            }
        };
        Ok(bytes)
    }

    /// Deserialize an IR entry payload from bytes.
    /// Converts byte vectors back to typed K, V fields.
    fn deserialize_payload<K: DeserializeOwned, V: DeserializeOwned>(
        entry_type: VlogEntryType,
        bytes: &[u8],
    ) -> Result<IrPayloadInline<K, V>, StorageError> {
        match entry_type {
            VlogEntryType::Prepare => {
                let p: PreparePayloadSer = bitcode::deserialize(bytes)
                    .map_err(|e| StorageError::Codec(e.to_string()))?;
                let read_set: Vec<(K, Timestamp)> = p
                    .read_set
                    .iter()
                    .map(|(kb, ts)| {
                        let k: K = bitcode::deserialize(kb)
                            .map_err(|e| StorageError::Codec(e.to_string()))?;
                        Ok((k, *ts))
                    })
                    .collect::<Result<_, StorageError>>()?;
                let write_set: Vec<(K, Option<V>)> = p
                    .write_set
                    .iter()
                    .map(|(kb, vb)| {
                        let k: K = bitcode::deserialize(kb)
                            .map_err(|e| StorageError::Codec(e.to_string()))?;
                        let v = if vb.is_empty() {
                            None
                        } else {
                            Some(
                                bitcode::deserialize::<V>(vb)
                                    .map_err(|e| StorageError::Codec(e.to_string()))?,
                            )
                        };
                        Ok((k, v))
                    })
                    .collect::<Result<_, StorageError>>()?;
                let scan_set: Vec<(K, K, Timestamp)> = p
                    .scan_set
                    .iter()
                    .map(|(skb, ekb, ts)| {
                        let sk: K = bitcode::deserialize(skb)
                            .map_err(|e| StorageError::Codec(e.to_string()))?;
                        let ek: K = bitcode::deserialize(ekb)
                            .map_err(|e| StorageError::Codec(e.to_string()))?;
                        Ok((sk, ek, *ts))
                    })
                    .collect::<Result<_, StorageError>>()?;
                Ok(IrPayloadInline::Prepare {
                    transaction_id: OccTransactionId {
                        client_id: IrClientId(p.txn_client_id),
                        number: p.txn_number,
                    },
                    commit_ts: Timestamp {
                        time: p.commit_time,
                        client_id: IrClientId(p.commit_client_id),
                    },
                    read_set,
                    write_set,
                    scan_set,
                })
            }
            VlogEntryType::Commit => {
                let c: CommitPayloadSer = bitcode::deserialize(bytes)
                    .map_err(|e| StorageError::Codec(e.to_string()))?;
                Ok(IrPayloadInline::Commit {
                    transaction_id: OccTransactionId {
                        client_id: IrClientId(c.txn_client_id),
                        number: c.txn_number,
                    },
                    commit_ts: Timestamp {
                        time: c.commit_time,
                        client_id: IrClientId(c.commit_client_id),
                    },
                    prepare_ref: c.prepare_ref,
                })
            }
            VlogEntryType::Abort => {
                let a: AbortPayloadSer = bitcode::deserialize(bytes)
                    .map_err(|e| StorageError::Codec(e.to_string()))?;
                Ok(IrPayloadInline::Abort {
                    transaction_id: OccTransactionId {
                        client_id: IrClientId(a.txn_client_id),
                        number: a.txn_number,
                    },
                    commit_ts: a.commit_ts.map(|(time, cid)| Timestamp {
                        time,
                        client_id: IrClientId(cid),
                    }),
                })
            }
            VlogEntryType::QuorumRead => {
                let q: QuorumReadPayloadSer = bitcode::deserialize(bytes)
                    .map_err(|e| StorageError::Codec(e.to_string()))?;
                let key: K = bitcode::deserialize(&q.key)
                    .map_err(|e| StorageError::Codec(e.to_string()))?;
                Ok(IrPayloadInline::QuorumRead {
                    key,
                    timestamp: Timestamp {
                        time: q.time,
                        client_id: IrClientId(q.client_id),
                    },
                })
            }
            VlogEntryType::QuorumScan => {
                let q: QuorumScanPayloadSer = bitcode::deserialize(bytes)
                    .map_err(|e| StorageError::Codec(e.to_string()))?;
                let start_key: K = bitcode::deserialize(&q.start_key)
                    .map_err(|e| StorageError::Codec(e.to_string()))?;
                let end_key: K = bitcode::deserialize(&q.end_key)
                    .map_err(|e| StorageError::Codec(e.to_string()))?;
                Ok(IrPayloadInline::QuorumScan {
                    start_key,
                    end_key,
                    snapshot_ts: Timestamp {
                        time: q.time,
                        client_id: IrClientId(q.client_id),
                    },
                })
            }
            VlogEntryType::RaiseMinPrepareTime => {
                let time: u64 = bitcode::deserialize(bytes)
                    .map_err(|e| StorageError::Codec(e.to_string()))?;
                Ok(IrPayloadInline::RaiseMinPrepareTime { time })
            }
        }
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
        let total_len = HEADER_SIZE + payload_bytes.len() + CRC_SIZE;

        let mut raw = Vec::with_capacity(total_len);

        // entry_type (1 byte)
        raw.push(entry_type as u8);

        // entry_len (4 bytes, total including header)
        raw.extend_from_slice(&(total_len as u32).to_le_bytes());

        // op_id.client_id (8 bytes)
        raw.extend_from_slice(&op_id.client_id.0.to_le_bytes());

        // op_id.number (8 bytes)
        raw.extend_from_slice(&op_id.number.to_le_bytes());

        // payload
        raw.extend_from_slice(&payload_bytes);

        // crc32 over everything before the CRC
        let crc = crc32fast::hash(&raw);
        raw.extend_from_slice(&crc.to_le_bytes());

        debug_assert_eq!(raw.len(), total_len);

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

    /// Append a batch of IR entries to the segment.
    /// Returns a vector of VLog pointers, one per entry.
    pub fn append_batch<K: Serialize, V: Serialize>(
        &mut self,
        entries: &[(OpId, VlogEntryType, &IrPayloadInline<K, V>)],
    ) -> Result<Vec<UnifiedVlogPtr>, StorageError> {
        if entries.is_empty() {
            return Ok(Vec::new());
        }

        // Serialize all entries into a single buffer for one write.
        let mut all_bytes = Vec::new();
        let mut entry_offsets = Vec::with_capacity(entries.len());

        for (op_id, entry_type, payload) in entries {
            let payload_bytes = Self::serialize_payload(payload)?;
            let total_len = HEADER_SIZE + payload_bytes.len() + CRC_SIZE;

            let entry_offset = self.write_offset + all_bytes.len() as u64;
            entry_offsets.push((entry_offset, total_len as u32));

            // entry_type
            all_bytes.push(*entry_type as u8);
            // entry_len
            all_bytes.extend_from_slice(&(total_len as u32).to_le_bytes());
            // op_id
            all_bytes.extend_from_slice(&op_id.client_id.0.to_le_bytes());
            all_bytes.extend_from_slice(&op_id.number.to_le_bytes());
            // payload
            all_bytes.extend_from_slice(&payload_bytes);
            // crc32 over the entry (everything before CRC)
            let crc_start = all_bytes.len() - (HEADER_SIZE + payload_bytes.len());
            let crc = crc32fast::hash(&all_bytes[crc_start..]);
            all_bytes.extend_from_slice(&crc.to_le_bytes());
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

    /// Read and verify an entry at the given pointer.
    /// Returns the OpId, entry type, and deserialized payload.
    pub fn read_entry<K: DeserializeOwned, V: DeserializeOwned>(
        &self,
        ptr: &UnifiedVlogPtr,
    ) -> Result<(OpId, VlogEntryType, IrPayloadInline<K, V>), StorageError> {
        let total = ptr.length as usize;
        if total < MIN_ENTRY_SIZE {
            return Err(StorageError::Corruption {
                file: self.path.display().to_string(),
                offset: ptr.offset,
                expected_crc: 0,
                actual_crc: 0,
            });
        }

        let read_size = round_up(total);
        let mut buf = AlignedBuf::new(read_size);
        IO::block_on(self.io.as_ref().unwrap().pread(&mut buf, ptr.offset))?;

        let raw = &buf.as_full_slice()[..total];

        // Verify CRC
        let stored_crc = u32::from_le_bytes([
            raw[total - 4],
            raw[total - 3],
            raw[total - 2],
            raw[total - 1],
        ]);
        let computed_crc = crc32fast::hash(&raw[..total - 4]);
        if stored_crc != computed_crc {
            return Err(StorageError::Corruption {
                file: self.path.display().to_string(),
                offset: ptr.offset,
                expected_crc: stored_crc,
                actual_crc: computed_crc,
            });
        }

        // Parse header
        let entry_type_byte = raw[0];
        let entry_type = match entry_type_byte {
            0x01 => VlogEntryType::Prepare,
            0x02 => VlogEntryType::Commit,
            0x03 => VlogEntryType::Abort,
            0x04 => VlogEntryType::QuorumRead,
            0x05 => VlogEntryType::QuorumScan,
            0x06 => VlogEntryType::RaiseMinPrepareTime,
            _ => {
                return Err(StorageError::Codec(format!(
                    "unknown VLog entry type: {entry_type_byte:#04x}"
                )));
            }
        };

        let _entry_len = u32::from_le_bytes([raw[1], raw[2], raw[3], raw[4]]);

        let client_id = u64::from_le_bytes([
            raw[5], raw[6], raw[7], raw[8], raw[9], raw[10], raw[11], raw[12],
        ]);
        let number = u64::from_le_bytes([
            raw[13], raw[14], raw[15], raw[16], raw[17], raw[18], raw[19], raw[20],
        ]);
        let op_id = OpId {
            client_id: IrClientId(client_id),
            number,
        };

        // Payload is between header and CRC
        let payload_bytes = &raw[HEADER_SIZE..total - CRC_SIZE];
        let payload = Self::deserialize_payload(entry_type, payload_bytes)?;

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

// -- Serialization helper structs (bitcode-compatible, no skip_serializing_if) --

use super::types::PrepareRef;
use serde::Deserialize;

#[derive(Serialize, Deserialize)]
struct PreparePayloadSer {
    txn_client_id: u64,
    txn_number: u64,
    commit_time: u64,
    commit_client_id: u64,
    read_set: Vec<(Vec<u8>, Timestamp)>,
    write_set: Vec<(Vec<u8>, Vec<u8>)>,
    scan_set: Vec<(Vec<u8>, Vec<u8>, Timestamp)>,
}

#[derive(Serialize, Deserialize)]
struct CommitPayloadSer {
    txn_client_id: u64,
    txn_number: u64,
    commit_time: u64,
    commit_client_id: u64,
    prepare_ref: PrepareRef,
}

#[derive(Serialize, Deserialize)]
struct AbortPayloadSer {
    txn_client_id: u64,
    txn_number: u64,
    commit_ts: Option<(u64, u64)>,
}

#[derive(Serialize, Deserialize)]
struct QuorumReadPayloadSer {
    key: Vec<u8>,
    time: u64,
    client_id: u64,
}

#[derive(Serialize, Deserialize)]
struct QuorumScanPayloadSer {
    start_key: Vec<u8>,
    end_key: Vec<u8>,
    time: u64,
    client_id: u64,
}
