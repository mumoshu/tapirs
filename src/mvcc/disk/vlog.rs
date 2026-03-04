use super::aligned_buf::{AlignedBuf, round_up};
use super::disk_io::DiskIo;
use super::error::StorageError;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Fixed overhead per vlog entry: key_len(4) + value_len(4) + ts_len(4) + crc_kts(4) + crc_val(4).
const ENTRY_OVERHEAD: usize = 20;

/// Header size: key_len(4) + value_len(4) + ts_len(4).
const HEADER_SIZE: usize = 12;

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
pub(crate) struct VlogSegment<IO: DiskIo> {
    pub(crate) id: u64,
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
    /// [key_len(4) | value_len(4) | ts_len(4) | key_bytes | ts_bytes(ts_len) | crc_kts(4) | value_bytes | crc_val(4)]
    ///
    /// The paper specifies `(key size, value size, key, value)` — per-field sizes enabling
    /// field-addressable reads. We add a variable-size timestamp (serialized via bitcode)
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
    ///   wasting ~98% of each block — write amplification that defeats WiscKey's core
    ///   design goal of reducing I/O amplification.
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
    /// - `read_key_ts()`: Reads only `12 + key_len + ts_len + 4` bytes (header + key +
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

        let key_len = key_bytes.len() as u32;
        let value_len = value_bytes.len() as u32;
        let ts_len = ts_bytes.len() as u32;

        // CRC over key+ts region (for GC key-only verification).
        let mut kts_hasher = crc32fast::Hasher::new();
        kts_hasher.update(&key_bytes);
        kts_hasher.update(&ts_bytes);
        let crc_kts = kts_hasher.finalize();

        // CRC over value region.
        let crc_val = crc32fast::hash(&value_bytes);

        let total_len = ENTRY_OVERHEAD + key_bytes.len() + ts_bytes.len() + value_bytes.len();

        let mut buf = AlignedBuf::new(total_len);
        let slice = buf.as_full_slice_mut();
        let mut pos = 0;

        // Header: key_len(4) | value_len(4) | ts_len(4)
        slice[pos..pos + 4].copy_from_slice(&key_len.to_le_bytes());
        pos += 4;
        slice[pos..pos + 4].copy_from_slice(&value_len.to_le_bytes());
        pos += 4;
        slice[pos..pos + 4].copy_from_slice(&ts_len.to_le_bytes());
        pos += 4;

        // Key + timestamp + crc_kts
        slice[pos..pos + key_bytes.len()].copy_from_slice(&key_bytes);
        pos += key_bytes.len();
        slice[pos..pos + ts_bytes.len()].copy_from_slice(&ts_bytes);
        pos += ts_bytes.len();
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
        let ts_len = u32::from_le_bytes([raw[8], raw[9], raw[10], raw[11]]) as usize;

        let expected_total = ENTRY_OVERHEAD + key_len + ts_len + value_len;
        if expected_total != total {
            return Err(StorageError::Corruption {
                file: self.path.display().to_string(),
                offset: ptr.offset,
                expected_crc: 0,
                actual_crc: 0,
            });
        }

        // Extract regions
        let key_start = HEADER_SIZE;
        let key_end = key_start + key_len;
        let ts_start = key_end;
        let ts_end = ts_start + ts_len;
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
        // Read the 12-byte header to get key_len and ts_len
        let header_read_size = round_up(HEADER_SIZE);
        let mut header_buf = AlignedBuf::new(header_read_size);
        self.io.pread(&mut header_buf, ptr.offset).await?;
        let header = header_buf.as_full_slice();
        let key_len = u32::from_le_bytes([header[0], header[1], header[2], header[3]]) as usize;
        let ts_len = u32::from_le_bytes([header[8], header[9], header[10], header[11]]) as usize;

        // Read header + key + ts + crc_kts
        let kts_size = HEADER_SIZE + key_len + ts_len + 4;
        let read_size = round_up(kts_size);
        let mut buf = AlignedBuf::new(read_size);
        self.io.pread(&mut buf, ptr.offset).await?;
        let raw = buf.as_full_slice();

        let key_start = HEADER_SIZE;
        let key_end = key_start + key_len;
        let ts_start = key_end;
        let ts_end = ts_start + ts_len;
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

    /// Read and verify a vlog entry before returning it to the caller.
    ///
    /// **What the original paper does** (WiscKey, Lu et al., FAST'16, Section
    /// 3.3.3, paragraph starting "if the key could be found in the LSM tree,
    /// however, an additional step is required"):
    ///
    /// The paper specifies two verification checks on vlog reads:
    ///   1. Range check: verify the value address falls within the valid vLog range.
    ///   2. Key match: verify the vlog entry's key matches the queried key.
    ///      If either fails, the paper returns "not found." The paper also mentions
    ///      checksums as optional: "if necessary, a magic number or checksum can be
    ///      easily added to the header" — the paper does NOT require CRC verification
    ///      on reads, relying instead on the range + key-match checks for crash
    ///      consistency.
    ///
    /// **Our extensions beyond the paper**:
    ///
    /// 1. CRC verification (mandatory): We verify both crc_kts and crc_val on
    ///    every read. The paper treats checksums as optional, but without them,
    ///    silent value corruption (bit-rot) would go undetected — the key-match
    ///    check only catches key corruption, not value corruption.
    ///
    /// 2. Errors instead of "not found": The paper returns "not found" for all
    ///    verification failures, which is safe for a single-machine store with
    ///    no recovery option. In TAPIR's quorum system (f+1 replicas), silently
    ///    returning "not found" is dangerous: if a correlated event (e.g.,
    ///    datacenter power outage) causes data loss on a quorum of replicas for
    ///    the same key, all replicas return "not found" and the quorum read
    ///    produces silent data loss for a committed key. We return Err instead
    ///    so the consumer can propagate the error for retry on another replica
    ///    or trigger repair.
    ///
    /// 3. Specific error variants: We use four distinct error variants instead
    ///    of the paper's blanket "not found": VLogDataMissing (pointer outside
    ///    valid range), VLogKeyCrcMismatch (key/ts CRC failed), VLogValueCrcMismatch
    ///    (value CRC failed), VLogKeyMismatch (key doesn't match despite CRCs
    ///    passing). The paper does not distinguish failure types. All trigger the
    ///    same consumer action (propagate error for retry on another replica),
    ///    but the distinction aids diagnostics — see each variant's doc comment
    ///    in error.rs for direct cause, possible root causes, and consumer
    ///    guidance.
    ///
    /// **Verification checks**:
    ///
    /// 1. Range: pointer must fall within the valid vLog range [0, write_offset).
    ///    Failure → Err(VLogDataMissing): the vlog data is missing. This happens
    ///    when an SST contains a ValuePointer but the vlog was truncated in a
    ///    crash (e.g., crash between SST flush and manifest save, or fsync that
    ///    didn't reach non-volatile storage). The data was never durably written
    ///    to this replica.
    ///
    /// 2. CRC: both crc_kts and crc_val must match (our extension — paper does
    ///    not require CRC on reads). Failure → Err(VLogKeyCrcMismatch) or
    ///    Err(VLogValueCrcMismatch): the vlog data is present but damaged
    ///    (bit-rot). On the normal read path, the entry was previously verified
    ///    during flush or recovery, so a new CRC failure indicates silent disk
    ///    corruption after the fact.
    ///
    /// 3. Key match (from the paper): the vlog entry's key must match the
    ///    expected key from the LSM. Failure → Err(VLogKeyMismatch). Since the
    ///    vlog CRCs passed, this mismatch has multiple possible root causes:
    ///    (a) SSTable key corruption — bit-rot corrupted the key stored in the
    ///    SST, causing the LSM to return a wrong entry for the query;
    ///    (b) SSTable pointer corruption — the key in the SST is correct but
    ///    the ValuePointer's offset/segment_id was corrupted, pointing to a
    ///    different (valid) vlog entry; (c) vlog CRC false collision (~1/2³²,
    ///    negligible) — the vlog data is actually corrupt but the corruption
    ///    happened to pass CRC verification.
    ///
    /// Without this method, we originally called vlog.read() directly from
    /// resolve_value(), which had no range check or key-match verification
    /// (neither the paper's checks nor our extensions).
    pub async fn read_verified<K, V, TS>(
        &self,
        ptr: &ValuePointer,
        expected_key: &K,
    ) -> Result<VlogEntry<K, V, TS>, StorageError>
    where
        K: PartialEq + for<'de> Deserialize<'de>,
        V: for<'de> Deserialize<'de>,
        TS: for<'de> Deserialize<'de>,
    {
        // 1. Range check: pointer must fall within valid vLog range.
        if ptr.offset + ptr.length as u64 > self.write_offset {
            return Err(StorageError::VLogDataMissing {
                file: self.path.display().to_string(),
                offset: ptr.offset,
            });
        }

        // 2. CRC verification via read() — propagates VLogKeyCrcMismatch
        //    or VLogValueCrcMismatch on failure.
        let entry = self.read::<K, V, TS>(ptr).await?;

        // 3. Key match: vlog entry key must match expected key from LSM.
        if entry.key != *expected_key {
            return Err(StorageError::VLogKeyMismatch {
                file: self.path.display().to_string(),
                offset: ptr.offset,
            });
        }

        Ok(entry)
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
        let file_size = self.io.file_len()?;
        let mut current_offset = from_offset;

        while current_offset + HEADER_SIZE as u64 <= file_size {
            // Read header (12 bytes) to get key_len, value_len, ts_len.
            let header_read_size = round_up(HEADER_SIZE);
            let mut header_buf = AlignedBuf::new(header_read_size);
            self.io.pread(&mut header_buf, current_offset).await?;
            let header = header_buf.as_full_slice();

            let key_len =
                u32::from_le_bytes([header[0], header[1], header[2], header[3]]) as usize;
            let value_len =
                u32::from_le_bytes([header[4], header[5], header[6], header[7]]) as usize;
            let ts_len =
                u32::from_le_bytes([header[8], header[9], header[10], header[11]]) as usize;

            // Zero-filled padding: every real entry has at least 1 byte of key.
            if key_len == 0 {
                break;
            }

            let total_len = ENTRY_OVERHEAD + key_len + ts_len + value_len;

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

    /// Scan vlog entries and return only their ValuePointers, without deserializing
    /// any fields.
    ///
    /// Same scanning logic as recover_entries() — reads the 8-byte header to compute
    /// entry boundaries, verifies both CRCs (crc_kts and crc_val) over raw bytes —
    /// but skips all bitcode deserialization. Used by GC where the scan phase only
    /// needs entry boundaries (ValuePointers), and the liveness check uses
    /// read_key_ts() for key+ts access.
    pub async fn recover_pointers(
        &self,
        from_offset: u64,
    ) -> Result<Vec<ValuePointer>, StorageError> {
        let mut recovered = Vec::new();
        let file_size = self.io.file_len()?;
        let mut current_offset = from_offset;

        while current_offset + HEADER_SIZE as u64 <= file_size {
            // Read header (12 bytes) to get key_len, value_len, ts_len.
            let header_read_size = round_up(HEADER_SIZE);
            let mut header_buf = AlignedBuf::new(header_read_size);
            self.io.pread(&mut header_buf, current_offset).await?;
            let header = header_buf.as_full_slice();

            let key_len =
                u32::from_le_bytes([header[0], header[1], header[2], header[3]]) as usize;
            let value_len =
                u32::from_le_bytes([header[4], header[5], header[6], header[7]]) as usize;
            let ts_len =
                u32::from_le_bytes([header[8], header[9], header[10], header[11]]) as usize;

            // Zero-filled padding at the end of the file (from 4 KiB-aligned
            // pwrite) can look like a valid header with key_len=0, ts_len=0.
            // Every real entry has at least 1 byte of key data — stop here.
            if key_len == 0 {
                break;
            }

            let total_len = ENTRY_OVERHEAD + key_len + ts_len + value_len;

            // Partial write: not enough data for the full entry.
            if current_offset + total_len as u64 > file_size {
                break;
            }

            // Read the full entry bytes for CRC verification (no deserialization).
            let read_size = round_up(total_len);
            let mut buf = AlignedBuf::new(read_size);
            self.io.pread(&mut buf, current_offset).await?;
            let raw = buf.as_full_slice();

            // Verify crc_kts over key_bytes | ts_bytes.
            let key_start = HEADER_SIZE;
            let key_end = key_start + key_len;
            let ts_end = key_end + ts_len;
            let crc_kts_start = ts_end;

            let key_bytes = &raw[key_start..key_end];
            let ts_bytes = &raw[key_end..ts_end];

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
                break; // CRC failure — stop recovery (prefix property).
            }

            // Verify crc_val over value_bytes.
            let value_start = crc_kts_start + 4;
            let value_end = value_start + value_len;
            let crc_val_start = value_end;

            let value_bytes = &raw[value_start..value_end];
            let stored_crc_val = u32::from_le_bytes([
                raw[crc_val_start],
                raw[crc_val_start + 1],
                raw[crc_val_start + 2],
                raw[crc_val_start + 3],
            ]);
            let actual_crc_val = crc32fast::hash(value_bytes);

            if stored_crc_val != actual_crc_val {
                break; // CRC failure — stop recovery (prefix property).
            }

            let ptr = ValuePointer {
                segment_id: self.id,
                offset: current_offset,
                length: total_len as u32,
            };
            recovered.push(ptr);
            current_offset += total_len as u64;
        }

        Ok(recovered)
    }

    /// Append multiple entries in a single write, returning a `ValuePointer` per entry.
    ///
    /// Serializes all entries into one contiguous buffer and issues a single
    /// `pwrite()`. Each entry uses the same on-disk layout as `append()`. The
    /// returned pointers address individual entries within the batch — existing
    /// consumers (`read()`, `read_key_ts()`, `read_verified()`, `recover_entries()`)
    /// work unchanged because each entry is self-describing via its header.
    pub async fn append_batch<K, V, TS>(
        &mut self,
        entries: &[VlogEntry<K, V, TS>],
    ) -> Result<Vec<ValuePointer>, StorageError>
    where
        K: Serialize,
        V: Serialize,
        TS: Serialize,
    {
        if entries.is_empty() {
            return Ok(Vec::new());
        }

        // Pre-serialize all entries to compute total buffer size.
        struct SerializedEntry {
            key_bytes: Vec<u8>,
            ts_bytes: Vec<u8>,
            value_bytes: Vec<u8>,
            crc_kts: u32,
            crc_val: u32,
            total_len: usize,
        }

        let mut serialized = Vec::with_capacity(entries.len());
        let mut total_buf_size = 0usize;

        for entry in entries {
            let key_bytes =
                bitcode::serialize(&entry.key).map_err(|e| StorageError::Codec(e.to_string()))?;
            let ts_bytes = bitcode::serialize(&entry.timestamp)
                .map_err(|e| StorageError::Codec(e.to_string()))?;
            let value_bytes = bitcode::serialize(&entry.value)
                .map_err(|e| StorageError::Codec(e.to_string()))?;

            let mut kts_hasher = crc32fast::Hasher::new();
            kts_hasher.update(&key_bytes);
            kts_hasher.update(&ts_bytes);
            let crc_kts = kts_hasher.finalize();
            let crc_val = crc32fast::hash(&value_bytes);

            let total_len = ENTRY_OVERHEAD + key_bytes.len() + ts_bytes.len() + value_bytes.len();
            total_buf_size += total_len;

            serialized.push(SerializedEntry {
                key_bytes,
                ts_bytes,
                value_bytes,
                crc_kts,
                crc_val,
                total_len,
            });
        }

        // Build one contiguous buffer with all entries.
        let mut buf = AlignedBuf::new(total_buf_size);
        let slice = buf.as_full_slice_mut();
        let mut pos = 0;
        let mut ptrs = Vec::with_capacity(serialized.len());
        let base_offset = self.write_offset;

        for se in &serialized {
            let entry_offset = base_offset + pos as u64;
            let key_len = se.key_bytes.len() as u32;
            let value_len = se.value_bytes.len() as u32;

            let ts_len = se.ts_bytes.len() as u32;

            // Header: key_len(4) | value_len(4) | ts_len(4)
            slice[pos..pos + 4].copy_from_slice(&key_len.to_le_bytes());
            pos += 4;
            slice[pos..pos + 4].copy_from_slice(&value_len.to_le_bytes());
            pos += 4;
            slice[pos..pos + 4].copy_from_slice(&ts_len.to_le_bytes());
            pos += 4;

            // Key + timestamp + crc_kts
            slice[pos..pos + se.key_bytes.len()].copy_from_slice(&se.key_bytes);
            pos += se.key_bytes.len();
            slice[pos..pos + se.ts_bytes.len()].copy_from_slice(&se.ts_bytes);
            pos += se.ts_bytes.len();
            slice[pos..pos + 4].copy_from_slice(&se.crc_kts.to_le_bytes());
            pos += 4;

            // Value + crc_val
            slice[pos..pos + se.value_bytes.len()].copy_from_slice(&se.value_bytes);
            pos += se.value_bytes.len();
            slice[pos..pos + 4].copy_from_slice(&se.crc_val.to_le_bytes());
            pos += 4;

            ptrs.push(ValuePointer {
                segment_id: self.id,
                offset: entry_offset,
                length: se.total_len as u32,
            });
        }

        buf.set_len(total_buf_size);
        self.io.pwrite(&buf, base_offset).await?;
        self.write_offset += total_buf_size as u64;

        Ok(ptrs)
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
        let _ptr1 = seg.append(&e1).await.unwrap();

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

    #[tokio::test]
    async fn append_batch_roundtrip() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();
        let flags = OpenFlags {
            create: true,
            direct: false,
        };

        let mut seg = VlogSegment::<BufferedIo>::open(1, path, flags).unwrap();

        let entries = vec![
            VlogEntry::<String, String, u64> {
                key: "key-a".into(),
                timestamp: 10,
                value: Some("val-a".into()),
            },
            VlogEntry {
                key: "key-b".into(),
                timestamp: 10,
                value: Some("val-b".into()),
            },
            VlogEntry {
                key: "key-c".into(),
                timestamp: 10,
                value: None, // tombstone
            },
        ];

        let ptrs = seg.append_batch(&entries).await.unwrap();
        assert_eq!(ptrs.len(), 3);
        assert_eq!(ptrs[0].offset, 0);
        assert!(ptrs[1].offset > 0);
        assert!(ptrs[2].offset > ptrs[1].offset);

        // Read each entry back individually.
        for (i, ptr) in ptrs.iter().enumerate() {
            let e: VlogEntry<String, String, u64> = seg.read(ptr).await.unwrap();
            assert_eq!(e.key, entries[i].key);
            assert_eq!(e.timestamp, entries[i].timestamp);
            assert_eq!(e.value, entries[i].value);
        }
    }

    #[tokio::test]
    async fn append_batch_matches_individual_appends() {
        // Verify that append_batch produces the same logical layout as individual appends:
        // same value pointers and identical entry data when read back.
        let entries = vec![
            VlogEntry::<u64, String, u64> {
                key: 1,
                timestamp: 100,
                value: Some("hello".into()),
            },
            VlogEntry {
                key: 2,
                timestamp: 200,
                value: Some("world".into()),
            },
        ];

        // Path A: individual appends.
        let tmp_a = NamedTempFile::new().unwrap();
        let flags = OpenFlags { create: true, direct: false };
        let mut seg_a = VlogSegment::<BufferedIo>::open(1, tmp_a.path().to_path_buf(), flags).unwrap();
        let ptr_a0 = seg_a.append(&entries[0]).await.unwrap();
        let ptr_a1 = seg_a.append(&entries[1]).await.unwrap();

        // Path B: batch append.
        let tmp_b = NamedTempFile::new().unwrap();
        let mut seg_b = VlogSegment::<BufferedIo>::open(1, tmp_b.path().to_path_buf(), flags).unwrap();
        let ptrs_b = seg_b.append_batch(&entries).await.unwrap();

        // Pointers should match (same offsets and lengths).
        assert_eq!(ptr_a0, ptrs_b[0]);
        assert_eq!(ptr_a1, ptrs_b[1]);

        // Entries should read back identically from both segments.
        let e0a = seg_a.read::<u64, String, u64>(&ptr_a0).await.unwrap();
        let e0b = seg_b.read::<u64, String, u64>(&ptrs_b[0]).await.unwrap();
        assert_eq!(e0a.key, e0b.key);
        assert_eq!(e0a.timestamp, e0b.timestamp);
        assert_eq!(e0a.value, e0b.value);

        let e1a = seg_a.read::<u64, String, u64>(&ptr_a1).await.unwrap();
        let e1b = seg_b.read::<u64, String, u64>(&ptrs_b[1]).await.unwrap();
        assert_eq!(e1a.key, e1b.key);
        assert_eq!(e1a.timestamp, e1b.timestamp);
        assert_eq!(e1a.value, e1b.value);
    }

    #[tokio::test]
    async fn append_batch_recovery() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();
        let flags = OpenFlags { create: true, direct: false };

        let mut seg = VlogSegment::<BufferedIo>::open(1, path.clone(), flags).unwrap();

        let entries = vec![
            VlogEntry::<String, String, u64> {
                key: "k1".into(),
                timestamp: 1,
                value: Some("v1".into()),
            },
            VlogEntry {
                key: "k2".into(),
                timestamp: 2,
                value: Some("v2".into()),
            },
            VlogEntry {
                key: "k3".into(),
                timestamp: 3,
                value: Some("v3".into()),
            },
        ];

        seg.append_batch(&entries).await.unwrap();
        seg.sync().await.unwrap();
        drop(seg);

        // Recover from offset 0.
        let seg2 = VlogSegment::<BufferedIo>::open(1, path, flags).unwrap();
        let recovered = seg2.recover_entries::<String, String, u64>(0).await.unwrap();

        assert_eq!(recovered.len(), 3);
        for (i, (entry, _ptr)) in recovered.iter().enumerate() {
            assert_eq!(entry.key, entries[i].key);
            assert_eq!(entry.value, entries[i].value);
        }
    }

    #[tokio::test]
    async fn append_batch_empty() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();
        let flags = OpenFlags { create: true, direct: false };

        let mut seg = VlogSegment::<BufferedIo>::open(1, path, flags).unwrap();
        let ptrs = seg.append_batch::<String, String, u64>(&[]).await.unwrap();
        assert!(ptrs.is_empty());
        assert_eq!(seg.write_offset(), 0);
    }
}
