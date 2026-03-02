use std::fmt;
use std::io;

/// Errors from the disk-backed storage engine.
#[derive(Debug)]
pub enum StorageError {
    /// Data corruption detected (CRC mismatch).
    Corruption {
        file: String,
        offset: u64,
        expected_crc: u32,
        actual_crc: u32,
    },
    /// VLog data is missing — the ValuePointer falls outside the valid vLog range
    /// [0, write_offset).
    ///
    /// **Direct cause**: The LSM contains a ValuePointer whose offset + length
    /// exceeds the vlog's known write extent (write_offset from the manifest or
    /// recovery scan).
    ///
    /// **Possible root causes**:
    /// 1. Crash between vlog append and manifest save — the vlog was truncated
    ///    or never fsynced, but the SST referencing this pointer survived because
    ///    it was flushed in a prior manifest cycle.
    /// 2. Crash between fsync and the OS actually persisting to non-volatile
    ///    storage (hardware write-back cache not honoring flush).
    ///
    /// **Consumer action**: The data was never durably written to this replica.
    /// A direct store user should treat this as a retrieval failure. A TAPIR
    /// replica should propagate this error to the client, which can retry the
    /// read on a different replica that may have the data. Silently returning
    /// None (as the WiscKey paper suggests for single-machine stores) would
    /// cause silent data loss if a quorum of replicas all lost the same entry
    /// in a correlated crash (e.g., datacenter power outage).
    VLogDataMissing {
        file: String,
        offset: u64,
    },

    /// VLog key+timestamp CRC mismatch — crc_kts verification failed.
    ///
    /// **Direct cause**: The CRC32 computed over `key_bytes | ts_bytes` does not
    /// match the stored crc_kts in the vlog entry.
    ///
    /// **Possible root causes**:
    /// 1. Silent disk corruption (bit-rot) in the key or timestamp region of
    ///    the vlog entry after it was originally written and verified.
    /// 2. Partial/torn write that corrupted the key or timestamp bytes but
    ///    left enough of the entry intact to be found by offset (not caught
    ///    during recovery because recovery stops at the first invalid entry,
    ///    but this entry may have been valid at recovery time and corrupted
    ///    later).
    ///
    /// **Consumer action**: The vlog data is present but the key/timestamp
    /// portion is damaged. A direct store user should treat this as a read
    /// error. A TAPIR replica should propagate the error to the client for
    /// retry on another replica. The replica may also trigger background
    /// repair (e.g., re-replicating from a healthy replica) or a view change
    /// if the corruption is persistent.
    VLogKeyCrcMismatch {
        file: String,
        offset: u64,
        expected_crc: u32,
        actual_crc: u32,
    },

    /// VLog value CRC mismatch — crc_val verification failed.
    ///
    /// **Direct cause**: The CRC32 computed over `value_bytes` does not match
    /// the stored crc_val in the vlog entry. The key and timestamp CRCs passed
    /// (crc_kts is verified first), so the key is intact but the value is
    /// damaged.
    ///
    /// **Possible root causes**:
    /// 1. Silent disk corruption (bit-rot) in the value region of the vlog
    ///    entry. The key-match check alone (from the WiscKey paper) cannot
    ///    catch this — without crc_val, a corrupted value with an intact key
    ///    would be returned to the caller as valid data.
    /// 2. Partial/torn write that corrupted only the value bytes.
    ///
    /// **Consumer action**: Same as VLogKeyCrcMismatch — propagate error,
    /// retry on another replica. This variant is distinguished from
    /// VLogKeyCrcMismatch for diagnostics: value-only corruption is the most
    /// common bit-rot pattern (values are larger and more likely to span
    /// sectors with latent defects).
    VLogValueCrcMismatch {
        file: String,
        offset: u64,
        expected_crc: u32,
        actual_crc: u32,
    },

    /// VLog key mismatch — the key deserialized from the vlog entry does not
    /// match the expected key from the LSM lookup.
    ///
    /// **Direct cause**: Both CRCs passed (crc_kts and crc_val verified
    /// successfully), but the vlog entry's key differs from the key the LSM
    /// returned during lookup.
    ///
    /// **Possible root causes** (since vlog CRCs passed, the vlog data is
    /// internally consistent — the problem is in the LSM→vlog mapping):
    /// 1. SSTable key corruption — bit-rot corrupted the key stored in the
    ///    SST, causing the LSM to return this key for a query that should have
    ///    matched a different entry. The vlog entry pointed to is valid but
    ///    belongs to a different key.
    /// 2. SSTable ValuePointer corruption — the key in the SST is correct,
    ///    but the ValuePointer's offset or segment_id was corrupted by bit-rot,
    ///    causing it to point to a valid vlog entry that belongs to a different
    ///    key.
    /// 3. VLog CRC false collision (~1/2³² per CRC, negligible) — the vlog
    ///    data is actually corrupt, but the corruption happened to produce
    ///    bytes that pass CRC verification. Extremely unlikely but
    ///    theoretically possible.
    ///
    /// **Consumer action**: Same as CRC mismatch variants — propagate error,
    /// retry on another replica. This variant specifically indicates an
    /// LSM-layer integrity issue (root causes 1-2), which may warrant
    /// additional diagnostics such as SSTable CRC verification or compaction
    /// to rebuild the LSM from known-good vlog data.
    VLogKeyMismatch {
        file: String,
        offset: u64,
    },

    /// No prepared transaction found for the given transaction ID.
    ///
    /// Returned by `commit_prepared` when neither the
    /// in-memory `prepare_registry` nor the on-disk `prepare_vlog_index`
    /// contains the transaction. This indicates either a caller bug
    /// (forgot to call `register_prepare` before commit) or data loss
    /// (UnifiedStore lost a previously prepared transaction).
    PrepareNotFound {
        client_id: u64,
        txn_number: u64,
    },

    /// Underlying I/O error.
    Io(io::Error),
    /// Serialization/deserialization error.
    Codec(String),
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Corruption {
                file,
                offset,
                expected_crc,
                actual_crc,
            } => write!(
                f,
                "corruption in {file} at offset {offset}: \
                 expected CRC {expected_crc:#010x}, got {actual_crc:#010x}"
            ),
            Self::VLogDataMissing { file, offset } => write!(
                f,
                "vlog data missing in {file} at offset {offset}: \
                 ValuePointer falls outside valid vLog range"
            ),
            Self::VLogKeyCrcMismatch {
                file,
                offset,
                expected_crc,
                actual_crc,
            } => write!(
                f,
                "vlog key+timestamp CRC mismatch in {file} at offset {offset}: \
                 expected {expected_crc:#010x}, got {actual_crc:#010x}"
            ),
            Self::VLogValueCrcMismatch {
                file,
                offset,
                expected_crc,
                actual_crc,
            } => write!(
                f,
                "vlog value CRC mismatch in {file} at offset {offset}: \
                 expected {expected_crc:#010x}, got {actual_crc:#010x}"
            ),
            Self::VLogKeyMismatch { file, offset } => write!(
                f,
                "vlog key mismatch in {file} at offset {offset}: \
                 entry key does not match expected key from LSM"
            ),
            Self::PrepareNotFound {
                client_id,
                txn_number,
            } => write!(
                f,
                "prepare not found: client_id={client_id}, txn_number={txn_number}"
            ),
            Self::Io(e) => write!(f, "I/O error: {e}"),
            Self::Codec(msg) => write!(f, "codec error: {msg}"),
        }
    }
}

impl std::error::Error for StorageError {}

impl From<io::Error> for StorageError {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}
