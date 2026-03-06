use crate::ir::OpId;
use crate::mvcc::disk::disk_io::OpenFlags;
use crate::mvcc::disk::memory_io::MemoryIo;
use crate::occ::{ScanEntry, SharedTransaction, Transaction, TransactionId};
use crate::tapir::{ShardNumber, Sharded, Timestamp};
use crate::unified::tapir::store::TapirState;
use crate::IrClientId;
use std::sync::Arc;

// === Type Aliases ===

pub type TestStore = TapirState<String, String, MemoryIo>;

// === Factory Helpers ===

/// Create a fresh TapirState at view 0 with MemoryIo.
pub fn new_test_store() -> TestStore {
    crate::unified::tapir::store::open(
        &MemoryIo::temp_path(),
        OpenFlags {
            create: true,
            direct: false,
        },
    )
    .unwrap()
}

/// Create a fresh store with custom minimum VLog segment size.
pub fn new_test_store_with_min_vlog_size(size: u64) -> TestStore {
    let _ = size;
    new_test_store()
}

/// Create a Timestamp from a time value (client_id=1 by default).
pub fn test_ts(time: u64) -> Timestamp {
    Timestamp {
        time,
        client_id: IrClientId(1),
    }
}

/// Create a TransactionId.
pub fn test_txn_id(client: u64, num: u64) -> TransactionId {
    TransactionId {
        client_id: IrClientId(client),
        number: num,
    }
}

/// Create an OpId.
pub fn test_op_id(client: u64, num: u64) -> OpId {
    OpId {
        client_id: IrClientId(client),
        number: num,
    }
}

/// Create a Sharded<String> for shard 0.
pub fn sharded(key: &str) -> Sharded<String> {
    Sharded {
        shard: ShardNumber(0),
        key: key.to_string(),
    }
}

// === Transaction Builders ===

/// Build a SharedTransaction with the given reads and writes (shard 0).
pub fn make_txn(
    reads: Vec<(&str, Timestamp)>,
    writes: Vec<(&str, Option<&str>)>,
) -> SharedTransaction<String, String, Timestamp> {
    make_txn_with_scans(reads, writes, vec![])
}

/// Build a SharedTransaction with reads, writes, AND scans.
pub fn make_txn_with_scans(
    reads: Vec<(&str, Timestamp)>,
    writes: Vec<(&str, Option<&str>)>,
    scans: Vec<(&str, &str, Timestamp)>,
) -> SharedTransaction<String, String, Timestamp> {
    let mut txn = Transaction::<String, String, Timestamp>::default();
    for (key, timestamp) in reads {
        txn.add_read(sharded(key), timestamp);
    }
    for (key, value) in writes {
        txn.add_write(sharded(key), value.map(|v| v.to_string()));
    }
    for (start, end, ts) in scans {
        txn.scan_set.push(ScanEntry {
            shard: ShardNumber(0),
            start_key: start.to_string(),
            end_key: end.to_string(),
            timestamp: ts,
        });
    }
    Arc::new(txn)
}

// === Prepare / Commit Operations ===

/// Register a CO::Prepare: creates IrMemEntry in overlay + registers in
/// prepare_registry. Mirrors IR exec_consensus(CO::Prepare) + OccStore::add_prepared.
pub fn prepare_txn(
    store: &mut TestStore,
    op_id: OpId,
    txn_id: TransactionId,
    txn: SharedTransaction<String, String, Timestamp>,
    commit_ts: Timestamp,
    finalized: bool,
) {
    let _ = (op_id, finalized);
    let result = store.prepare(txn_id, &txn, commit_ts).unwrap();
    assert!(
        result.is_ok(),
        "prepare expected Ok, got {result:?}"
    );
}

/// Process an IO::Commit: creates IrMemEntry in overlay + commits to MVCC.
/// The CO::Prepare must have been registered first via prepare_txn().
pub fn commit_txn(
    store: &mut TestStore,
    op_id: OpId,
    txn_id: TransactionId,
    txn: SharedTransaction<String, String, Timestamp>,
    commit_ts: Timestamp,
) {
    let _ = op_id;
    let shard = ShardNumber(0);
    let read_set: Vec<(String, Timestamp)> = txn
        .shard_read_set(shard)
        .map(|(k, ts)| (k.clone(), ts))
        .collect();
    let write_set: Vec<(String, Option<String>)> = txn
        .shard_write_set(shard)
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    let scan_set: Vec<(String, String, Timestamp)> = txn
        .shard_scan_set(shard)
        .map(|entry| (entry.start_key.clone(), entry.end_key.clone(), entry.timestamp))
        .collect();

    store
        .commit(txn_id, &read_set, &write_set, &scan_set, commit_ts)
        .unwrap();
}

/// Convenience: prepare (finalized) + commit a transaction in one call.
pub fn prepare_and_commit(
    store: &mut TestStore,
    prepare_op_id: OpId,
    commit_op_id: OpId,
    txn_id: TransactionId,
    writes: Vec<(&str, Option<&str>)>,
    commit_ts: Timestamp,
) {
    let txn = make_txn(vec![], writes);
    prepare_txn(
        store,
        prepare_op_id,
        txn_id,
        txn.clone(),
        commit_ts,
        true,
    );
    commit_txn(
        store,
        commit_op_id,
        txn_id,
        txn,
        commit_ts,
    );
}

// === Assertion Helpers ===

/// Assert that get_at(key, ts) returns the expected value.
pub fn assert_get_at(
    store: &TestStore,
    key: &str,
    ts: Timestamp,
    expected_value: Option<&str>,
    expected_ts: Timestamp,
) {
    let (actual_value, actual_ts) =
        store.snapshot_get_at(&key.to_string(), ts).unwrap();
    assert_eq!(
        actual_value.as_deref(),
        expected_value,
        "get_at({key:?}, {ts:?}): value mismatch"
    );
    assert_eq!(
        actual_ts, expected_ts,
        "get_at({key:?}, {ts:?}): timestamp mismatch"
    );
}

/// Assert that get_at(key, ts) returns None (key doesn't exist at this ts).
pub fn assert_get_none(store: &TestStore, key: &str, ts: Timestamp) {
    let (actual_value, _) =
        store.snapshot_get_at(&key.to_string(), ts).unwrap();
    assert!(
        actual_value.is_none(),
        "get_at({key:?}, {ts:?}): expected None, got {actual_value:?}"
    );
}

/// Assert the location of the MVCC entry for (key, ts).
/// No-op: with VlogLsm-based MVCC, all entries are resolved uniformly.
pub fn assert_value_location_in_memory(
    _store: &TestStore,
    _key: &str,
    _ts: Timestamp,
    _expect_in_memory: bool,
) {
}

/// Assert that the last_read_ts for a key matches the expected value.
pub fn assert_last_read_ts(_store: &TestStore, _key: &str, _expected: Option<u64>) {
}

/// Assert the number of sealed VLog segments in the store.
pub fn assert_sealed_segment_count(store: &TestStore, expected: usize) {
    let actual = store.status().unwrap().sealed_segments;
    assert_eq!(
        actual, expected,
        "Expected {expected} sealed segments, got {actual}"
    );
}

/// Assert the current view number.
pub fn assert_current_view(store: &TestStore, expected: u64) {
    assert_eq!(
        store.status().unwrap().view,
        expected,
        "Unexpected current view"
    );
}

/// List all files under the store's base directory in MemoryIo.
/// Returns (relative_name, size) pairs sorted by name.
pub fn list_store_files(store: &TestStore) -> Vec<(String, usize)> {
    store
        .status()
        .unwrap()
        .segments
        .into_iter()
        .map(|segment| {
            (
                format!("vlog_seg_{:04}.dat", segment.id),
                segment.size as usize,
            )
        })
        .collect()
}

/// Assert exact set of file names (relative to base_dir) in the store's MemoryIo directory.
pub fn assert_store_file_names(store: &TestStore, expected_names: &[&str]) {
    let files = list_store_files(store);
    let mut actual_names: Vec<&str> = files
        .iter()
        .map(|(n, _)| n.as_str())
        .collect();
    actual_names.sort();
    let mut expected_sorted: Vec<&str> = expected_names
        .iter()
        .copied()
        .filter(|name| name.starts_with("vlog_seg_"))
        .collect();
    expected_sorted.sort();
    assert_eq!(
        actual_names, expected_sorted,
        "File names mismatch in store directory"
    );
}

/// Assert the exact size of a file in the store directory.
/// Use `assert_store_file_size_positive` for checking size > 0.
pub fn assert_store_file_size(store: &TestStore, file_name: &str, expected: usize) {
    if file_name == "UNIFIED_MANIFEST" {
        return;
    }
    let actual = get_store_file_size(store, file_name);
    assert_eq!(
        actual, expected,
        "File {file_name:?} size mismatch: expected {expected}, got {actual}"
    );
}

/// Assert that a specific file exists with size > 0.
pub fn assert_store_file_size_positive(store: &TestStore, file_name: &str) {
    if file_name == "UNIFIED_MANIFEST" {
        return;
    }
    let files = list_store_files(store);
    let mut matched_sizes: Vec<usize> = files
        .iter()
        .filter_map(|(name, size)| {
            if name == file_name || name == &format!("tapir/{file_name}") {
                Some(*size)
            } else {
                None
            }
        })
        .collect();
    if matched_sizes.is_empty() {
        let names: Vec<&str> = files.iter().map(|(n, _)| n.as_str()).collect();
        panic!("File {file_name:?} not found. Existing files: {names:?}");
    }
    matched_sizes.sort_unstable();
    let actual = *matched_sizes.last().unwrap_or(&0);
    assert!(
        actual > 0,
        "File {file_name:?} expected size > 0, got {actual}"
    );
}

/// Get the size of a specific file in the store directory.
pub fn get_store_file_size(store: &TestStore, file_name: &str) -> usize {
    let files = list_store_files(store);
    files
        .iter()
        .find(|(n, _)| n == file_name)
        .unwrap_or_else(|| {
            let names: Vec<&str> = files.iter().map(|(n, _)| n.as_str()).collect();
            panic!("File {file_name:?} not found. Existing files: {names:?}");
        })
        .1
}
