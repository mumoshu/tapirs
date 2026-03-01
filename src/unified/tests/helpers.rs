use crate::ir::OpId;
use crate::mvcc::backend::MvccBackend;
use crate::mvcc::disk::memory_io::MemoryIo;
use crate::occ::{ScanEntry, SharedTransaction, Transaction, TransactionId};
use crate::tapir::{ShardNumber, Sharded, Timestamp};
use crate::unified::mvcc_backend::UnifiedMvccBackend;
use crate::unified::types::*;
use crate::unified::UnifiedStore;
use crate::IrClientId;
use std::sync::Arc;

// === Type Aliases ===

pub type TestStore = UnifiedMvccBackend<String, String, MemoryIo>;
pub type TestUnifiedStore = UnifiedStore<String, String, MemoryIo>;

// === Factory Helpers ===

/// Create a fresh UnifiedMvccBackend at view 0 with MemoryIo.
pub fn new_test_store() -> TestStore {
    let store = TestUnifiedStore::open(MemoryIo::temp_path()).unwrap();
    TestStore::new(store)
}

/// Create a fresh store with custom minimum VLog segment size.
pub fn new_test_store_with_min_vlog_size(size: u64) -> TestStore {
    let store =
        TestUnifiedStore::open_with_options(MemoryIo::temp_path(), size).unwrap();
    TestStore::new(store)
}

/// Create a Timestamp from a time value (client_id=1 by default).
pub fn test_ts(time: u64) -> Timestamp {
    Timestamp {
        time,
        client_id: IrClientId(1),
    }
}

/// Create a Timestamp with explicit client_id.
pub fn test_ts_client(time: u64, client: u64) -> Timestamp {
    Timestamp {
        time,
        client_id: IrClientId(client),
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
    // Insert CO::Prepare IrMemEntry into the IR overlay
    let current_view = store.inner().current_view();
    store.inner_mut().insert_ir_entry(
        op_id,
        IrMemEntry {
            entry_type: VlogEntryType::Prepare,
            state: if finalized {
                IrState::Finalized(current_view)
            } else {
                IrState::Tentative
            },
            payload: IrPayloadInline::Prepare {
                transaction_id: txn_id,
                commit_ts,
                read_set: txn
                    .shard_read_set(ShardNumber(0))
                    .map(|(k, ts)| (bitcode::serialize(k).unwrap_or_default(), ts))
                    .collect(),
                write_set: txn
                    .shard_write_set(ShardNumber(0))
                    .map(|(k, v)| {
                        (
                            bitcode::serialize(k).unwrap_or_default(),
                            v.as_ref()
                                .map(|s| bitcode::serialize(s).unwrap_or_default())
                                .unwrap_or_default(),
                        )
                    })
                    .collect(),
                scan_set: vec![],
            },
        },
    );

    // Register in prepare_registry (mirrors OccStore::add_prepared → register_prepare)
    store.register_prepare(txn_id, &txn, commit_ts);
}

/// Process an IO::Commit: creates IrMemEntry in overlay + commits to MVCC.
/// The CO::Prepare must have been registered first via prepare_txn().
pub fn commit_txn(
    store: &mut TestStore,
    op_id: OpId,
    txn_id: TransactionId,
    txn: &SharedTransaction<String, String, Timestamp>,
    commit_ts: Timestamp,
    prepare_ref: PrepareRef,
) {
    // Insert IO::Commit IrMemEntry into the IR overlay
    let current_view = store.inner().current_view();
    store.inner_mut().insert_ir_entry(
        op_id,
        IrMemEntry {
            entry_type: VlogEntryType::Commit,
            state: IrState::Finalized(current_view),
            payload: IrPayloadInline::Commit {
                transaction_id: txn_id,
                commit_ts,
                prepare_ref,
            },
        },
    );

    // Commit via MvccBackend (mirrors OccStore::commit → commit_batch_for_transaction)
    let writes: Vec<(String, Option<String>)> = txn
        .shard_write_set(ShardNumber(0))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    let reads: Vec<(String, Timestamp)> = txn
        .shard_read_set(ShardNumber(0))
        .map(|(k, ts)| (k.clone(), ts))
        .collect();
    store
        .commit_batch_for_transaction(txn_id, writes, reads, commit_ts)
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
        &txn,
        commit_ts,
        PrepareRef::SameView(prepare_op_id),
    );
}

// === View Change Operations ===

/// Seal the current view.
pub fn seal_view(store: &mut TestStore) {
    store.inner_mut().seal_current_view().unwrap();
}

/// Build a minimal merged record containing given finalized entries.
pub fn build_merged_record(
    entries: Vec<(OpId, IrMemEntry)>,
    target_view: u64,
) -> Vec<(OpId, IrMemEntry)> {
    entries
        .into_iter()
        .map(|(op_id, mut entry)| {
            entry.state = IrState::Finalized(target_view);
            (op_id, entry)
        })
        .collect()
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
        MvccBackend::get_at(store, &key.to_string(), ts).unwrap();
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
        MvccBackend::get_at(store, &key.to_string(), ts).unwrap();
    assert!(
        actual_value.is_none(),
        "get_at({key:?}, {ts:?}): expected None, got {actual_value:?}"
    );
}

/// Assert the ValueLocation type of the MVCC entry for (key, ts).
/// `expect_in_memory=true` → ValueLocation::InMemory (current view).
/// `expect_in_memory=false` → ValueLocation::OnDisk (sealed VLog).
pub fn assert_value_location_in_memory(
    store: &TestStore,
    key: &str,
    ts: Timestamp,
    expect_in_memory: bool,
) {
    let (_, entry) = store
        .inner()
        .unified_memtable()
        .get_at(&key.to_string(), ts)
        .expect("MVCC entry not found");
    match &entry.value_ref {
        Some(ValueLocation::InMemory { .. }) => {
            assert!(expect_in_memory, "Expected OnDisk, got InMemory");
        }
        Some(ValueLocation::OnDisk(_)) => {
            assert!(!expect_in_memory, "Expected InMemory, got OnDisk");
        }
        None => panic!("Expected value_ref, got None (tombstone)"),
    }
}

/// Assert that the last_read_ts for a key matches the expected value.
pub fn assert_last_read_ts(store: &TestStore, key: &str, expected: Option<u64>) {
    let actual = MvccBackend::get_last_read(store, &key.to_string()).unwrap();
    let actual_time = actual.map(|ts| ts.time);
    assert_eq!(
        actual_time, expected,
        "last_read_ts({key:?}): expected {expected:?}, got {actual_time:?}"
    );
}

/// Assert the number of sealed VLog segments in the store.
pub fn assert_sealed_segment_count(store: &TestStore, expected: usize) {
    let actual = store.inner().sealed_vlog_segments().len();
    assert_eq!(
        actual, expected,
        "Expected {expected} sealed segments, got {actual}"
    );
}

/// Assert the current view number.
pub fn assert_current_view(store: &TestStore, expected: u64) {
    assert_eq!(
        store.inner().current_view(),
        expected,
        "Unexpected current view"
    );
}

/// List all files under the store's base directory in MemoryIo.
/// Returns (relative_name, size) pairs sorted by name.
pub fn list_store_files(store: &TestStore) -> Vec<(String, usize)> {
    let base_dir = store.inner().base_dir();
    let files = MemoryIo::list_files(base_dir);
    let prefix = format!("{}/", base_dir.display());
    files
        .into_iter()
        .map(|(p, size)| {
            let name = p
                .to_string_lossy()
                .strip_prefix(&prefix)
                .unwrap_or(&p.to_string_lossy())
                .to_string();
            (name, size)
        })
        .collect()
}

/// Assert exact set of file names (relative to base_dir) in the store's MemoryIo directory.
pub fn assert_store_file_names(store: &TestStore, expected_names: &[&str]) {
    let files = list_store_files(store);
    let actual_names: Vec<&str> = files.iter().map(|(n, _)| n.as_str()).collect();
    let mut expected_sorted: Vec<&str> = expected_names.to_vec();
    expected_sorted.sort();
    assert_eq!(
        actual_names, expected_sorted,
        "File names mismatch in store directory"
    );
}

/// Assert the exact size of a file in the store directory.
/// Use `assert_store_file_size_positive` for checking size > 0.
pub fn assert_store_file_size(store: &TestStore, file_name: &str, expected: usize) {
    let actual = get_store_file_size(store, file_name);
    assert_eq!(
        actual, expected,
        "File {file_name:?} size mismatch: expected {expected}, got {actual}"
    );
}

/// Assert that a specific file exists with size > 0.
pub fn assert_store_file_size_positive(store: &TestStore, file_name: &str) {
    let actual = get_store_file_size(store, file_name);
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
