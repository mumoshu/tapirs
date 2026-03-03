#![allow(dead_code)]

use crate::ir::OpId;
use crate::mvcc::disk::memory_io::MemoryIo;
use crate::occ::{ScanEntry, SharedTransaction, Transaction, TransactionId};
use crate::tapir::{ShardNumber, Sharded, Timestamp};
use crate::unified::ir::record::{IrMemEntry, IrPayloadInline, IrState, PrepareRef, VlogEntryType};
use crate::unified::tapir::storage_types::ValueLocation;
pub(crate) use crate::unified::tapir_recovery::teststore::TestStore;
use crate::IrClientId;
use std::sync::Arc;

// === Factory Helpers ===

pub fn new_test_store() -> TestStore {
    TestStore::open(MemoryIo::temp_path()).unwrap()
}

pub fn new_test_store_with_min_vlog_size(size: u64) -> TestStore {
    TestStore::open_with_options(MemoryIo::temp_path(), size).unwrap()
}

pub fn test_ts(time: u64) -> Timestamp {
    Timestamp {
        time,
        client_id: IrClientId(1),
    }
}

pub fn test_ts_client(time: u64, client: u64) -> Timestamp {
    Timestamp {
        time,
        client_id: IrClientId(client),
    }
}

pub fn test_txn_id(client: u64, num: u64) -> TransactionId {
    TransactionId {
        client_id: IrClientId(client),
        number: num,
    }
}

pub fn test_op_id(client: u64, num: u64) -> OpId {
    OpId {
        client_id: IrClientId(client),
        number: num,
    }
}

pub fn sharded(key: &str) -> Sharded<String> {
    Sharded {
        shard: ShardNumber(0),
        key: key.to_string(),
    }
}

pub fn make_txn(
    reads: Vec<(&str, Timestamp)>,
    writes: Vec<(&str, Option<&str>)>,
) -> SharedTransaction<String, String, Timestamp> {
    make_txn_with_scans(reads, writes, vec![])
}

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

pub fn build_txn_from_parts(
    read_set: &[(String, Timestamp)],
    write_set: &[(String, Option<String>)],
    scan_set: &[(String, String, Timestamp)],
) -> SharedTransaction<String, String, Timestamp> {
    let mut txn = Transaction::<String, String, Timestamp>::default();
    for (k, ts) in read_set {
        txn.add_read(sharded(k), *ts);
    }
    for (k, v) in write_set {
        txn.add_write(sharded(k), v.clone());
    }
    for (start, end, ts) in scan_set {
        txn.scan_set.push(ScanEntry {
            shard: ShardNumber(0),
            start_key: start.clone(),
            end_key: end.clone(),
            timestamp: *ts,
        });
    }
    Arc::new(txn)
}

pub fn prepare_txn(
    store: &mut TestStore,
    op_id: OpId,
    txn_id: TransactionId,
    txn: SharedTransaction<String, String, Timestamp>,
    commit_ts: Timestamp,
    finalized: bool,
) {
    let current_view = store.current_view();
    store.insert_ir_entry(
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
                    .map(|(k, ts)| (k.clone(), ts))
                    .collect(),
                write_set: txn
                    .shard_write_set(ShardNumber(0))
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect(),
                scan_set: vec![],
            },
        },
    );

    store.register_prepare(txn_id, &txn, commit_ts);
}

pub fn commit_txn(
    store: &mut TestStore,
    op_id: OpId,
    txn_id: TransactionId,
    txn: SharedTransaction<String, String, Timestamp>,
    commit_ts: Timestamp,
    prepare_ref: PrepareRef,
) {
    let current_view = store.current_view();
    store.insert_ir_entry(
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

    let shard = crate::tapir::ShardNumber(0);
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
        .commit_transaction_data(txn_id, &read_set, &write_set, &scan_set, commit_ts)
        .unwrap();
}

pub fn prepare_and_commit(
    store: &mut TestStore,
    prepare_op_id: OpId,
    commit_op_id: OpId,
    txn_id: TransactionId,
    writes: Vec<(&str, Option<&str>)>,
    commit_ts: Timestamp,
) {
    let txn = make_txn(vec![], writes);
    prepare_txn(store, prepare_op_id, txn_id, txn.clone(), commit_ts, true);
    commit_txn(
        store,
        commit_op_id,
        txn_id,
        txn,
        commit_ts,
        PrepareRef::SameView(prepare_op_id),
    );
}

pub fn seal_view(store: &mut TestStore) {
    store.seal_current_view().unwrap();
}

pub fn build_merged_record(
    entries: Vec<(OpId, IrMemEntry<String, String>)>,
    target_view: u64,
) -> Vec<(OpId, IrMemEntry<String, String>)> {
    entries
        .into_iter()
        .map(|(op_id, mut entry)| {
            entry.state = IrState::Finalized(target_view);
            (op_id, entry)
        })
        .collect()
}

pub fn assert_get_at(
    store: &TestStore,
    key: &str,
    ts: Timestamp,
    expected_value: Option<&str>,
    expected_ts: Timestamp,
) {
    let (actual_value, actual_ts) = store.do_uncommitted_get_at(&key.to_string(), ts).unwrap();
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

pub fn assert_get_none(store: &TestStore, key: &str, ts: Timestamp) {
    let (actual_value, _) = store.do_uncommitted_get_at(&key.to_string(), ts).unwrap();
    assert!(
        actual_value.is_none(),
        "get_at({key:?}, {ts:?}): expected None, got {actual_value:?}"
    );
}

pub fn assert_value_location_in_memory(
    store: &TestStore,
    key: &str,
    ts: Timestamp,
    expect_in_memory: bool,
) {
    let entry = store
        .memtable_entry_at(&key.to_string(), ts)
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

pub fn assert_last_read_ts(store: &TestStore, key: &str, expected: Option<u64>) {
    let actual = store.get_last_read(&key.to_string()).unwrap();
    let actual_time = actual.map(|ts| ts.time);
    assert_eq!(
        actual_time, expected,
        "last_read_ts({key:?}): expected {expected:?}, got {actual_time:?}"
    );
}

pub fn assert_sealed_segment_count(store: &TestStore, expected: usize) {
    let actual = store.sealed_vlog_segments().len();
    assert_eq!(
        actual, expected,
        "Expected {expected} sealed segments, got {actual}"
    );
}

pub fn assert_current_view(store: &TestStore, expected: u64) {
    assert_eq!(store.current_view(), expected, "Unexpected current view");
}

pub fn list_store_files(store: &TestStore) -> Vec<(String, usize)> {
    let base_dir = store.base_dir();
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

pub fn assert_store_file_names(store: &TestStore, expected_names: &[&str]) {
    let files = list_store_files(store);
    let mut actual_names: Vec<&str> = files
        .iter()
        .map(|(n, _)| n.as_str())
        .filter(|name| !name.starts_with("tapir/"))
        .collect();
    actual_names.sort();
    let mut expected_sorted: Vec<&str> = expected_names.to_vec();
    expected_sorted.sort();
    assert_eq!(
        actual_names, expected_sorted,
        "File names mismatch in store directory"
    );
}

pub fn assert_store_file_size(store: &TestStore, file_name: &str, expected: usize) {
    let actual = get_store_file_size(store, file_name);
    assert_eq!(
        actual, expected,
        "File {file_name:?} size mismatch: expected {expected}, got {actual}"
    );
}

pub fn assert_store_file_size_positive(store: &TestStore, file_name: &str) {
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

pub fn get_store_file_size(store: &TestStore, file_name: &str) -> usize {
    let files = list_store_files(store);
    files
        .iter()
        .find(|(n, _)| n == file_name)
        .or_else(|| {
            let tapir_name = format!("tapir/{file_name}");
            files.iter().find(|(n, _)| n == &tapir_name)
        })
        .unwrap_or_else(|| {
            let names: Vec<&str> = files.iter().map(|(n, _)| n.as_str()).collect();
            panic!("File {file_name:?} not found. Existing files: {names:?}");
        })
        .1
}
