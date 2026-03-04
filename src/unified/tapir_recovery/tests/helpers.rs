#![allow(dead_code)]

use crate::ir::OpId;
use crate::mvcc::disk::memory_io::MemoryIo;
use crate::occ::{ScanEntry, SharedTransaction, Transaction, TransactionId};
use crate::tapir::{ShardNumber, Sharded, Timestamp};
use crate::unified::ir::record::{IrMemEntry, IrState, PrepareRef};
pub(crate) use crate::unified::tapir_recovery::teststore::TestStore;
use crate::IrClientId;
use std::path::Path;
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

pub fn prepare_and_commit(
    store: &mut TestStore,
    prepare_op_id: OpId,
    commit_op_id: OpId,
    txn_id: TransactionId,
    writes: Vec<(&str, Option<&str>)>,
    commit_ts: Timestamp,
) {
    let txn = make_txn(vec![], writes);
    store.prepare(prepare_op_id, txn_id, txn.clone(), commit_ts, true);
    store.commit(
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

pub fn list_store_files(base_dir: &Path) -> Vec<(String, usize)> {
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

pub fn assert_store_file_names(base_dir: &Path, expected_names: &[&str]) {
    let files = list_store_files(base_dir);
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

pub fn assert_store_file_size(base_dir: &Path, file_name: &str, expected: usize) {
    let actual = get_store_file_size(base_dir, file_name);
    assert_eq!(
        actual, expected,
        "File {file_name:?} size mismatch: expected {expected}, got {actual}"
    );
}

pub fn assert_store_file_size_positive(base_dir: &Path, file_name: &str) {
    let files = list_store_files(base_dir);
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

pub fn get_store_file_size(base_dir: &Path, file_name: &str) -> usize {
    let files = list_store_files(base_dir);
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
