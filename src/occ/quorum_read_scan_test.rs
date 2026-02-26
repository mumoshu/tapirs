use super::{PrepareResult, Store};
use crate::mvcc::disk::{DiskStore, disk_io::BufferedIo};
use crate::occ::{ScanEntry, SharedTransaction, Transaction, TransactionId};
use crate::tapir::{ShardNumber, Sharded, Timestamp as TapirTimestamp};
use crate::IrClientId;
use std::sync::Arc;
use tempfile::TempDir;

type TS = TapirTimestamp;
type TestStore = Store<String, String, TS, DiskStore<String, String, TS, BufferedIo>>;

fn ts(time: u64, client_id: u64) -> TS {
    TapirTimestamp {
        time,
        client_id: IrClientId(client_id),
    }
}

fn txn_id(client: u64, num: u64) -> TransactionId {
    TransactionId {
        client_id: IrClientId(client),
        number: num,
    }
}

fn sharded(key: &str) -> Sharded<String> {
    Sharded {
        shard: ShardNumber(0),
        key: key.to_string(),
    }
}

fn new_store(linearizable: bool) -> (TempDir, TestStore) {
    let dir = TempDir::new().unwrap();
    let backend = DiskStore::open(dir.path().to_path_buf()).unwrap();
    (dir, Store::new_with_backend(ShardNumber(0), linearizable, backend))
}

fn make_txn(
    reads: Vec<(&str, TS)>,
    writes: Vec<(&str, Option<&str>)>,
    scans: Vec<ScanEntry<String, TS>>,
) -> SharedTransaction<String, String, TS> {
    let mut txn = Transaction::<String, String, TS>::default();
    for (key, timestamp) in reads {
        txn.add_read(sharded(key), timestamp);
    }
    for (key, value) in writes {
        txn.add_write(sharded(key), value.map(|v| v.to_string()));
    }
    txn.scan_set = scans;
    Arc::new(txn)
}

// RO after RW: the RW's IO::Commit IR Finalize has not yet been
// received by the replica. The prepared write at ts(5,1) conflicts
// with quorum_read at snapshot_ts ts(10,1).
#[test]
fn quorum_read_conflicts_with_prepared_write() {
    let (_dir, mut store) = new_store(true);
    store.put("x".into(), Some("v1".into()), ts(1, 1));
    let txn = make_txn(vec![], vec![("x", Some("v2"))], vec![]);
    assert_eq!(
        store.prepare(txn_id(1, 1), txn, ts(5, 1), false),
        PrepareResult::Ok
    );
    assert!(store.quorum_read("x".into(), ts(10, 1)).is_err());
}

// RO snapshot is before the prepared write's commit timestamp.
// The write would not be visible even if committed — no conflict.
#[test]
fn quorum_read_no_conflict_when_prepared_write_is_future() {
    let (_dir, mut store) = new_store(true);
    store.put("x".into(), Some("v1".into()), ts(1, 1));
    let txn = make_txn(vec![], vec![("x", Some("v2"))], vec![]);
    assert_eq!(
        store.prepare(txn_id(1, 1), txn, ts(15, 1), false),
        PrepareResult::Ok
    );
    let result = store.quorum_read("x".into(), ts(10, 1));
    assert!(result.is_ok());
    assert_eq!(result.unwrap().0, Some("v1".into()));
}

// RW fully committed: commit() removes the prepared entry and applies
// the write to MVCC. No conflict — quorum_read returns new value.
#[test]
fn quorum_read_no_conflict_after_commit() {
    let (_dir, mut store) = new_store(true);
    store.put("x".into(), Some("v1".into()), ts(1, 1));
    let txn = make_txn(vec![], vec![("x", Some("v2"))], vec![]);
    assert_eq!(
        store.prepare(txn_id(1, 1), txn.clone(), ts(5, 1), false),
        PrepareResult::Ok
    );
    store.commit(txn_id(1, 1), &txn, ts(5, 1));
    let result = store.quorum_read("x".into(), ts(10, 1));
    assert!(result.is_ok());
    assert_eq!(result.unwrap().0, Some("v2".into()));
}

// RW aborted: remove_prepared() clears the entry without applying
// writes. No conflict — quorum_read returns the original value.
#[test]
fn quorum_read_no_conflict_after_abort() {
    let (_dir, mut store) = new_store(true);
    store.put("x".into(), Some("v1".into()), ts(1, 1));
    let txn = make_txn(vec![], vec![("x", Some("v2"))], vec![]);
    assert_eq!(
        store.prepare(txn_id(1, 1), txn, ts(5, 1), false),
        PrepareResult::Ok
    );
    store.remove_prepared(txn_id(1, 1));
    let result = store.quorum_read("x".into(), ts(10, 1));
    assert!(result.is_ok());
    assert_eq!(result.unwrap().0, Some("v1".into()));
}

// RO scan: prepared write at key "m" (in scan range ["a","z"]) at
// ts(5,1) conflicts with quorum_scan at snapshot_ts ts(10,1).
#[test]
fn quorum_scan_conflicts_with_prepared_write_in_range() {
    let (_dir, mut store) = new_store(true);
    let txn = make_txn(vec![], vec![("m", Some("v1"))], vec![]);
    assert_eq!(
        store.prepare(txn_id(1, 1), txn, ts(5, 1), false),
        PrepareResult::Ok
    );
    assert!(store
        .quorum_scan("a".into(), "z".into(), ts(10, 1))
        .is_err());
}

// Prepared write at key "z" is outside scan range ["a","c"] — no conflict.
#[test]
fn quorum_scan_no_conflict_when_prepared_write_outside_range() {
    let (_dir, mut store) = new_store(true);
    let txn = make_txn(vec![], vec![("z", Some("v1"))], vec![]);
    assert_eq!(
        store.prepare(txn_id(1, 1), txn, ts(5, 1), false),
        PrepareResult::Ok
    );
    assert!(store
        .quorum_scan("a".into(), "c".into(), ts(10, 1))
        .is_ok());
}

// Boundary: prepared write at ts(5,1) conflicts with quorum_read at
// snapshot_ts ts(5,1). Equality satisfies ts <= snapshot_ts.
#[test]
fn quorum_read_conflicts_at_exact_timestamp() {
    let (_dir, mut store) = new_store(true);
    store.put("x".into(), Some("v1".into()), ts(1, 1));
    let txn = make_txn(vec![], vec![("x", Some("v2"))], vec![]);
    assert_eq!(
        store.prepare(txn_id(1, 1), txn, ts(5, 1), false),
        PrepareResult::Ok
    );
    assert!(store.quorum_read("x".into(), ts(5, 1)).is_err());
}
