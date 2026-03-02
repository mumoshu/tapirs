use crate::mvcc::disk::{DiskStore, disk_io::BufferedIo};
use crate::occ::{ScanEntry, SharedTransaction, Transaction, TransactionId};
use crate::tapir::{ShardNumber, Sharded, Timestamp};
use crate::tapirstore::InMemTapirStore;
use crate::IrClientId;
use std::sync::Arc;
use tempfile::TempDir;

type TS = Timestamp;
type Backend = DiskStore<String, String, TS, BufferedIo>;
type TestStore = InMemTapirStore<String, String, Backend>;

fn ts(time: u64, client_id: u64) -> TS {
    Timestamp {
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

fn new_store() -> (TempDir, TestStore) {
    let dir = TempDir::new().unwrap();
    let backend = DiskStore::open(dir.path().to_path_buf()).unwrap();
    (dir, InMemTapirStore::new_with_backend(ShardNumber(0), true, backend))
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

// ---------------------------------------------------------------------------
// Conformance tests (generated via macro — trait-only, reusable)
// ---------------------------------------------------------------------------

crate::tapir_store_conformance_tests!(new_store());

// ---------------------------------------------------------------------------
// Impl-specific tests (use InMemTapirStore-only methods)
// ---------------------------------------------------------------------------

#[test]
fn min_prepare_time_round_trip() {
    let (_dir, mut store) = new_store();

    assert_eq!(store.min_prepare_time(), 0);
    store.set_min_prepare_time(100);
    assert_eq!(store.min_prepare_time(), 100);
}

#[test]
fn finalized_min_prepare_time_round_trip() {
    let (_dir, mut store) = new_store();

    assert_eq!(store.finalized_min_prepare_time(), 0);
    store.set_finalized_min_prepare_time(200);
    assert_eq!(store.finalized_min_prepare_time(), 200);
}

#[test]
fn commit_records_in_txn_log() {
    use crate::tapirstore::TapirStore;

    let (_dir, mut store) = new_store();

    // Setup: commit "x" = "v1" at ts(1,1) via OCC flow.
    let setup_txn = make_txn(vec![], vec![("x", Some("v1"))], vec![]);
    assert_eq!(
        store.occ_mut().try_prepare_txn(txn_id(99, 1), setup_txn.clone(), ts(1, 1)),
        crate::occ::PrepareResult::Ok
    );
    store.commit(txn_id(99, 1), &setup_txn, ts(1, 1));

    let txn = make_txn(
        vec![("x", ts(1, 1))],
        vec![("x", Some("v2"))],
        vec![],
    );
    store.try_prepare_txn(txn_id(1, 1), txn.clone(), ts(5, 1));

    // Before commit, txn_log should be empty.
    assert!(store.txn_log_get(&txn_id(1, 1)).is_none());

    // Record outcome in txn_log (as TapirReplica does in exec_inconsistent).
    store.txn_log_insert(txn_id(1, 1), ts(5, 1), true);
    store.commit(txn_id(1, 1), &txn, ts(5, 1));

    let (log_ts, committed) = store.txn_log_get(&txn_id(1, 1)).unwrap();
    assert_eq!(log_ts, ts(5, 1));
    assert!(committed);
}
