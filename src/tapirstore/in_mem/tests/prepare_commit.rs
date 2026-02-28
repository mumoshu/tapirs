use super::*;
use crate::occ::PrepareResult;
use crate::tapirstore::TapirStore;

#[test]
fn prepare_commit_read() {
    let (_dir, mut store) = new_store();

    // Write initial value via direct put (simulates prior committed data).
    store.occ_mut().put("x".into(), Some("v1".into()), ts(1, 1));

    // Prepare a write transaction.
    let txn = make_txn(
        vec![("x", ts(1, 1))],
        vec![("x", Some("v2"))],
        vec![],
    );
    let result = store.prepare(txn_id(1, 1), txn.clone(), ts(5, 1), false);
    assert_eq!(result, PrepareResult::Ok);

    // Commit.
    store.commit(txn_id(1, 1), &txn, ts(5, 1));

    // MVCC read should return committed value.
    let (val, write_ts) = store.get_at(&"x".to_string(), ts(10, 1));
    assert_eq!(val, Some("v2".to_string()));
    assert_eq!(write_ts, ts(5, 1));
}

#[test]
fn commit_records_in_txn_log() {
    let (_dir, mut store) = new_store();

    store.occ_mut().put("x".into(), Some("v1".into()), ts(1, 1));

    let txn = make_txn(
        vec![("x", ts(1, 1))],
        vec![("x", Some("v2"))],
        vec![],
    );
    store.prepare(txn_id(1, 1), txn.clone(), ts(5, 1), false);

    // Before commit, txn_log should be empty.
    assert!(store.txn_log_get(&txn_id(1, 1)).is_none());

    // Record outcome in txn_log (as TapirReplica does in exec_inconsistent).
    store.txn_log_insert(txn_id(1, 1), ts(5, 1), true);
    store.commit(txn_id(1, 1), &txn, ts(5, 1));

    let (log_ts, committed) = store.txn_log_get(&txn_id(1, 1)).unwrap();
    assert_eq!(log_ts, ts(5, 1));
    assert!(committed);
}

#[test]
fn commit_and_log_writes_both() {
    let (_dir, mut store) = new_store();

    store.occ_mut().put("x".into(), Some("v1".into()), ts(1, 1));

    let txn = make_txn(
        vec![("x", ts(1, 1))],
        vec![("x", Some("v2"))],
        vec![],
    );
    store.prepare(txn_id(1, 1), txn.clone(), ts(5, 1), false);

    // commit_and_log should record in txn_log and apply writes.
    store.commit_and_log(txn_id(1, 1), &txn, ts(5, 1));

    // txn_log should have the entry.
    let (log_ts, committed) = store.txn_log_get(&txn_id(1, 1)).unwrap();
    assert_eq!(log_ts, ts(5, 1));
    assert!(committed);

    // MVCC read should return committed value.
    let (val, write_ts) = store.get_at(&"x".to_string(), ts(10, 1));
    assert_eq!(val, Some("v2".to_string()));
    assert_eq!(write_ts, ts(5, 1));

    // Idempotent: calling again should not panic.
    store.commit_and_log(txn_id(1, 1), &txn, ts(5, 1));
}
