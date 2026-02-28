use super::*;
use crate::occ::PrepareResult;
use crate::tapirstore::TapirStore;

#[test]
fn prepare_abort_removes_from_prepared() {
    let (_dir, mut store) = new_store();

    store.occ_mut().put("x".into(), Some("v1".into()), ts(1, 1));

    let txn = make_txn(vec![], vec![("x", Some("v2"))], vec![]);
    let result = store.prepare(txn_id(1, 1), txn, ts(5, 1), false);
    assert_eq!(result, PrepareResult::Ok);
    assert_eq!(store.prepared_count(), 1);

    // Abort (remove_prepared).
    let removed = store.remove_prepared(txn_id(1, 1));
    assert!(removed);
    assert_eq!(store.prepared_count(), 0);
}

#[test]
fn remove_prepared_returns_false_if_not_found() {
    let (_dir, mut store) = new_store();
    assert!(!store.remove_prepared(txn_id(99, 99)));
}

#[test]
fn abort_does_not_affect_txn_log() {
    let (_dir, mut store) = new_store();

    let txn = make_txn(vec![], vec![("x", Some("v2"))], vec![]);
    store.prepare(txn_id(1, 1), txn, ts(5, 1), false);

    // Record abort in txn_log.
    store.txn_log_insert(txn_id(1, 1), Timestamp::default(), false);
    store.remove_prepared(txn_id(1, 1));

    let (_, committed) = store.txn_log_get(&txn_id(1, 1)).unwrap();
    assert!(!committed);
}
