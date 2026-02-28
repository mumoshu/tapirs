use super::*;
use crate::tapirstore::TapirStore;

#[test]
fn txn_log_insert_get_contains_len() {
    let (_dir, mut store) = new_store();

    assert_eq!(store.txn_log_len(), 0);
    assert!(!store.txn_log_contains(&txn_id(1, 1)));
    assert!(store.txn_log_get(&txn_id(1, 1)).is_none());

    // Insert committed.
    let prev = store.txn_log_insert(txn_id(1, 1), ts(10, 1), true);
    assert!(prev.is_none());
    assert_eq!(store.txn_log_len(), 1);
    assert!(store.txn_log_contains(&txn_id(1, 1)));

    let (log_ts, committed) = store.txn_log_get(&txn_id(1, 1)).unwrap();
    assert_eq!(log_ts, ts(10, 1));
    assert!(committed);

    // Insert aborted (different txn).
    store.txn_log_insert(txn_id(2, 1), Timestamp::default(), false);
    assert_eq!(store.txn_log_len(), 2);
}

#[test]
fn txn_log_insert_returns_previous_on_duplicate() {
    let (_dir, mut store) = new_store();

    store.txn_log_insert(txn_id(1, 1), ts(10, 1), true);

    // Insert again with same id returns previous value.
    let prev = store.txn_log_insert(txn_id(1, 1), ts(20, 1), false);
    assert!(prev.is_some());
    let (prev_ts, prev_committed) = prev.unwrap();
    assert_eq!(prev_ts, ts(10, 1));
    assert!(prev_committed);

    // Current value is the new one.
    let (log_ts, committed) = store.txn_log_get(&txn_id(1, 1)).unwrap();
    assert_eq!(log_ts, ts(20, 1));
    assert!(!committed);
}
