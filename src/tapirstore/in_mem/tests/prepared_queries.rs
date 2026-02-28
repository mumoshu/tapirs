use super::*;
use crate::tapirstore::TapirStore;

#[test]
fn prepared_get_and_at_timestamp() {
    let (_dir, mut store) = new_store();

    let txn = make_txn(vec![], vec![("x", Some("v1"))], vec![]);
    store.prepare(txn_id(1, 1), txn, ts(5, 1), false);

    // prepared_get returns the entry.
    let entry = store.prepared_get(&txn_id(1, 1));
    assert!(entry.is_some());
    let (commit_ts, _txn, finalized) = entry.unwrap();
    assert_eq!(*commit_ts, ts(5, 1));
    assert!(!finalized);

    // prepared_at_timestamp matches.
    assert_eq!(store.prepared_at_timestamp(&txn_id(1, 1), &ts(5, 1)), Some(false));

    // Wrong timestamp returns None.
    assert!(store.prepared_at_timestamp(&txn_id(1, 1), &ts(99, 1)).is_none());

    // Non-existent txn returns None.
    assert!(store.prepared_get(&txn_id(99, 99)).is_none());
}

#[test]
fn set_prepared_finalized() {
    let (_dir, mut store) = new_store();

    let txn = make_txn(vec![], vec![("x", Some("v1"))], vec![]);
    store.prepare(txn_id(1, 1), txn, ts(5, 1), false);

    // Mark as finalized.
    assert!(store.set_prepared_finalized(&txn_id(1, 1), &ts(5, 1)));

    // Verify it's finalized.
    let entry = store.prepared_get(&txn_id(1, 1)).unwrap();
    assert!(entry.2); // finalized flag

    // Wrong timestamp returns false.
    assert!(!store.set_prepared_finalized(&txn_id(1, 1), &ts(99, 1)));
}

#[test]
fn oldest_prepared_returns_min_timestamp() {
    let (_dir, mut store) = new_store();

    assert!(store.oldest_prepared().is_none());

    let txn1 = make_txn(vec![], vec![("a", Some("v1"))], vec![]);
    let txn2 = make_txn(vec![], vec![("b", Some("v2"))], vec![]);
    store.prepare(txn_id(1, 1), txn1, ts(20, 1), false);
    store.prepare(txn_id(2, 1), txn2, ts(10, 1), false);

    let (id, commit_ts, _txn) = store.oldest_prepared().unwrap();
    assert_eq!(id, txn_id(2, 1));
    assert_eq!(commit_ts, ts(10, 1));
}

#[test]
fn remove_unfinalized_prepared() {
    let (_dir, mut store) = new_store();

    let txn1 = make_txn(vec![], vec![("a", Some("v1"))], vec![]);
    let txn2 = make_txn(vec![], vec![("b", Some("v2"))], vec![]);
    store.prepare(txn_id(1, 1), txn1, ts(5, 1), false);
    store.prepare(txn_id(2, 1), txn2, ts(10, 1), false);

    // Finalize only txn 1.
    store.set_prepared_finalized(&txn_id(1, 1), &ts(5, 1));

    assert_eq!(store.prepared_count(), 2);

    // Remove unfinalized — should remove txn 2 but keep txn 1.
    store.remove_unfinalized_prepared();

    assert_eq!(store.prepared_count(), 1);
    assert!(store.prepared_get(&txn_id(1, 1)).is_some());
    assert!(store.prepared_get(&txn_id(2, 1)).is_none());
}
