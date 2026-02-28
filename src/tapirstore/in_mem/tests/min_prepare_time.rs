use super::*;
use crate::tapirstore::TapirStore;

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
fn min_prepared_timestamp_returns_minimum() {
    let (_dir, mut store) = new_store();

    assert!(store.min_prepared_timestamp().is_none());

    // Prepare two transactions at different timestamps.
    let txn1 = make_txn(vec![], vec![("a", Some("v1"))], vec![]);
    let txn2 = make_txn(vec![], vec![("b", Some("v2"))], vec![]);
    store.prepare(txn_id(1, 1), txn1, ts(20, 1), false);
    store.prepare(txn_id(2, 1), txn2, ts(10, 1), false);

    assert_eq!(store.min_prepared_timestamp(), Some(10));
}
