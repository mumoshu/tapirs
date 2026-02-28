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

#[test]
fn raise_min_prepare_time_caps_at_min_prepared() {
    let (_dir, mut store) = new_store();

    // With no prepared transactions, min_prepared_ts = u64::MAX,
    // so raise should set to max(0, min(100, MAX)) = 100.
    let result = store.raise_min_prepare_time(100);
    assert_eq!(result, 100);
    assert_eq!(store.min_prepare_time(), 100);

    // Prepare a transaction at time=50.
    let txn = make_txn(vec![], vec![("a", Some("v1"))], vec![]);
    store.prepare(txn_id(1, 1), txn, ts(50, 1), false);

    // Raising to 200 should cap at min_prepared_ts=50: max(100, min(200, 50)) = 100.
    let result = store.raise_min_prepare_time(200);
    assert_eq!(result, 100);

    // Raising to 30 is below current 100: max(100, min(30, 50)) = 100.
    let result = store.raise_min_prepare_time(30);
    assert_eq!(result, 100);
}
