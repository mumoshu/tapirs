use super::*;
use crate::tapirstore::TapirStore;

#[test]
fn quorum_read_returns_committed_value() {
    let (_dir, mut store) = new_store();

    // Write initial value.
    store.occ_mut().put("x".into(), Some("v1".into()), ts(1, 1));

    // Prepare + commit a write.
    let txn = make_txn(
        vec![("x", ts(1, 1))],
        vec![("x", Some("v2"))],
        vec![],
    );
    store.prepare(txn_id(1, 1), txn.clone(), ts(5, 1), false);
    store.commit(txn_id(1, 1), &txn, ts(5, 1));

    // Quorum read should return v2 at snapshot_ts >= 5.
    let (val, write_ts) = store.quorum_read("x".into(), ts(10, 1)).unwrap();
    assert_eq!(val, Some("v2".to_string()));
    assert_eq!(write_ts, ts(5, 1));
}

#[test]
fn quorum_scan_returns_range() {
    let (_dir, mut store) = new_store();

    store.occ_mut().put("a".into(), Some("v1".into()), ts(1, 1));
    store.occ_mut().put("b".into(), Some("v2".into()), ts(1, 1));
    store.occ_mut().put("c".into(), Some("v3".into()), ts(1, 1));

    let results = store
        .quorum_scan("a".into(), "c".into(), ts(10, 1))
        .unwrap();
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].0, "a");
    assert_eq!(results[1].0, "b");
    assert_eq!(results[2].0, "c");
}

#[test]
fn quorum_read_conflicts_with_prepared_write() {
    let (_dir, mut store) = new_store();

    store.occ_mut().put("x".into(), Some("v1".into()), ts(1, 1));

    // Prepare a write at ts(5,1).
    let txn = make_txn(vec![], vec![("x", Some("v2"))], vec![]);
    store.prepare(txn_id(1, 1), txn, ts(5, 1), false);

    // Quorum read at snapshot_ts >= 5 should conflict.
    assert!(store.quorum_read("x".into(), ts(10, 1)).is_err());
}
