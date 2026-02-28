use super::*;
use crate::tapirstore::TapirStore;

#[test]
fn get_validated_returns_none_before_quorum_read() {
    let (_dir, mut store) = new_store();

    store.occ_mut().put("x".into(), Some("v1".into()), ts(1, 1));

    // No quorum_read has been done yet, so get_validated should return None.
    assert!(store.get_validated(&"x".to_string(), ts(5, 1)).is_none());
}

#[test]
fn get_validated_returns_some_after_quorum_read() {
    let (_dir, mut store) = new_store();

    store.occ_mut().put("x".into(), Some("v1".into()), ts(1, 1));

    // Quorum read at ts(5,1) sets read_ts.
    store.quorum_read("x".into(), ts(5, 1)).unwrap();

    // Now get_validated at same ts should return the value.
    let result = store.get_validated(&"x".to_string(), ts(5, 1));
    assert!(result.is_some());
    let (val, write_ts) = result.unwrap();
    assert_eq!(val, Some("v1".to_string()));
    assert_eq!(write_ts, ts(1, 1));
}

#[test]
fn scan_validated_returns_none_before_quorum_scan() {
    let (_dir, mut store) = new_store();

    store.occ_mut().put("a".into(), Some("v1".into()), ts(1, 1));
    store.occ_mut().put("b".into(), Some("v2".into()), ts(1, 1));

    assert!(store
        .scan_validated(&"a".to_string(), &"b".to_string(), ts(5, 1))
        .is_none());
}

#[test]
fn scan_validated_returns_some_after_quorum_scan() {
    let (_dir, mut store) = new_store();

    store.occ_mut().put("a".into(), Some("v1".into()), ts(1, 1));
    store.occ_mut().put("b".into(), Some("v2".into()), ts(1, 1));

    // Quorum scan covers [a, b] at ts(5,1).
    store
        .quorum_scan("a".into(), "b".into(), ts(5, 1))
        .unwrap();

    // scan_validated at same range + ts should return results.
    let result = store.scan_validated(&"a".to_string(), &"b".to_string(), ts(5, 1));
    assert!(result.is_some());
    let entries = result.unwrap();
    assert_eq!(entries.len(), 2);
}
