use super::*;
use crate::tapirstore::TapirStore;

#[test]
fn min_prepare_baseline_fresh_store() {
    let (_dir, store) = new_store();

    let (max_rr, max_rc) = store.min_prepare_baseline();
    assert!(max_rr.is_none());
    assert!(max_rc.is_none());
}

#[test]
fn min_prepare_baseline_after_quorum_read() {
    let (_dir, mut store) = new_store();

    store.occ_mut().put("x".into(), Some("v1".into()), ts(1, 1));

    // Quorum read updates max_read_commit_time.
    store.quorum_read("x".into(), ts(5, 1)).unwrap();

    let (max_rr, max_rc) = store.min_prepare_baseline();
    // No range reads yet.
    assert!(max_rr.is_none());
    // max_read_commit_time should be ts(5,1).
    assert_eq!(max_rc, Some(ts(5, 1)));
}

#[test]
fn min_prepare_baseline_after_quorum_scan() {
    let (_dir, mut store) = new_store();

    store.occ_mut().put("a".into(), Some("v1".into()), ts(1, 1));
    store.occ_mut().put("b".into(), Some("v2".into()), ts(1, 1));

    // Quorum scan records range read.
    store
        .quorum_scan("a".into(), "b".into(), ts(7, 1))
        .unwrap();

    let (max_rr, _max_rc) = store.min_prepare_baseline();
    assert_eq!(max_rr, Some(ts(7, 1)));
}
