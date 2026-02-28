use super::*;
use crate::tapirstore::{CheckPrepareStatus, TapirStore};

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

#[test]
fn check_prepare_status_all_variants() {
    let (_dir, mut store) = new_store();

    let commit = ts(5, 1);

    // Unknown: no txn_log entry, no prepare.
    assert_eq!(
        store.check_prepare_status(&txn_id(1, 1), &commit),
        CheckPrepareStatus::Unknown,
    );

    // PreparedAtTimestamp (not finalized): prepare the transaction.
    let txn = make_txn(vec![], vec![("a", Some("v1"))], vec![]);
    store.prepare(txn_id(1, 1), txn.clone(), commit, false);
    assert_eq!(
        store.check_prepare_status(&txn_id(1, 1), &commit),
        CheckPrepareStatus::PreparedAtTimestamp { finalized: false },
    );

    // PreparedAtTimestamp (finalized): mark as finalized.
    store.set_prepared_finalized(&txn_id(1, 1), &commit);
    assert_eq!(
        store.check_prepare_status(&txn_id(1, 1), &commit),
        CheckPrepareStatus::PreparedAtTimestamp { finalized: true },
    );

    // CommittedAtTimestamp: commit the transaction.
    store.commit_and_log(txn_id(1, 1), &txn, commit);
    assert_eq!(
        store.check_prepare_status(&txn_id(1, 1), &commit),
        CheckPrepareStatus::CommittedAtTimestamp,
    );

    // CommittedDifferent: check with a different timestamp.
    assert_eq!(
        store.check_prepare_status(&txn_id(1, 1), &ts(99, 1)),
        CheckPrepareStatus::CommittedDifferent { proposed: 5 },
    );

    // Aborted: record an abort in the txn log.
    store.txn_log_insert(txn_id(2, 1), Default::default(), false);
    assert_eq!(
        store.check_prepare_status(&txn_id(2, 1), &commit),
        CheckPrepareStatus::Aborted,
    );

    // TooLate: set min_prepare_time above the commit time.
    store.set_min_prepare_time(100);
    assert_eq!(
        store.check_prepare_status(&txn_id(3, 1), &commit),
        CheckPrepareStatus::TooLate,
    );

    // TooLate via prepared_get: prepare at a low timestamp, then raise min_prepare_time.
    let txn4 = make_txn(vec![], vec![("b", Some("v2"))], vec![]);
    store.set_min_prepare_time(0); // reset so prepare succeeds
    store.prepare(txn_id(4, 1), txn4, ts(3, 1), false);
    store.set_min_prepare_time(10); // now the prepare's commit ts (3) < min_prepare_time (10)
    // Checking with a different timestamp than prepared, so prepared_at_timestamp won't match,
    // but prepared_get finds it and its commit time (3) < min_prepare_time (10).
    assert_eq!(
        store.check_prepare_status(&txn_id(4, 1), &ts(20, 1)),
        CheckPrepareStatus::TooLate,
    );
}
