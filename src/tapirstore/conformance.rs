//! TapirStore conformance test suite.
//!
//! Generic test functions that exercise only TapirStore trait methods.
//! Each implementation invokes the `tapir_store_conformance_tests!` macro
//! to generate `#[test]` wrappers from a factory function.

use crate::ir::OpId;
use crate::occ::{PrepareResult, ScanEntry, SharedTransaction, Transaction, TransactionId};
use crate::tapir::{LeaderRecordDelta, ShardNumber, Sharded, Timestamp};
use crate::tapirstore::{CheckPrepareStatus, TapirStore};
use crate::IrClientId;

const DUMMY_OP_ID: OpId = OpId { client_id: IrClientId(0), number: 0 };
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

pub(crate) fn ts(time: u64, client_id: u64) -> Timestamp {
    Timestamp {
        time,
        client_id: IrClientId(client_id),
    }
}

pub(crate) fn txn_id(client: u64, num: u64) -> TransactionId {
    TransactionId {
        client_id: IrClientId(client),
        number: num,
    }
}

pub(crate) fn sharded(key: &str) -> Sharded<String> {
    Sharded {
        shard: ShardNumber(0),
        key: key.to_string(),
    }
}

pub(crate) fn make_txn(
    reads: Vec<(&str, Timestamp)>,
    writes: Vec<(&str, Option<&str>)>,
    scans: Vec<ScanEntry<String, Timestamp>>,
) -> SharedTransaction<String, String, Timestamp> {
    let mut txn = Transaction::<String, String, Timestamp>::default();
    for (key, timestamp) in reads {
        txn.add_read(sharded(key), timestamp);
    }
    for (key, value) in writes {
        txn.add_write(sharded(key), value.map(|v| v.to_string()));
    }
    txn.scan_set = scans;
    Arc::new(txn)
}

static SEED_COUNTER: AtomicU64 = AtomicU64::new(1);
const SEED_CLIENT_ID: u64 = 999;

/// Seed a key-value pair into the store using only trait methods.
/// Uses a dedicated client_id (999) to avoid txn_id collisions with test bodies.
pub(crate) fn seed_value(
    store: &mut impl TapirStore<String, String>,
    key: &str,
    value: &str,
    at: Timestamp,
) {
    let num = SEED_COUNTER.fetch_add(1, Ordering::Relaxed);
    let id = txn_id(SEED_CLIENT_ID, num);
    let txn = make_txn(vec![], vec![(key, Some(value))], vec![]);
    let result = store.try_prepare_txn(DUMMY_OP_ID,id, txn.clone(), at);
    assert_eq!(
        result,
        PrepareResult::Ok,
        "seed_value prepare failed for {key}"
    );
    store.commit_txn(DUMMY_OP_ID,id, &txn, at);
}

// ---------------------------------------------------------------------------
// Transaction Log
// ---------------------------------------------------------------------------

pub(crate) fn test_txn_log_insert_get_contains_len(
    store: &mut impl TapirStore<String, String>,
) {
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

pub(crate) fn test_txn_log_insert_returns_previous_on_duplicate(
    store: &mut impl TapirStore<String, String>,
) {
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

// ---------------------------------------------------------------------------
// CDC Deltas
// ---------------------------------------------------------------------------

fn make_delta(from: u64, to: u64) -> LeaderRecordDelta<String, String> {
    LeaderRecordDelta {
        from_view: from,
        to_view: to,
        changes: vec![],
    }
}

pub(crate) fn test_record_and_query_cdc_deltas(
    store: &mut impl TapirStore<String, String>,
) {
    assert!(store.cdc_max_view().is_none());
    assert!(store.cdc_deltas_from(0).is_empty());

    store.record_cdc_delta(0, make_delta(0, 1));
    store.record_cdc_delta(1, make_delta(1, 2));
    store.record_cdc_delta(3, make_delta(3, 4));

    assert_eq!(store.cdc_max_view(), Some(3));

    let all = store.cdc_deltas_from(0);
    assert_eq!(all.len(), 3);
    assert_eq!(all[0].from_view, 0);
    assert_eq!(all[1].from_view, 1);
    assert_eq!(all[2].from_view, 3);
}

pub(crate) fn test_cdc_deltas_from_filters_by_view(
    store: &mut impl TapirStore<String, String>,
) {
    store.record_cdc_delta(0, make_delta(0, 1));
    store.record_cdc_delta(1, make_delta(1, 2));
    store.record_cdc_delta(3, make_delta(3, 4));

    // from_view=2 should skip view 0 and 1, return only view 3.
    let deltas = store.cdc_deltas_from(2);
    assert_eq!(deltas.len(), 1);
    assert_eq!(deltas[0].from_view, 3);

    // from_view=4 should return nothing.
    assert!(store.cdc_deltas_from(4).is_empty());
}

// ---------------------------------------------------------------------------
// Prepare / Abort
// ---------------------------------------------------------------------------

pub(crate) fn test_prepare_abort_removes_from_prepared(
    store: &mut impl TapirStore<String, String>,
) {
    seed_value(store, "x", "v1", ts(1, 1));

    let txn = make_txn(vec![], vec![("x", Some("v2"))], vec![]);
    let result = store.try_prepare_txn(DUMMY_OP_ID,txn_id(1, 1), txn, ts(5, 1));
    assert_eq!(result, PrepareResult::Ok);
    assert_eq!(store.prepared_count(), 1);

    // Abort (remove_prepared_txn).
    let removed = store.remove_prepared_txn(txn_id(1, 1));
    assert!(removed);
    assert_eq!(store.prepared_count(), 0);
}

pub(crate) fn test_remove_prepared_returns_false_if_not_found(
    store: &mut impl TapirStore<String, String>,
) {
    assert!(!store.remove_prepared_txn(txn_id(99, 99)));
}

pub(crate) fn test_abort_does_not_affect_txn_log(
    store: &mut impl TapirStore<String, String>,
) {
    let txn = make_txn(vec![], vec![("x", Some("v2"))], vec![]);
    store.try_prepare_txn(DUMMY_OP_ID,txn_id(1, 1), txn, ts(5, 1));

    // Record abort in txn_log.
    store.txn_log_insert(txn_id(1, 1), Timestamp::default(), false);
    store.remove_prepared_txn(txn_id(1, 1));

    let (_, committed) = store.txn_log_get(&txn_id(1, 1)).unwrap();
    assert!(!committed);
}

// ---------------------------------------------------------------------------
// Prepare / Commit
// ---------------------------------------------------------------------------

pub(crate) fn test_prepare_commit_read(
    store: &mut impl TapirStore<String, String>,
) {
    // Write initial value via seed_value (trait-only).
    seed_value(store, "x", "v1", ts(1, 1));

    // Prepare a write transaction.
    let txn = make_txn(vec![("x", ts(1, 1))], vec![("x", Some("v2"))], vec![]);
    let result = store.try_prepare_txn(DUMMY_OP_ID,txn_id(1, 1), txn.clone(), ts(5, 1));
    assert_eq!(result, PrepareResult::Ok);

    // Commit via trait method.
    store.commit_txn(DUMMY_OP_ID,txn_id(1, 1), &txn, ts(5, 1));

    // MVCC read should return committed value.
    let (val, write_ts) = store.do_uncommitted_get_at(&"x".to_string(), ts(10, 1)).unwrap();
    assert_eq!(val, Some("v2".to_string()));
    assert_eq!(write_ts, ts(5, 1));
}

pub(crate) fn test_commit_txn_writes_both(
    store: &mut impl TapirStore<String, String>,
) {
    seed_value(store, "x", "v1", ts(1, 1));

    let txn = make_txn(vec![("x", ts(1, 1))], vec![("x", Some("v2"))], vec![]);
    store.try_prepare_txn(DUMMY_OP_ID,txn_id(1, 1), txn.clone(), ts(5, 1));

    // commit_txn should record in txn_log and apply writes.
    store.commit_txn(DUMMY_OP_ID,txn_id(1, 1), &txn, ts(5, 1));

    // txn_log should have the entry.
    let (log_ts, committed) = store.txn_log_get(&txn_id(1, 1)).unwrap();
    assert_eq!(log_ts, ts(5, 1));
    assert!(committed);

    // MVCC read should return committed value.
    let (val, write_ts) = store.do_uncommitted_get_at(&"x".to_string(), ts(10, 1)).unwrap();
    assert_eq!(val, Some("v2".to_string()));
    assert_eq!(write_ts, ts(5, 1));

    // Idempotent: calling again should not panic.
    store.commit_txn(DUMMY_OP_ID,txn_id(1, 1), &txn, ts(5, 1));
}

// ---------------------------------------------------------------------------
// Prepared Queries
// ---------------------------------------------------------------------------

pub(crate) fn test_prepared_get(store: &mut impl TapirStore<String, String>) {
    let txn = make_txn(vec![], vec![("x", Some("v1"))], vec![]);
    store.try_prepare_txn(DUMMY_OP_ID,txn_id(1, 1), txn, ts(5, 1));

    // prepared_get returns the entry.
    let entry = store.get_prepared_txn(&txn_id(1, 1));
    assert!(entry.is_some());
    let (commit_ts, _txn, finalized) = entry.unwrap();
    assert_eq!(commit_ts, ts(5, 1));
    assert!(!finalized);

    // Non-existent txn returns None.
    assert!(store.get_prepared_txn(&txn_id(99, 99)).is_none());
}

pub(crate) fn test_finalize_prepared_txn(
    store: &mut impl TapirStore<String, String>,
) {
    let txn = make_txn(vec![], vec![("x", Some("v1"))], vec![]);
    store.try_prepare_txn(DUMMY_OP_ID,txn_id(1, 1), txn, ts(5, 1));

    // Mark as finalized.
    assert!(store.finalize_prepared_txn(&txn_id(1, 1), &ts(5, 1)));

    // Verify it's finalized.
    let entry = store.get_prepared_txn(&txn_id(1, 1)).unwrap();
    assert!(entry.2); // finalized flag

    // Wrong timestamp returns false.
    assert!(!store.finalize_prepared_txn(&txn_id(1, 1), &ts(99, 1)));
}

pub(crate) fn test_oldest_prepared_returns_min_timestamp(
    store: &mut impl TapirStore<String, String>,
) {
    assert!(store.get_oldest_prepared_txn().is_none());

    let txn1 = make_txn(vec![], vec![("a", Some("v1"))], vec![]);
    let txn2 = make_txn(vec![], vec![("b", Some("v2"))], vec![]);
    store.try_prepare_txn(DUMMY_OP_ID,txn_id(1, 1), txn1, ts(20, 1));
    store.try_prepare_txn(DUMMY_OP_ID,txn_id(2, 1), txn2, ts(10, 1));

    let (id, commit_ts, _txn) = store.get_oldest_prepared_txn().unwrap();
    assert_eq!(id, txn_id(2, 1));
    assert_eq!(commit_ts, ts(10, 1));
}

pub(crate) fn test_remove_unfinalized_prepared(
    store: &mut impl TapirStore<String, String>,
) {
    let txn1 = make_txn(vec![], vec![("a", Some("v1"))], vec![]);
    let txn2 = make_txn(vec![], vec![("b", Some("v2"))], vec![]);
    store.try_prepare_txn(DUMMY_OP_ID,txn_id(1, 1), txn1, ts(5, 1));
    store.try_prepare_txn(DUMMY_OP_ID,txn_id(2, 1), txn2, ts(10, 1));

    // Finalize only txn 1.
    store.finalize_prepared_txn(&txn_id(1, 1), &ts(5, 1));

    assert_eq!(store.prepared_count(), 2);

    // Remove unfinalized — should remove txn 2 but keep txn 1.
    store.remove_all_unfinalized_prepared_txns();

    assert_eq!(store.prepared_count(), 1);
    assert!(store.get_prepared_txn(&txn_id(1, 1)).is_some());
    assert!(store.get_prepared_txn(&txn_id(2, 1)).is_none());
}

// ---------------------------------------------------------------------------
// Check Prepare Status (split from check_prepare_status_all_variants)
// ---------------------------------------------------------------------------

pub(crate) fn test_check_prepare_status_unknown(
    store: &mut impl TapirStore<String, String>,
) {
    assert_eq!(
        store.check_prepare_status(&txn_id(1, 1), &ts(5, 1)),
        CheckPrepareStatus::Unknown,
    );
}

pub(crate) fn test_check_prepare_status_prepared(
    store: &mut impl TapirStore<String, String>,
) {
    let commit = ts(5, 1);
    let txn = make_txn(vec![], vec![("a", Some("v1"))], vec![]);
    store.try_prepare_txn(DUMMY_OP_ID,txn_id(1, 1), txn, commit);

    // Not finalized yet.
    assert_eq!(
        store.check_prepare_status(&txn_id(1, 1), &commit),
        CheckPrepareStatus::PreparedAtTimestamp { finalized: false },
    );

    // Mark as finalized.
    store.finalize_prepared_txn(&txn_id(1, 1), &commit);
    assert_eq!(
        store.check_prepare_status(&txn_id(1, 1), &commit),
        CheckPrepareStatus::PreparedAtTimestamp { finalized: true },
    );
}

pub(crate) fn test_check_prepare_status_committed(
    store: &mut impl TapirStore<String, String>,
) {
    let commit = ts(5, 1);
    let txn = make_txn(vec![], vec![("a", Some("v1"))], vec![]);
    store.try_prepare_txn(DUMMY_OP_ID,txn_id(1, 1), txn.clone(), commit);
    store.commit_txn(DUMMY_OP_ID,txn_id(1, 1), &txn, commit);

    // Same timestamp → CommittedAtTimestamp.
    assert_eq!(
        store.check_prepare_status(&txn_id(1, 1), &commit),
        CheckPrepareStatus::CommittedAtTimestamp,
    );

    // Different timestamp → CommittedDifferent.
    assert_eq!(
        store.check_prepare_status(&txn_id(1, 1), &ts(99, 1)),
        CheckPrepareStatus::CommittedDifferent { proposed: 5 },
    );
}

pub(crate) fn test_check_prepare_status_aborted(
    store: &mut impl TapirStore<String, String>,
) {
    store.txn_log_insert(txn_id(2, 1), Default::default(), false);
    assert_eq!(
        store.check_prepare_status(&txn_id(2, 1), &ts(5, 1)),
        CheckPrepareStatus::Aborted,
    );
}

pub(crate) fn test_check_prepare_status_too_late(
    store: &mut impl TapirStore<String, String>,
) {
    // Use finalize_min_prepare_time to set mpt to 100.
    store.finalize_min_prepare_time(100);

    // ts(5,1): 5 < 100 → TooLate.
    assert_eq!(
        store.check_prepare_status(&txn_id(3, 1), &ts(5, 1)),
        CheckPrepareStatus::TooLate,
    );

    // ts(100,1): 100 < 100 is false → Unknown (no entry exists).
    assert_eq!(
        store.check_prepare_status(&txn_id(3, 1), &ts(100, 1)),
        CheckPrepareStatus::Unknown,
    );
}

pub(crate) fn test_check_prepare_status_too_late_via_prepared(
    store: &mut impl TapirStore<String, String>,
) {
    // Prepare at ts(3,1).
    let txn = make_txn(vec![], vec![("b", Some("v2"))], vec![]);
    store.try_prepare_txn(DUMMY_OP_ID,txn_id(4, 1), txn, ts(3, 1));

    // Raise min_prepare_time above the prepared commit time.
    // finalize_min_prepare_time bypasses the min_prepared_ts cap.
    store.finalize_min_prepare_time(10);

    // Check with a different timestamp than prepared (ts(20,1) != ts(3,1)),
    // so prepared_at_timestamp won't match, but prepared_get finds it
    // and its commit time (3) < min_prepare_time (10).
    assert_eq!(
        store.check_prepare_status(&txn_id(4, 1), &ts(20, 1)),
        CheckPrepareStatus::TooLate,
    );
}

// ---------------------------------------------------------------------------
// Quorum Read / Scan
// ---------------------------------------------------------------------------

pub(crate) fn test_quorum_read_returns_committed_value(
    store: &mut impl TapirStore<String, String>,
) {
    // Write initial value.
    seed_value(store, "x", "v1", ts(1, 1));

    // Prepare + commit a write via trait method.
    let txn = make_txn(vec![("x", ts(1, 1))], vec![("x", Some("v2"))], vec![]);
    store.try_prepare_txn(DUMMY_OP_ID,txn_id(1, 1), txn.clone(), ts(5, 1));
    store.commit_txn(DUMMY_OP_ID,txn_id(1, 1), &txn, ts(5, 1));

    // Quorum read should return v2 at snapshot_ts >= 5.
    let (val, write_ts) = store.do_committed_get("x".into(), ts(10, 1)).unwrap();
    assert_eq!(val, Some("v2".to_string()));
    assert_eq!(write_ts, ts(5, 1));
}

pub(crate) fn test_quorum_scan_returns_range(
    store: &mut impl TapirStore<String, String>,
) {
    seed_value(store, "a", "v1", ts(1, 1));
    seed_value(store, "b", "v2", ts(1, 1));
    seed_value(store, "c", "v3", ts(1, 1));

    let results = store
        .do_committed_scan("a".into(), "c".into(), ts(10, 1))
        .unwrap();
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].0, "a");
    assert_eq!(results[1].0, "b");
    assert_eq!(results[2].0, "c");
}

pub(crate) fn test_quorum_read_conflicts_with_prepared_write(
    store: &mut impl TapirStore<String, String>,
) {
    seed_value(store, "x", "v1", ts(1, 1));

    // Prepare a write at ts(5,1).
    let txn = make_txn(vec![], vec![("x", Some("v2"))], vec![]);
    store.try_prepare_txn(DUMMY_OP_ID,txn_id(1, 1), txn, ts(5, 1));

    // Quorum read at snapshot_ts >= 5 should conflict.
    assert!(store.do_committed_get("x".into(), ts(10, 1)).is_err());
}

// ---------------------------------------------------------------------------
// Fast Path Validation
// ---------------------------------------------------------------------------

pub(crate) fn test_get_validated_returns_none_before_quorum_read(
    store: &mut impl TapirStore<String, String>,
) {
    seed_value(store, "x", "v1", ts(1, 1));

    // No quorum_read has been done yet, so get_validated should return None.
    assert!(store.do_uncommitted_get_validated(&"x".to_string(), ts(5, 1)).is_none());
}

pub(crate) fn test_get_validated_returns_some_after_quorum_read(
    store: &mut impl TapirStore<String, String>,
) {
    seed_value(store, "x", "v1", ts(1, 1));

    // Quorum read at ts(5,1) sets read_ts.
    store.do_committed_get("x".into(), ts(5, 1)).unwrap();

    // Now get_validated at same ts should return the value.
    let result = store.do_uncommitted_get_validated(&"x".to_string(), ts(5, 1));
    assert!(result.is_some());
    let (val, write_ts) = result.unwrap();
    assert_eq!(val, Some("v1".to_string()));
    assert_eq!(write_ts, ts(1, 1));
}

pub(crate) fn test_scan_validated_returns_none_before_quorum_scan(
    store: &mut impl TapirStore<String, String>,
) {
    seed_value(store, "a", "v1", ts(1, 1));
    seed_value(store, "b", "v2", ts(1, 1));

    assert!(store
        .do_uncommitted_scan_validated(&"a".to_string(), &"b".to_string(), ts(5, 1))
        .is_none());
}

pub(crate) fn test_scan_validated_returns_some_after_quorum_scan(
    store: &mut impl TapirStore<String, String>,
) {
    seed_value(store, "a", "v1", ts(1, 1));
    seed_value(store, "b", "v2", ts(1, 1));

    // Quorum scan covers [a, b] at ts(5,1).
    store
        .do_committed_scan("a".into(), "b".into(), ts(5, 1))
        .unwrap();

    // scan_validated at same range + ts should return results.
    let result = store.do_uncommitted_scan_validated(&"a".to_string(), &"b".to_string(), ts(5, 1));
    assert!(result.is_some());
    let entries = result.unwrap();
    assert_eq!(entries.len(), 2);
}

// ---------------------------------------------------------------------------
// Resharding
// ---------------------------------------------------------------------------

pub(crate) fn test_min_prepare_baseline_fresh_store(
    store: &mut impl TapirStore<String, String>,
) {
    assert!(store.min_prepare_baseline().is_none());
}

pub(crate) fn test_min_prepare_baseline_after_quorum_read(
    store: &mut impl TapirStore<String, String>,
) {
    seed_value(store, "x", "v1", ts(1, 1));

    // Quorum read updates max_read_time.
    store.do_committed_get("x".into(), ts(5, 1)).unwrap();

    assert_eq!(store.min_prepare_baseline(), Some(ts(5, 1)));
}

pub(crate) fn test_min_prepare_baseline_after_quorum_scan(
    store: &mut impl TapirStore<String, String>,
) {
    seed_value(store, "a", "v1", ts(1, 1));
    seed_value(store, "b", "v2", ts(1, 1));

    // Quorum scan updates max_read_time.
    store
        .do_committed_scan("a".into(), "b".into(), ts(7, 1))
        .unwrap();

    assert_eq!(store.min_prepare_baseline(), Some(ts(7, 1)));
}

// ---------------------------------------------------------------------------
// Min Prepare Time
// ---------------------------------------------------------------------------

pub(crate) fn test_oldest_prepared_is_min_prepared_timestamp(
    store: &mut impl TapirStore<String, String>,
) {
    assert!(store.get_oldest_prepared_txn().is_none());

    // Prepare two transactions at different timestamps.
    let txn1 = make_txn(vec![], vec![("a", Some("v1"))], vec![]);
    let txn2 = make_txn(vec![], vec![("b", Some("v2"))], vec![]);
    store.try_prepare_txn(DUMMY_OP_ID,txn_id(1, 1), txn1, ts(20, 1));
    store.try_prepare_txn(DUMMY_OP_ID,txn_id(2, 1), txn2, ts(10, 1));

    let (_, oldest_ts, _) = store.get_oldest_prepared_txn().unwrap();
    assert_eq!(oldest_ts.time, 10);
}

pub(crate) fn test_raise_min_prepare_time_caps_at_min_prepared(
    store: &mut impl TapirStore<String, String>,
) {
    // With no prepared transactions, min_prepared_ts = u64::MAX,
    // so raise should set to max(0, min(100, MAX)) = 100.
    let result = store.raise_min_prepare_time(100);
    assert_eq!(result, 100);
    assert_eq!(store.raise_min_prepare_time(0), 100); // observe tentative

    // Prepare a transaction at time=50.
    let txn = make_txn(vec![], vec![("a", Some("v1"))], vec![]);
    store.try_prepare_txn(DUMMY_OP_ID,txn_id(1, 1), txn, ts(50, 1));

    // Raising to 200 should cap at min_prepared_ts=50: max(100, min(200, 50)) = 100.
    let result = store.raise_min_prepare_time(200);
    assert_eq!(result, 100);

    // Raising to 30 is below current 100: max(100, min(30, 50)) = 100.
    let result = store.raise_min_prepare_time(30);
    assert_eq!(result, 100);
}

pub(crate) fn test_finalize_min_prepare_time_raises_both(
    store: &mut impl TapirStore<String, String>,
) {
    // Set tentative to 50 via raise.
    store.raise_min_prepare_time(50);

    // Finalize at 100: finalized becomes 100, tentative raised to max(50, 100) = 100.
    store.finalize_min_prepare_time(100);
    assert_eq!(store.raise_min_prepare_time(0), 100); // observe tentative

    // Finalize at 80 (below current): finalized stays 100, tentative stays 100.
    store.finalize_min_prepare_time(80);
    assert_eq!(store.raise_min_prepare_time(0), 100);

    // Raise tentative higher.
    store.raise_min_prepare_time(200);
    assert_eq!(store.raise_min_prepare_time(0), 200);

    // Finalize at 150: finalized=150, tentative stays at max(200, 150) = 200.
    store.finalize_min_prepare_time(150);
    assert_eq!(store.raise_min_prepare_time(0), 200); // tentative stays at 200

    // Verify finalized=150 (destructive — last assertion).
    store.reset_min_prepare_time_to_finalized();
    assert_eq!(store.raise_min_prepare_time(0), 150);
}

pub(crate) fn test_sync_min_prepare_time_can_rollback_tentative(
    store: &mut impl TapirStore<String, String>,
) {
    // Set tentative higher than finalized (speculative raise).
    // raise(100) → tentative=100, finalize(30) → finalized=30, tentative stays 100.
    store.raise_min_prepare_time(100);
    store.finalize_min_prepare_time(30);

    // Sync at 50: finalized becomes max(30, 50) = 50,
    // tentative becomes min(100, 50) = 50 (rolled back).
    store.sync_min_prepare_time(50);
    assert_eq!(store.raise_min_prepare_time(0), 50); // tentative=50

    // Sync at 40 (below current finalized): finalized stays 50,
    // tentative stays min(50, 50) = 50.
    store.sync_min_prepare_time(40);
    assert_eq!(store.raise_min_prepare_time(0), 50);

    // Verify finalized=50 (destructive — last assertion).
    store.reset_min_prepare_time_to_finalized();
    assert_eq!(store.raise_min_prepare_time(0), 50);
}

pub(crate) fn test_reset_min_prepare_time_to_finalized_resets_tentative(
    store: &mut impl TapirStore<String, String>,
) {
    // Build state: finalized=42, tentative=100.
    store.finalize_min_prepare_time(42);
    store.raise_min_prepare_time(100);

    store.reset_min_prepare_time_to_finalized();
    assert_eq!(store.raise_min_prepare_time(0), 42); // tentative = finalized = 42
}

// ---------------------------------------------------------------------------
// Macro
// ---------------------------------------------------------------------------

/// Generate `#[test]` wrappers for all conformance tests.
///
/// Usage: `tapir_store_conformance_tests!(new_store());`
///
/// The expression must evaluate to `(Guard, Store)` where `Guard` is any RAII
/// type (e.g., `TempDir`) that stays alive for the test duration and `Store`
/// implements `TapirStore<String, String>`.
#[macro_export]
#[cfg(test)]
macro_rules! tapir_store_conformance_tests {
    ($new_store:expr) => {
        // Transaction log
        #[test]
        fn txn_log_insert_get_contains_len() {
            let (_g, mut s) = $new_store;
            $crate::tapirstore::conformance::test_txn_log_insert_get_contains_len(&mut s);
        }
        #[test]
        fn txn_log_insert_returns_previous_on_duplicate() {
            let (_g, mut s) = $new_store;
            $crate::tapirstore::conformance::test_txn_log_insert_returns_previous_on_duplicate(
                &mut s,
            );
        }

        // CDC deltas
        #[test]
        fn record_and_query_cdc_deltas() {
            let (_g, mut s) = $new_store;
            $crate::tapirstore::conformance::test_record_and_query_cdc_deltas(&mut s);
        }
        #[test]
        fn cdc_deltas_from_filters_by_view() {
            let (_g, mut s) = $new_store;
            $crate::tapirstore::conformance::test_cdc_deltas_from_filters_by_view(&mut s);
        }

        // Prepare / abort
        #[test]
        fn prepare_abort_removes_from_prepared() {
            let (_g, mut s) = $new_store;
            $crate::tapirstore::conformance::test_prepare_abort_removes_from_prepared(&mut s);
        }
        #[test]
        fn remove_prepared_returns_false_if_not_found() {
            let (_g, mut s) = $new_store;
            $crate::tapirstore::conformance::test_remove_prepared_returns_false_if_not_found(
                &mut s,
            );
        }
        #[test]
        fn abort_does_not_affect_txn_log() {
            let (_g, mut s) = $new_store;
            $crate::tapirstore::conformance::test_abort_does_not_affect_txn_log(&mut s);
        }

        // Prepare / commit
        #[test]
        fn prepare_commit_read() {
            let (_g, mut s) = $new_store;
            $crate::tapirstore::conformance::test_prepare_commit_read(&mut s);
        }
        #[test]
        fn commit_txn_writes_both() {
            let (_g, mut s) = $new_store;
            $crate::tapirstore::conformance::test_commit_txn_writes_both(&mut s);
        }

        // Prepared queries
        #[test]
        fn prepared_get() {
            let (_g, mut s) = $new_store;
            $crate::tapirstore::conformance::test_prepared_get(&mut s);
        }
        #[test]
        fn finalize_prepared_txn() {
            let (_g, mut s) = $new_store;
            $crate::tapirstore::conformance::test_finalize_prepared_txn(&mut s);
        }
        #[test]
        fn oldest_prepared_returns_min_timestamp() {
            let (_g, mut s) = $new_store;
            $crate::tapirstore::conformance::test_oldest_prepared_returns_min_timestamp(&mut s);
        }
        #[test]
        fn remove_unfinalized_prepared() {
            let (_g, mut s) = $new_store;
            $crate::tapirstore::conformance::test_remove_unfinalized_prepared(&mut s);
        }

        // Check prepare status (split from check_prepare_status_all_variants)
        #[test]
        fn check_prepare_status_unknown() {
            let (_g, mut s) = $new_store;
            $crate::tapirstore::conformance::test_check_prepare_status_unknown(&mut s);
        }
        #[test]
        fn check_prepare_status_prepared() {
            let (_g, mut s) = $new_store;
            $crate::tapirstore::conformance::test_check_prepare_status_prepared(&mut s);
        }
        #[test]
        fn check_prepare_status_committed() {
            let (_g, mut s) = $new_store;
            $crate::tapirstore::conformance::test_check_prepare_status_committed(&mut s);
        }
        #[test]
        fn check_prepare_status_aborted() {
            let (_g, mut s) = $new_store;
            $crate::tapirstore::conformance::test_check_prepare_status_aborted(&mut s);
        }
        #[test]
        fn check_prepare_status_too_late() {
            let (_g, mut s) = $new_store;
            $crate::tapirstore::conformance::test_check_prepare_status_too_late(&mut s);
        }
        #[test]
        fn check_prepare_status_too_late_via_prepared() {
            let (_g, mut s) = $new_store;
            $crate::tapirstore::conformance::test_check_prepare_status_too_late_via_prepared(
                &mut s,
            );
        }

        // Quorum read / scan
        #[test]
        fn quorum_read_returns_committed_value() {
            let (_g, mut s) = $new_store;
            $crate::tapirstore::conformance::test_quorum_read_returns_committed_value(&mut s);
        }
        #[test]
        fn quorum_scan_returns_range() {
            let (_g, mut s) = $new_store;
            $crate::tapirstore::conformance::test_quorum_scan_returns_range(&mut s);
        }
        #[test]
        fn quorum_read_conflicts_with_prepared_write() {
            let (_g, mut s) = $new_store;
            $crate::tapirstore::conformance::test_quorum_read_conflicts_with_prepared_write(
                &mut s,
            );
        }

        // Fast path validation
        #[test]
        fn get_validated_returns_none_before_quorum_read() {
            let (_g, mut s) = $new_store;
            $crate::tapirstore::conformance::test_get_validated_returns_none_before_quorum_read(
                &mut s,
            );
        }
        #[test]
        fn get_validated_returns_some_after_quorum_read() {
            let (_g, mut s) = $new_store;
            $crate::tapirstore::conformance::test_get_validated_returns_some_after_quorum_read(
                &mut s,
            );
        }
        #[test]
        fn scan_validated_returns_none_before_quorum_scan() {
            let (_g, mut s) = $new_store;
            $crate::tapirstore::conformance::test_scan_validated_returns_none_before_quorum_scan(
                &mut s,
            );
        }
        #[test]
        fn scan_validated_returns_some_after_quorum_scan() {
            let (_g, mut s) = $new_store;
            $crate::tapirstore::conformance::test_scan_validated_returns_some_after_quorum_scan(
                &mut s,
            );
        }

        // Resharding
        #[test]
        fn min_prepare_baseline_fresh_store() {
            let (_g, mut s) = $new_store;
            $crate::tapirstore::conformance::test_min_prepare_baseline_fresh_store(&mut s);
        }
        #[test]
        fn min_prepare_baseline_after_quorum_read() {
            let (_g, mut s) = $new_store;
            $crate::tapirstore::conformance::test_min_prepare_baseline_after_quorum_read(&mut s);
        }
        #[test]
        fn min_prepare_baseline_after_quorum_scan() {
            let (_g, mut s) = $new_store;
            $crate::tapirstore::conformance::test_min_prepare_baseline_after_quorum_scan(&mut s);
        }

        // Min prepare time
        #[test]
        fn oldest_prepared_is_min_prepared_timestamp() {
            let (_g, mut s) = $new_store;
            $crate::tapirstore::conformance::test_oldest_prepared_is_min_prepared_timestamp(
                &mut s,
            );
        }
        #[test]
        fn raise_min_prepare_time_caps_at_min_prepared() {
            let (_g, mut s) = $new_store;
            $crate::tapirstore::conformance::test_raise_min_prepare_time_caps_at_min_prepared(
                &mut s,
            );
        }
        #[test]
        fn finalize_min_prepare_time_raises_both() {
            let (_g, mut s) = $new_store;
            $crate::tapirstore::conformance::test_finalize_min_prepare_time_raises_both(&mut s);
        }
        #[test]
        fn sync_min_prepare_time_can_rollback_tentative() {
            let (_g, mut s) = $new_store;
            $crate::tapirstore::conformance::test_sync_min_prepare_time_can_rollback_tentative(
                &mut s,
            );
        }
        #[test]
        fn reset_min_prepare_time_to_finalized_resets_tentative() {
            let (_g, mut s) = $new_store;
            $crate::tapirstore::conformance::test_reset_min_prepare_time_to_finalized_resets_tentative(
                &mut s,
            );
        }
    };
}
