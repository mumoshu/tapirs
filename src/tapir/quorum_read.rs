use super::{Key, Timestamp, TransactionError, Value, IR};

/// Merge quorum read results from `invoke_inconsistent_with_result`.
///
/// Returns `Ok` with the highest-timestamp value when at least `f_plus_one`
/// responses carry `QuorumRead` data. Returns `Err(OutOfRange)` when all
/// responses are `OutOfRange`, or `Err(PrepareConflict)` when fewer than
/// `f_plus_one` responses carry data due to prepared-but-uncommitted writes.
/// Returns `Err(Unavailable)` when no responses were received (e.g. deadline).
pub(crate) fn merge_quorum_read_results<K: Key, V: Value>(
    results: Vec<IR<K, V>>,
    f_plus_one: usize,
) -> Result<(Option<V>, Timestamp), TransactionError> {
    let mut best: Option<(Option<V>, Timestamp)> = None;
    let mut ok_count = 0usize;
    let mut all_out_of_range = !results.is_empty();
    let mut has_prepare_conflict = false;

    for ir in results {
        match ir {
            IR::QuorumRead(value, ts) => {
                ok_count += 1;
                all_out_of_range = false;
                if best.as_ref().is_none_or(|(_, best_ts)| ts > *best_ts) {
                    best = Some((value, ts));
                }
            }
            IR::OutOfRange => {}
            IR::PrepareConflict => {
                has_prepare_conflict = true;
                all_out_of_range = false;
            }
            _ => {
                all_out_of_range = false;
            }
        }
    }

    if all_out_of_range {
        return Err(TransactionError::OutOfRange);
    }
    if ok_count >= f_plus_one {
        return Ok(best.unwrap_or((None, Timestamp::default())));
    }
    if has_prepare_conflict {
        return Err(TransactionError::PrepareConflict);
    }
    Err(TransactionError::Unavailable)
}

/// Merge quorum scan results from `invoke_inconsistent_with_result`.
///
/// Returns `Ok` with merged entries (highest `write_ts` per key) when at
/// least `f_plus_one` responses carry `QuorumScan` data. Error semantics
/// match [`merge_quorum_read_results`].
pub(crate) fn merge_quorum_scan_results<K: Key, V: Value>(
    results: Vec<IR<K, V>>,
    f_plus_one: usize,
) -> Result<Vec<(K, Option<V>, Timestamp)>, TransactionError> {
    let mut merged = std::collections::BTreeMap::<K, (Option<V>, Timestamp)>::new();
    let mut ok_count = 0usize;
    let mut all_out_of_range = !results.is_empty();
    let mut has_prepare_conflict = false;

    for ir in results {
        match ir {
            IR::QuorumScan(entries) => {
                ok_count += 1;
                all_out_of_range = false;
                for (key, value, write_ts) in entries {
                    merged
                        .entry(key)
                        .and_modify(|(v, ts)| {
                            if write_ts > *ts {
                                *v = value.clone();
                                *ts = write_ts;
                            }
                        })
                        .or_insert((value, write_ts));
                }
            }
            IR::OutOfRange => {}
            IR::PrepareConflict => {
                has_prepare_conflict = true;
                all_out_of_range = false;
            }
            _ => {
                all_out_of_range = false;
            }
        }
    }

    if all_out_of_range {
        return Err(TransactionError::OutOfRange);
    }
    if ok_count >= f_plus_one {
        return Ok(merged
            .into_iter()
            .map(|(k, (v, ts))| (k, v, ts))
            .collect());
    }
    if has_prepare_conflict {
        return Err(TransactionError::PrepareConflict);
    }
    Err(TransactionError::Unavailable)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ts(time: u64) -> Timestamp {
        Timestamp { time, client_id: crate::IrClientId(0) }
    }

    // --- merge_quorum_read_results tests ---

    #[test]
    fn quorum_read_all_success() {
        let results: Vec<IR<i64, i64>> = vec![
            IR::QuorumRead(Some(1), ts(5)),
            IR::QuorumRead(Some(1), ts(3)),
        ];
        let r = merge_quorum_read_results(results, 2);
        assert!(r.is_ok());
        assert_eq!(r.unwrap(), (Some(1), ts(5)));
    }

    #[test]
    fn quorum_read_tolerates_prepare_conflict_with_enough_data() {
        // 3 responses (all 2f+1 arrived), 2 data + 1 PrepareConflict.
        // f+1=2, so 2 >= 2 → success.
        let results: Vec<IR<i64, i64>> = vec![
            IR::QuorumRead(Some(1), ts(5)),
            IR::QuorumRead(Some(1), ts(3)),
            IR::PrepareConflict,
        ];
        let r = merge_quorum_read_results(results, 2);
        assert!(r.is_ok());
        assert_eq!(r.unwrap(), (Some(1), ts(5)));
    }

    #[test]
    fn quorum_read_not_enough_data_returns_prepare_conflict() {
        // 2 responses, 1 data + 1 PrepareConflict. f+1=2, so 1 < 2 → error.
        let results: Vec<IR<i64, i64>> = vec![
            IR::QuorumRead(Some(1), ts(5)),
            IR::PrepareConflict,
        ];
        let r = merge_quorum_read_results(results, 2);
        assert!(matches!(r, Err(TransactionError::PrepareConflict)));
    }

    #[test]
    fn quorum_read_all_prepare_conflict() {
        let results: Vec<IR<i64, i64>> = vec![
            IR::PrepareConflict,
            IR::PrepareConflict,
        ];
        let r = merge_quorum_read_results(results, 2);
        assert!(matches!(r, Err(TransactionError::PrepareConflict)));
    }

    #[test]
    fn quorum_read_all_out_of_range() {
        let results: Vec<IR<i64, i64>> = vec![IR::OutOfRange, IR::OutOfRange];
        let r = merge_quorum_read_results(results, 2);
        assert!(matches!(r, Err(TransactionError::OutOfRange)));
    }

    #[test]
    fn quorum_read_mixed_out_of_range_and_prepare_conflict() {
        let results: Vec<IR<i64, i64>> = vec![IR::OutOfRange, IR::PrepareConflict];
        let r = merge_quorum_read_results(results, 2);
        assert!(matches!(r, Err(TransactionError::PrepareConflict)));
    }

    #[test]
    fn quorum_read_5_replicas_tolerates_2_conflicts() {
        // 5 responses, 3 data + 2 PrepareConflict. f+1=3 → success.
        let results: Vec<IR<i64, i64>> = vec![
            IR::QuorumRead(Some(10), ts(5)),
            IR::QuorumRead(Some(10), ts(7)),
            IR::QuorumRead(Some(10), ts(3)),
            IR::PrepareConflict,
            IR::PrepareConflict,
        ];
        let r = merge_quorum_read_results(results, 3);
        assert!(r.is_ok());
        assert_eq!(r.unwrap(), (Some(10), ts(7)));
    }

    #[test]
    fn quorum_read_5_replicas_not_enough_data() {
        // 5 responses, 2 data + 3 PrepareConflict. f+1=3, so 2 < 3 → error.
        let results: Vec<IR<i64, i64>> = vec![
            IR::QuorumRead(Some(10), ts(5)),
            IR::QuorumRead(Some(10), ts(3)),
            IR::PrepareConflict,
            IR::PrepareConflict,
            IR::PrepareConflict,
        ];
        let r = merge_quorum_read_results(results, 3);
        assert!(matches!(r, Err(TransactionError::PrepareConflict)));
    }

    // --- merge_quorum_scan_results tests ---

    #[test]
    fn quorum_scan_all_success() {
        let results: Vec<IR<i64, i64>> = vec![
            IR::QuorumScan(vec![(1, Some(10), ts(5)), (2, Some(20), ts(3))]),
            IR::QuorumScan(vec![(1, Some(10), ts(3)), (2, Some(20), ts(5))]),
        ];
        let r = merge_quorum_scan_results(results, 2);
        assert!(r.is_ok());
        let entries = r.unwrap();
        assert_eq!(entries, vec![(1, Some(10), ts(5)), (2, Some(20), ts(5))]);
    }

    #[test]
    fn quorum_scan_tolerates_prepare_conflict_with_enough_data() {
        let results: Vec<IR<i64, i64>> = vec![
            IR::QuorumScan(vec![(1, Some(10), ts(5))]),
            IR::QuorumScan(vec![(1, Some(10), ts(3))]),
            IR::PrepareConflict,
        ];
        let r = merge_quorum_scan_results(results, 2);
        assert!(r.is_ok());
        assert_eq!(r.unwrap(), vec![(1, Some(10), ts(5))]);
    }

    #[test]
    fn quorum_scan_not_enough_data_returns_prepare_conflict() {
        let results: Vec<IR<i64, i64>> = vec![
            IR::QuorumScan(vec![(1, Some(10), ts(5))]),
            IR::PrepareConflict,
        ];
        let r = merge_quorum_scan_results(results, 2);
        assert!(matches!(r, Err(TransactionError::PrepareConflict)));
    }

    #[test]
    fn quorum_scan_all_prepare_conflict() {
        let results: Vec<IR<i64, i64>> = vec![
            IR::PrepareConflict,
            IR::PrepareConflict,
        ];
        let r = merge_quorum_scan_results(results, 2);
        assert!(matches!(r, Err(TransactionError::PrepareConflict)));
    }

    #[test]
    fn quorum_scan_all_out_of_range() {
        let results: Vec<IR<i64, i64>> = vec![IR::OutOfRange, IR::OutOfRange];
        let r = merge_quorum_scan_results(results, 2);
        assert!(matches!(r, Err(TransactionError::OutOfRange)));
    }

    #[test]
    fn quorum_read_empty_results_is_unavailable() {
        let results: Vec<IR<i64, i64>> = vec![];
        let r = merge_quorum_read_results(results, 2);
        assert!(matches!(r, Err(TransactionError::Unavailable)));
    }

    #[test]
    fn quorum_scan_empty_results_is_unavailable() {
        let results: Vec<IR<i64, i64>> = vec![];
        let r = merge_quorum_scan_results(results, 2);
        assert!(matches!(r, Err(TransactionError::Unavailable)));
    }
}
