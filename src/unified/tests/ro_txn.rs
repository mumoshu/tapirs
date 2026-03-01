use super::helpers::*;
use crate::tapirstore::TapirStore;

// === Test 2: RO Transaction (QuorumRead + QuorumScan) ===

#[test]
fn ro_txn_quorum_read_scan() {
    let mut store = new_test_store();

    // Fresh store: only active VLog segment (empty)
    assert_store_file_names(&store, &["vlog_seg_0000.dat"]);
    assert_store_file_size(&store, "vlog_seg_0000.dat", 0);

    // Setup committed data: two keys
    prepare_and_commit(
        &mut store,
        test_op_id(0, 1),
        test_op_id(0, 2),
        test_txn_id(0, 1),
        vec![("x", Some("v1")), ("y", Some("v2"))],
        test_ts(5),
    );

    // Verify both values and timestamps via get_at
    assert_get_at(&store, "x", test_ts(5), Some("v1"), test_ts(5));
    assert_get_at(&store, "y", test_ts(5), Some("v2"), test_ts(5));

    // Initially no last_read_ts for either key
    assert_last_read_ts(&store, "x", None);
    assert_last_read_ts(&store, "y", None);

    // QuorumRead sets last_read_ts via commit_get
    store.commit_get("x".to_string(), test_ts(5), test_ts(10)).unwrap();
    assert_last_read_ts(&store, "x", Some(10));
    // y untouched
    assert_last_read_ts(&store, "y", None);

    // get_last_read_at should also return the timestamp for the specific version
    let lr_at = store.get_last_read_at(&"x".to_string(), test_ts(5)).unwrap();
    assert_eq!(
        lr_at.map(|ts| ts.time),
        Some(10),
        "get_last_read_at should match commit_get timestamp"
    );

    // QuorumScan returns correct values — verify ALL fields of each result
    let results =
        store.do_uncommitted_scan(&"a".to_string(), &"z".to_string(), test_ts(10)).unwrap();
    assert_eq!(results.len(), 2, "scan should return 2 entries");

    // Results should be sorted by key
    assert_eq!(results[0].0, "x", "First scan result key should be 'x'");
    assert_eq!(
        results[0].1.as_deref(),
        Some("v1"),
        "First scan result value"
    );
    assert_eq!(results[0].2, test_ts(5), "First scan result timestamp");

    assert_eq!(results[1].0, "y", "Second scan result key should be 'y'");
    assert_eq!(
        results[1].1.as_deref(),
        Some("v2"),
        "Second scan result value"
    );
    assert_eq!(results[1].2, test_ts(5), "Second scan result timestamp");

    // Scan with narrower range should only return matching keys
    let narrow =
        store.do_uncommitted_scan(&"x".to_string(), &"x".to_string(), test_ts(10)).unwrap();
    assert_eq!(narrow.len(), 1, "narrow scan should return 1 entry");
    assert_eq!(narrow[0].0, "x");
    assert_eq!(narrow[0].1.as_deref(), Some("v1"));
    assert_eq!(narrow[0].2, test_ts(5));

    // has_writes_in_range should detect writes
    let has_writes = store.has_writes_in_range(
        &"a".to_string(),
        &"z".to_string(),
        test_ts(0),
        test_ts(10),
    )
    .unwrap();
    assert!(has_writes, "Expected writes in range (0, 10)");

    // No writes in range after ts=5
    let no_writes = store.has_writes_in_range(
        &"a".to_string(),
        &"z".to_string(),
        test_ts(5),
        test_ts(10),
    )
    .unwrap();
    assert!(!no_writes, "Expected no writes in range (5, 10)");

    // Scan before any writes should return empty
    let empty_scan =
        store.do_uncommitted_scan(&"a".to_string(), &"z".to_string(), test_ts(1)).unwrap();
    assert!(
        empty_scan.is_empty(),
        "Scan before commit_ts should return empty"
    );

    // No seal performed, files unchanged: only active VLog (still empty on disk)
    assert_store_file_names(&store, &["vlog_seg_0000.dat"]);
    assert_store_file_size(&store, "vlog_seg_0000.dat", 0);
}
