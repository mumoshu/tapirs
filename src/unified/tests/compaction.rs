use super::helpers::*;
use crate::tapirstore::TapirStore;

// === Test 8: Multi-view data integrity (MVCC SST compaction precursor) ===
//
// SST flush from UnifiedMemtable is not yet implemented (seal path has a TODO).
// This test exercises the multi-view OnDisk resolution path that SST compaction
// would eventually replace: commit data across several sealed views, then verify
// all values are still readable through inherent methods.

#[test]
fn multi_view_data_integrity() {
    // Use small segment threshold so each seal creates a sealed segment
    let mut store = new_test_store_with_min_vlog_size(64);

    assert_current_view(&store, 0);
    assert_sealed_segment_count(&store, 0);

    // Fresh store: only active segment 0
    assert_store_file_names(&store, &["vlog_seg_0000.dat"]);
    assert_store_file_size(&store, "vlog_seg_0000.dat", 0);

    // View 0: Write key "a"
    prepare_and_commit(
        &mut store,
        test_op_id(0, 1),
        test_op_id(0, 2),
        test_txn_id(0, 1),
        vec![("a", Some("v1"))],
        test_ts(1),
    );
    assert_value_location_in_memory(&store, "a", test_ts(1), true);
    seal_view(&mut store);
    assert_current_view(&store, 1);
    assert_value_location_in_memory(&store, "a", test_ts(1), false);

    // After first seal with 64-byte threshold: segment rotated
    // (even small entry > 64 bytes after serialization)
    let files_after_seal1 = list_store_files(&store);
    assert!(
        files_after_seal1.len() >= 2,
        "Should have manifest + at least 1 VLog file after seal, got: {files_after_seal1:?}"
    );
    assert_store_file_size_positive(&store, "UNIFIED_MANIFEST");

    // View 1: Write key "b", update key "a"
    prepare_and_commit(
        &mut store,
        test_op_id(0, 3),
        test_op_id(0, 4),
        test_txn_id(0, 2),
        vec![("b", Some("v2")), ("a", Some("v1-updated"))],
        test_ts(2),
    );
    assert_value_location_in_memory(&store, "b", test_ts(2), true);
    assert_value_location_in_memory(&store, "a", test_ts(2), true);
    seal_view(&mut store);
    assert_current_view(&store, 2);
    assert_value_location_in_memory(&store, "b", test_ts(2), false);
    assert_value_location_in_memory(&store, "a", test_ts(2), false);

    // View 2: Write key "c"
    prepare_and_commit(
        &mut store,
        test_op_id(0, 5),
        test_op_id(0, 6),
        test_txn_id(0, 3),
        vec![("c", Some("v3"))],
        test_ts(3),
    );
    seal_view(&mut store);
    assert_current_view(&store, 3);

    // All sealed segments should exist (64-byte threshold is tiny)
    let seg_count = store.sealed_vlog_segments().len();
    assert!(
        seg_count >= 1,
        "Expected at least 1 sealed segment, got {seg_count}"
    );

    // Reads across all sealed views should work via OnDisk resolution
    // Verify BOTH value AND timestamp for every read
    assert_get_at(&store, "a", test_ts(1), Some("v1"), test_ts(1));
    assert_get_at(&store, "a", test_ts(2), Some("v1-updated"), test_ts(2));
    assert_get_at(&store, "b", test_ts(2), Some("v2"), test_ts(2));
    assert_get_at(&store, "c", test_ts(3), Some("v3"), test_ts(3));

    // All entries should be OnDisk (sealed)
    assert_value_location_in_memory(&store, "a", test_ts(1), false);
    assert_value_location_in_memory(&store, "a", test_ts(2), false);
    assert_value_location_in_memory(&store, "b", test_ts(2), false);
    assert_value_location_in_memory(&store, "c", test_ts(3), false);

    // Key "b" should not exist at ts=1 (before it was written)
    assert_get_none(&store, "b", test_ts(1));

    // Key "c" should not exist at ts=1 or ts=2
    assert_get_none(&store, "c", test_ts(1));
    assert_get_none(&store, "c", test_ts(2));

    // No last_read_ts for any key
    assert_last_read_ts(&store, "a", None);
    assert_last_read_ts(&store, "b", None);
    assert_last_read_ts(&store, "c", None);

    // do_uncommitted_get() returns latest version of "a" (v1-updated at ts=2)
    let (val, ts) = store.do_uncommitted_get(&"a".to_string()).unwrap();
    assert_eq!(
        val.as_deref(),
        Some("v1-updated"),
        "get(a) latest: value mismatch"
    );
    assert_eq!(ts, test_ts(2), "get(a) latest: timestamp mismatch");

    // Scan returns all 3 unique keys at ts=3
    let results =
        store.do_uncommitted_scan(&"a".to_string(), &"z".to_string(), test_ts(3)).unwrap();
    assert_eq!(results.len(), 3, "scan should return 3 entries at ts=3");
    // Verify each result's key, value, AND timestamp
    assert_eq!(results[0].0, "a");
    assert_eq!(results[0].1.as_deref(), Some("v1-updated"));
    assert_eq!(results[0].2, test_ts(2));
    assert_eq!(results[1].0, "b");
    assert_eq!(results[1].1.as_deref(), Some("v2"));
    assert_eq!(results[1].2, test_ts(2));
    assert_eq!(results[2].0, "c");
    assert_eq!(results[2].1.as_deref(), Some("v3"));
    assert_eq!(results[2].2, test_ts(3));

    // After 3 seals with 64-byte threshold, multiple segment files should exist.
    // manifest + sealed segments + active segment
    let final_files = list_store_files(&store);
    assert!(
        final_files.len() >= 3,
        "Should have manifest + multiple VLog segments after 3 seals, got: {final_files:?}"
    );
    assert_store_file_size_positive(&store, "UNIFIED_MANIFEST");
    // All VLog segment files should be non-empty (except possibly the latest active segment)
    for (name, size) in &final_files {
        if name.starts_with("vlog_seg_") && name != final_files.last().unwrap().0.as_str() {
            assert!(*size > 0, "Sealed VLog segment {name} should be non-empty");
        }
    }
}
