use super::helpers::*;

// === Test 7: VLog Segment Grouping ===

#[test]
fn vlog_segment_grouping() {
    // Use 1KB threshold for segment rotation
    let mut store = new_test_store_with_min_vlog_size(1024);

    assert_current_view(&store, 0);
    assert_sealed_segment_count(&store, 0);

    // Fresh store: only active segment 0 (empty)
    assert_store_file_names(&store, &["vlog_seg_0000.dat"]);
    assert_store_file_size(&store, "vlog_seg_0000.dat", 0);

    // Views 0-1: small transactions (< 1KB each)
    prepare_and_commit(
        &mut store,
        test_op_id(0, 1),
        test_op_id(0, 2),
        test_txn_id(0, 1),
        vec![("a", Some("v1"))],
        test_ts(1),
    );
    seal_view(&mut store);
    assert_current_view(&store, 1);

    // After first seal: manifest + VLog with data, still same segment (< 1KB)
    assert_store_file_names(&store, &["UNIFIED_MANIFEST", "vlog_seg_0000.dat"]);
    assert_store_file_size_positive(&store, "vlog_seg_0000.dat");
    let vlog_size_after_seal1 = get_store_file_size(&store, "vlog_seg_0000.dat");

    prepare_and_commit(
        &mut store,
        test_op_id(0, 3),
        test_op_id(0, 4),
        test_txn_id(0, 2),
        vec![("b", Some("v2"))],
        test_ts(2),
    );
    seal_view(&mut store);
    assert_current_view(&store, 2);

    // Views 0 and 1 should share same segment (both < 1KB, but
    // segment is sealed once cumulative size >= 1KB, which hasn't happened yet).
    // Small entries: each ~50-100 bytes, two seals ~200 bytes total < 1KB.
    assert_sealed_segment_count(&store, 0);

    // After second seal: VLog grew but still same segment (no rotation)
    assert_store_file_names(&store, &["UNIFIED_MANIFEST", "vlog_seg_0000.dat"]);
    let vlog_size_after_seal2 = get_store_file_size(&store, "vlog_seg_0000.dat");
    assert!(
        vlog_size_after_seal2 > vlog_size_after_seal1,
        "VLog should grow after second seal: {vlog_size_after_seal2} > {vlog_size_after_seal1}"
    );

    // View 2: large transaction (> 1KB)
    let big_value = "x".repeat(2048);
    prepare_and_commit(
        &mut store,
        test_op_id(0, 5),
        test_op_id(0, 6),
        test_txn_id(0, 3),
        vec![("c", Some(&big_value))],
        test_ts(3),
    );
    seal_view(&mut store);
    assert_current_view(&store, 3);

    // Now the segment exceeded 1KB and was sealed → new active segment created
    assert_sealed_segment_count(&store, 1);

    // After segment rotation: old segment sealed, new active segment created
    assert_store_file_names(
        &store,
        &["UNIFIED_MANIFEST", "vlog_seg_0000.dat", "vlog_seg_0001.dat"],
    );
    // Sealed segment (0000) should have significant data (> 1KB with big_value)
    let sealed_size = get_store_file_size(&store, "vlog_seg_0000.dat");
    assert!(
        sealed_size > 1024,
        "Sealed segment should exceed 1KB threshold: got {sealed_size}"
    );
    // New active segment (0001) is empty (no writes in view 3 yet)
    assert_store_file_size(&store, "vlog_seg_0001.dat", 0);

    // Reads across segments work correctly — verify all values AND timestamps
    assert_get_at(&store, "a", test_ts(1), Some("v1"), test_ts(1));
    assert_get_at(&store, "b", test_ts(2), Some("v2"), test_ts(2));
    assert_get_at(&store, "c", test_ts(3), Some(&big_value), test_ts(3));

    // All entries should be OnDisk (sealed)
    assert_value_location_in_memory(&store, "a", test_ts(1), false);
    assert_value_location_in_memory(&store, "b", test_ts(2), false);
    assert_value_location_in_memory(&store, "c", test_ts(3), false);

    // Nonexistent keys should return None
    assert_get_none(&store, "d", test_ts(100));
    assert_get_none(&store, "a", test_ts(0));
}
