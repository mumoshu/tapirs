use crate::mvcc::disk::memory_io::MemoryIo;

use super::helpers::*;

// === Test 6: Recovery — VLog + manifest survive store reopen ===

#[test]
fn recovery_vlog_and_manifest_survive_reopen() {
    // Use a fixed path so we can reopen the same store
    let path = MemoryIo::temp_path();

    // Phase 1: Create store, commit data, seal view
    let vlog_size_before_drop;
    let manifest_size_before_drop;
    {
        let mut store = TestStore::open(path.clone()).unwrap();

        assert_current_view(&store, 0);
        assert_sealed_segment_count(&store, 0);

        // Fresh store: only active VLog segment
        assert_store_file_names(&store, &["vlog_seg_0000.dat"]);
        assert_store_file_size(&store, "vlog_seg_0000.dat", 0);

        prepare_and_commit(
            &mut store,
            test_op_id(0, 1),
            test_op_id(0, 2),
            test_txn_id(0, 1),
            vec![("x", Some("v1")), ("y", Some("v2"))],
            test_ts(5),
        );

        // Verify data is readable before seal
        assert_get_at(&store, "x", test_ts(5), Some("v1"), test_ts(5));
        assert_get_at(&store, "y", test_ts(5), Some("v2"), test_ts(5));

        // Seal view 0 → view 1 (persists VLog + manifest)
        seal_view(&mut store);
        assert_current_view(&store, 1);

        // After first seal: manifest + VLog with data
        assert_store_file_names(&store, &["UNIFIED_MANIFEST", "vlog_seg_0000.dat"]);
        assert_store_file_size_positive(&store, "UNIFIED_MANIFEST");
        assert_store_file_size_positive(&store, "vlog_seg_0000.dat");

        // Data still readable after seal (OnDisk now)
        assert_get_at(&store, "x", test_ts(5), Some("v1"), test_ts(5));
        assert_value_location_in_memory(&store, "x", test_ts(5), false);

        // Commit in view 1 and seal again
        prepare_and_commit(
            &mut store,
            test_op_id(0, 3),
            test_op_id(0, 4),
            test_txn_id(0, 2),
            vec![("z", Some("v3"))],
            test_ts(10),
        );
        seal_view(&mut store);
        assert_current_view(&store, 2);

        // After second seal: still same files (small data < 256KB)
        assert_store_file_names(&store, &["UNIFIED_MANIFEST", "vlog_seg_0000.dat"]);
        vlog_size_before_drop = get_store_file_size(&store, "vlog_seg_0000.dat");
        manifest_size_before_drop = get_store_file_size(&store, "UNIFIED_MANIFEST");
    }
    // Store dropped here — simulates crash/shutdown

    // Phase 2: Reopen from the same path
    let store = TestStore::open(path).unwrap();

    // Files should be unchanged after reopen (MemoryIo persists across drop/reopen)
    assert_store_file_names(&store, &["UNIFIED_MANIFEST", "vlog_seg_0000.dat"]);
    assert_store_file_size(&store, "vlog_seg_0000.dat", vlog_size_before_drop);
    assert_store_file_size(&store, "UNIFIED_MANIFEST", manifest_size_before_drop);

    // Manifest should restore current view
    assert_current_view(&store, 2);

    // VLog read count resets on reopen
    assert_eq!(store.vlog_read_count(), 0, "vlog_read_count should be 0 after reopen");

    // Sealed segment count depends on whether data exceeded 256KB threshold
    // (with default 256KB threshold, small test data stays in active segment)
    let seg_count = store.sealed_vlog_segments().len();
    assert_eq!(seg_count, 0, "Small test data should stay in active segment");
}

#[test]
fn recovery_sealed_segments_persist() {
    let path = MemoryIo::temp_path();

    // Phase 1: Create store with small segment threshold to force segment rotation
    {
        let mut store = TestStore::open_with_options(path.clone(), 1024).unwrap();

        assert_current_view(&store, 0);
        assert_sealed_segment_count(&store, 0);

        // Fresh store: only active segment 0
        assert_store_file_names(&store, &["vlog_seg_0000.dat"]);

        // Write enough data to exceed 1KB threshold
        let big_value = "x".repeat(2048);
        prepare_and_commit(
            &mut store,
            test_op_id(0, 1),
            test_op_id(0, 2),
            test_txn_id(0, 1),
            vec![("big_key", Some(&big_value))],
            test_ts(5),
        );

        // Verify readable before seal
        assert_get_at(&store, "big_key", test_ts(5), Some(&big_value), test_ts(5));

        seal_view(&mut store);

        assert_sealed_segment_count(&store, 1);
        assert_current_view(&store, 1);

        // After seal with segment rotation: sealed segment 0 + new active segment 1 + manifest
        assert_store_file_names(
            &store,
            &["UNIFIED_MANIFEST", "vlog_seg_0000.dat", "vlog_seg_0001.dat"],
        );
        assert_store_file_size_positive(&store, "UNIFIED_MANIFEST");
        let sealed_seg_size = get_store_file_size(&store, "vlog_seg_0000.dat");
        assert!(
            sealed_seg_size > 1024,
            "Sealed segment should exceed 1KB: got {sealed_seg_size}"
        );
        assert_store_file_size(&store, "vlog_seg_0001.dat", 0);
    }

    // Phase 2: Reopen and verify ALL sealed segment metadata
    let store = TestStore::open_with_options(path, 1024).unwrap();

    // Files should be unchanged after reopen
    assert_store_file_names(
        &store,
        &["UNIFIED_MANIFEST", "vlog_seg_0000.dat", "vlog_seg_0001.dat"],
    );
    assert_store_file_size_positive(&store, "vlog_seg_0000.dat");

    assert_current_view(&store, 1);
    assert_sealed_segment_count(&store, 1);
    assert_eq!(store.vlog_read_count(), 0, "vlog_read_count should be 0");

    // Verify sealed segment metadata
    let sealed_segs = store.sealed_vlog_segments();
    let (&seg_id, seg) = sealed_segs.iter().next().expect("Should have 1 sealed segment");
    assert_eq!(seg_id, seg.id, "Segment ID in map should match segment's own ID");
    assert!(!seg.views.is_empty(), "Sealed segment should have view ranges");
    assert_eq!(seg.views[0].view, 0, "First view range should be view 0");
    assert_eq!(seg.views[0].start_offset, 0, "View 0 should start at offset 0");
    assert!(
        seg.views[0].end_offset > 0,
        "View 0 end offset should be > 0 (data was written)"
    );
    assert!(
        seg.views[0].num_entries > 0,
        "View 0 should have at least 1 entry"
    );
    assert!(
        seg.write_offset() > 0,
        "Sealed segment should have non-zero write offset"
    );
}
