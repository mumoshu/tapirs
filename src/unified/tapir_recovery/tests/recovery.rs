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

        assert_eq!(store.get_metrics().current_view, 0);

        // Fresh store: only active VLog segment
        assert_store_file_names(&path, &["ir_vlog_0000.dat"]);
        assert_store_file_size(&path, "ir_vlog_0000.dat", 0);

        prepare_and_commit(
            &mut store,
            test_op_id(0, 1),
            test_op_id(0, 2),
            test_txn_id(0, 1),
            vec![("x", Some("v1")), ("y", Some("v2"))],
            test_ts(5),
        );

        // Verify data is readable before seal
        let (actual_value, actual_ts) = store.do_uncommitted_get_at(&"x".to_string(), test_ts(5)).unwrap();
        assert_eq!(actual_value.as_deref(), Some("v1"));
        assert_eq!(actual_ts, test_ts(5));
        let (actual_value, actual_ts) = store.do_uncommitted_get_at(&"y".to_string(), test_ts(5)).unwrap();
        assert_eq!(actual_value.as_deref(), Some("v2"));
        assert_eq!(actual_ts, test_ts(5));

        // Seal view 0 → view 1 (persists VLog + manifest)
        seal_view(&mut store);
        assert_eq!(store.get_metrics().current_view, 1);

        // After first seal: manifest + VLog with data + SST
        assert_store_file_names(&path, &["UNIFIED_MANIFEST", "ir_sst_0000.db", "ir_vlog_0000.dat"]);
        assert_store_file_size_positive(&path, "UNIFIED_MANIFEST");
        assert_store_file_size_positive(&path, "ir_vlog_0000.dat");

        // Data still readable after seal (OnDisk now)
        let (actual_value, actual_ts) = store.do_uncommitted_get_at(&"x".to_string(), test_ts(5)).unwrap();
        assert_eq!(actual_value.as_deref(), Some("v1"));
        assert_eq!(actual_ts, test_ts(5));

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
        assert_eq!(store.get_metrics().current_view, 2);

        // After second seal: still same vlog files (small data < 256KB), plus second SST
        assert_store_file_names(&path, &["UNIFIED_MANIFEST", "ir_sst_0000.db", "ir_sst_0001.db", "ir_vlog_0000.dat"]);
        vlog_size_before_drop = get_store_file_size(&path, "ir_vlog_0000.dat");
        manifest_size_before_drop = get_store_file_size(&path, "UNIFIED_MANIFEST");
    }
    // Store dropped here — simulates crash/shutdown

    // Phase 2: Reopen from the same path
    let store = TestStore::open(path.clone()).unwrap();

    // Files should be unchanged after reopen (MemoryIo persists across drop/reopen)
    assert_store_file_names(&path, &["UNIFIED_MANIFEST", "ir_sst_0000.db", "ir_sst_0001.db", "ir_vlog_0000.dat"]);
    assert_store_file_size(&path, "ir_vlog_0000.dat", vlog_size_before_drop);
    assert_store_file_size(&path, "UNIFIED_MANIFEST", manifest_size_before_drop);

    // Manifest should restore current view
    assert_eq!(store.get_metrics().current_view, 2);

    // Small test data stays in active segment: no additional segment files.
    assert_store_file_names(&path, &["UNIFIED_MANIFEST", "ir_sst_0000.db", "ir_sst_0001.db", "ir_vlog_0000.dat"]);
}

#[test]
fn recovery_sealed_segments_persist() {
    let path = MemoryIo::temp_path();

    // Phase 1: Create store with small segment threshold to force segment rotation
    {
        let mut store = TestStore::open_with_options(path.clone(), 1024).unwrap();

        assert_eq!(store.get_metrics().current_view, 0);

        // Fresh store: only active segment 0
        assert_store_file_names(&path, &["ir_vlog_0000.dat"]);

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
        let (actual_value, actual_ts) = store
            .do_uncommitted_get_at(&"big_key".to_string(), test_ts(5))
            .unwrap();
        assert_eq!(actual_value.as_deref(), Some(big_value.as_str()));
        assert_eq!(actual_ts, test_ts(5));

        seal_view(&mut store);

        assert_eq!(store.get_metrics().current_view, 1);

        // After seal with segment rotation: sealed segment 0 + new active segment 1 + manifest + SST
        assert_store_file_names(
            &path,
            &["UNIFIED_MANIFEST", "ir_sst_0000.db", "ir_vlog_0000.dat", "ir_vlog_0001.dat"],
        );
        assert_store_file_size_positive(&path, "UNIFIED_MANIFEST");
        let sealed_seg_size = get_store_file_size(&path, "ir_vlog_0000.dat");
        assert!(
            sealed_seg_size > 1024,
            "Sealed segment should exceed 1KB: got {sealed_seg_size}"
        );
        assert_store_file_size(&path, "ir_vlog_0001.dat", 0);
    }

    // Phase 2: Reopen and verify ALL sealed segment metadata
    let store = TestStore::open_with_options(path.clone(), 1024).unwrap();

    // Files should be unchanged after reopen
    assert_store_file_names(
        &path,
        &["UNIFIED_MANIFEST", "ir_sst_0000.db", "ir_vlog_0000.dat", "ir_vlog_0001.dat"],
    );
    assert_store_file_size_positive(&path, "ir_vlog_0000.dat");

    assert_eq!(store.get_metrics().current_view, 1);
    // Sealed data remains readable after reopen.
    let (actual_value, actual_ts) = store
        .do_uncommitted_get_at(&"big_key".to_string(), test_ts(5))
        .unwrap();
    let expected_big_value = "x".repeat(2048);
    assert_eq!(actual_value.as_deref(), Some(expected_big_value.as_str()));
    assert_eq!(actual_ts, test_ts(5));
}
