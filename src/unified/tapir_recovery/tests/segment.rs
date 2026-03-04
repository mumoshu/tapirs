use super::helpers::*;
use crate::mvcc::disk::memory_io::MemoryIo;

// === Test 7: VLog Segment Grouping ===

#[test]
fn vlog_segment_grouping() {
    // Use 1KB threshold for segment rotation
    let path = MemoryIo::temp_path();
    let mut store = TestStore::open_with_options(path.clone(), 1024).unwrap();

    assert_eq!(store.get_metrics().current_view, 0);

    // Fresh store: only active segment 0 (empty)
    assert_store_file_names(&path, &["ir_vlog_0000.dat"]);
    assert_store_file_size(&path, "ir_vlog_0000.dat", 0);

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
    assert_eq!(store.get_metrics().current_view, 1);

    // After first seal: manifest + VLog with data, still same segment (< 1KB)
    assert_store_file_names(&path, &["UNIFIED_MANIFEST", "ir_vlog_0000.dat"]);
    assert_store_file_size_positive(&path, "ir_vlog_0000.dat");
    let vlog_size_after_seal1 = get_store_file_size(&path, "ir_vlog_0000.dat");

    prepare_and_commit(
        &mut store,
        test_op_id(0, 3),
        test_op_id(0, 4),
        test_txn_id(0, 2),
        vec![("b", Some("v2"))],
        test_ts(2),
    );
    seal_view(&mut store);
    assert_eq!(store.get_metrics().current_view, 2);

    // Views 0 and 1 should share same segment (both < 1KB, but
    // segment is sealed once cumulative size >= 1KB, which hasn't happened yet).
    // Small entries: each ~50-100 bytes, two seals ~200 bytes total < 1KB.
    assert_store_file_names(&path, &["UNIFIED_MANIFEST", "ir_vlog_0000.dat"]);

    // After second seal: VLog grew but still same segment (no rotation)
    assert_store_file_names(&path, &["UNIFIED_MANIFEST", "ir_vlog_0000.dat"]);
    let vlog_size_after_seal2 = get_store_file_size(&path, "ir_vlog_0000.dat");
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
    assert_eq!(store.get_metrics().current_view, 3);

    // Now the segment exceeded 1KB and was sealed → new active segment created
    assert_store_file_names(
        &path,
        &["UNIFIED_MANIFEST", "ir_vlog_0000.dat", "ir_vlog_0001.dat"],
    );

    // After segment rotation: old segment sealed, new active segment created
    assert_store_file_names(
        &path,
        &["UNIFIED_MANIFEST", "ir_vlog_0000.dat", "ir_vlog_0001.dat"],
    );
    // Sealed segment (0000) should have significant data (> 1KB with big_value)
    let sealed_size = get_store_file_size(&path, "ir_vlog_0000.dat");
    assert!(
        sealed_size > 1024,
        "Sealed segment should exceed 1KB threshold: got {sealed_size}"
    );
    // New active segment (0001) is empty (no writes in view 3 yet)
    assert_store_file_size(&path, "ir_vlog_0001.dat", 0);

    // Reads across segments work correctly — verify all values AND timestamps
    let (actual_value, actual_ts) = store.do_uncommitted_get_at(&"a".to_string(), test_ts(1)).unwrap();
    assert_eq!(actual_value.as_deref(), Some("v1"));
    assert_eq!(actual_ts, test_ts(1));
    let (actual_value, actual_ts) = store.do_uncommitted_get_at(&"b".to_string(), test_ts(2)).unwrap();
    assert_eq!(actual_value.as_deref(), Some("v2"));
    assert_eq!(actual_ts, test_ts(2));
    let (actual_value, actual_ts) = store.do_uncommitted_get_at(&"c".to_string(), test_ts(3)).unwrap();
    assert_eq!(actual_value.as_deref(), Some(big_value.as_str()));
    assert_eq!(actual_ts, test_ts(3));

    // Nonexistent keys should return None
    let (actual_value, _) = store
        .do_uncommitted_get_at(&"d".to_string(), test_ts(100))
        .unwrap();
    assert!(actual_value.is_none());
    let (actual_value, _) = store.do_uncommitted_get_at(&"a".to_string(), test_ts(0)).unwrap();
    assert!(actual_value.is_none());
}
