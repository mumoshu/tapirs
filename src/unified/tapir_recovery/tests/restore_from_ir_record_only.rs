use crate::mvcc::disk::memory_io::MemoryIo;
use crate::unified::ir::record::{IrMemEntry, VlogEntryType};

use super::helpers::*;

/// Demonstrates how to take a backup of the IR record only and rebuild
/// everything including the MVCC memtable/SSTs.
///
/// Key insight: The IR record is the source of truth. CO::Prepare entries
/// carry the full write set (key_bytes, value_bytes), and IO::Commit
/// entries tell us which transactions were committed at what timestamp.
/// By replaying all committed transactions from the IR record, we can
/// fully rebuild the MVCC state.
///
/// This is the same process a replica would use during view change:
/// the leader sends the merged IR record, and the replica replays it
/// to rebuild its local MVCC state.
#[test]
fn restore_from_ir_record_rebuilds_mvcc() {
    // === Phase 1: Build up committed state ===

    let source_path = MemoryIo::temp_path();
    let mut source_store = TestStore::open(source_path.clone()).unwrap();

    // Fresh store: only active VLog segment
    assert_store_file_names(&source_path, &["ir_vlog_0000.dat"]);
    assert_store_file_size(&source_path, "ir_vlog_0000.dat", 0);

    // Txn 1: Write "x" = "v1" at ts=5
    prepare_and_commit(
        &mut source_store,
        test_op_id(0, 1),
        test_op_id(0, 2),
        test_txn_id(0, 1),
        vec![("x", Some("v1"))],
        test_ts(5),
    );

    // Txn 2: Write "y" = "v2", update "x" = "v1-updated" at ts=10
    prepare_and_commit(
        &mut source_store,
        test_op_id(0, 3),
        test_op_id(0, 4),
        test_txn_id(0, 2),
        vec![("y", Some("v2")), ("x", Some("v1-updated"))],
        test_ts(10),
    );

    // Txn 3: Write "z" = "v3" at ts=15 — but this one is prepared, NOT committed
    let uncommitted_txn = make_txn(vec![], vec![("z", Some("v3"))]);
    source_store.prepare(
        test_op_id(0, 5),
        test_txn_id(0, 3),
        uncommitted_txn,
        test_ts(15),
        true, // finalized prepare, but no commit follows
    );

    // Verify source state — values
    let (actual_value, actual_ts) = source_store.do_uncommitted_get_at(&"x".to_string(), test_ts(5)).unwrap();
    assert_eq!(actual_value.as_deref(), Some("v1"));
    assert_eq!(actual_ts, test_ts(5));
    let (actual_value, actual_ts) = source_store.do_uncommitted_get_at(&"x".to_string(), test_ts(10)).unwrap();
    assert_eq!(actual_value.as_deref(), Some("v1-updated"));
    assert_eq!(actual_ts, test_ts(10));
    let (actual_value, actual_ts) = source_store.do_uncommitted_get_at(&"y".to_string(), test_ts(10)).unwrap();
    assert_eq!(actual_value.as_deref(), Some("v2"));
    assert_eq!(actual_ts, test_ts(10));

    // === Phase 2: Extract the IR record (backup) ===
    //
    // In a real system, this would be the merged record from view change,
    // or a snapshot from backup. Here we extract all IR overlay entries.

    let ir_record: Vec<(crate::ir::OpId, IrMemEntry<String, String>)> = extract_ir_record(&source_store);

    // The IR record should contain:
    // - 2 CO::Prepare (finalized) for txn 1 and 2
    // - 2 IO::Commit for txn 1 and 2
    // - 1 CO::Prepare (finalized) for txn 3 (uncommitted — no matching Commit)
    let prepare_count = ir_record
        .iter()
        .filter(|(_, e)| e.entry_type == VlogEntryType::Prepare)
        .count();
    let commit_count = ir_record
        .iter()
        .filter(|(_, e)| e.entry_type == VlogEntryType::Commit)
        .count();
    assert_eq!(prepare_count, 3, "Expected 3 Prepare entries in IR record");
    assert_eq!(commit_count, 2, "Expected 2 Commit entries in IR record");

    // === Phase 3: Restore to a fresh store from IR record only ===

    let restored_path = MemoryIo::temp_path();
    let mut restored_store = TestStore::open(restored_path.clone()).unwrap();

    // Restored store starts fresh with just active VLog
    assert_store_file_names(&restored_path, &["ir_vlog_0000.dat"]);
    assert_store_file_size(&restored_path, "ir_vlog_0000.dat", 0);

    replay_committed_from_ir_record(&mut restored_store, &ir_record).unwrap();

    // === Phase 4: Verify restored MVCC state matches source ===

    // Committed values should be readable
    let (actual_value, actual_ts) = restored_store.do_uncommitted_get_at(&"x".to_string(), test_ts(5)).unwrap();
    assert_eq!(actual_value.as_deref(), Some("v1"));
    assert_eq!(actual_ts, test_ts(5));
    let (actual_value, actual_ts) = restored_store.do_uncommitted_get_at(&"x".to_string(), test_ts(10)).unwrap();
    assert_eq!(actual_value.as_deref(), Some("v1-updated"));
    assert_eq!(actual_ts, test_ts(10));
    let (actual_value, actual_ts) = restored_store.do_uncommitted_get_at(&"y".to_string(), test_ts(10)).unwrap();
    assert_eq!(actual_value.as_deref(), Some("v2"));
    assert_eq!(actual_ts, test_ts(10));

    // Uncommitted txn 3 ("z") should NOT be in the restored MVCC state
    // (it has a Prepare but no Commit in the IR record)
    let (actual_value, _) = restored_store.do_uncommitted_get_at(&"z".to_string(), test_ts(15)).unwrap();
    assert!(actual_value.is_none());
    let (actual_value, _) = restored_store.do_uncommitted_get_at(&"z".to_string(), test_ts(100)).unwrap();
    assert!(actual_value.is_none());

    // do_uncommitted_get() returns latest version
    let (val, ts) = restored_store.do_uncommitted_get(&"x".to_string()).unwrap();
    assert_eq!(val.as_deref(), Some("v1-updated"), "get(x): value mismatch");
    assert_eq!(ts, test_ts(10), "get(x): timestamp mismatch");

    let (val, ts) = restored_store.do_uncommitted_get(&"y".to_string()).unwrap();
    assert_eq!(val.as_deref(), Some("v2"), "get(y): value mismatch");
    assert_eq!(ts, test_ts(10), "get(y): timestamp mismatch");

    // Key not found returns None
    let (val, _) = restored_store.do_uncommitted_get(&"nonexistent".to_string()).unwrap();
    assert!(val.is_none(), "Nonexistent key should return None");

    // Multi-version reads work correctly
    let (actual_value, actual_ts) = restored_store.do_uncommitted_get_at(&"x".to_string(), test_ts(5)).unwrap();
    assert_eq!(actual_value.as_deref(), Some("v1"));
    assert_eq!(actual_ts, test_ts(5));
    let (actual_value, actual_ts) = restored_store.do_uncommitted_get_at(&"x".to_string(), test_ts(7)).unwrap();
    assert_eq!(actual_value.as_deref(), Some("v1"));
    assert_eq!(actual_ts, test_ts(5));
    let (actual_value, actual_ts) = restored_store.do_uncommitted_get_at(&"x".to_string(), test_ts(10)).unwrap();
    assert_eq!(actual_value.as_deref(), Some("v1-updated"));
    assert_eq!(actual_ts, test_ts(10));

    // Scan returns all committed keys at ts=10
    let scan_results = restored_store.do_uncommitted_scan(
        &"a".to_string(),
        &"z".to_string(),
        test_ts(10),
    )
    .unwrap();
    assert_eq!(scan_results.len(), 2, "scan should return 2 committed keys");
    assert_eq!(scan_results[0].0, "x");
    assert_eq!(scan_results[0].1.as_deref(), Some("v1-updated"));
    assert_eq!(scan_results[0].2, test_ts(10));
    assert_eq!(scan_results[1].0, "y");
    assert_eq!(scan_results[1].1.as_deref(), Some("v2"));
    assert_eq!(scan_results[1].2, test_ts(10));

    // Restored current-view values should remain readable.
    let (_value, _ts) = restored_store
        .do_uncommitted_get_at(&"x".to_string(), test_ts(10))
        .unwrap();

    // After restore (no seal): data is in memtable only, VLog is still empty.
    assert_store_file_names(&restored_path, &["ir_vlog_0000.dat"]);
}

/// Test that restore works after seal (IR entries are in VLog, not in memory).
/// This demonstrates backup from the sealed VLog rather than the in-memory overlay.
#[test]
fn restore_from_sealed_vlog_rebuilds_mvcc() {
    let path = MemoryIo::temp_path();

    // === Phase 1: Build state and seal ===
    let ir_record;
    {
        let mut store = TestStore::open(path.clone()).unwrap();

        prepare_and_commit(
            &mut store,
            test_op_id(0, 1),
            test_op_id(0, 2),
            test_txn_id(0, 1),
            vec![("a", Some("val_a")), ("b", Some("val_b"))],
            test_ts(5),
        );
        prepare_and_commit(
            &mut store,
            test_op_id(0, 3),
            test_op_id(0, 4),
            test_txn_id(0, 2),
            vec![("c", Some("val_c"))],
            test_ts(10),
        );

        // Verify before seal
        let (actual_value, actual_ts) = store.do_uncommitted_get_at(&"a".to_string(), test_ts(5)).unwrap();
        assert_eq!(actual_value.as_deref(), Some("val_a"));
        assert_eq!(actual_ts, test_ts(5));
        let (actual_value, actual_ts) = store.do_uncommitted_get_at(&"b".to_string(), test_ts(5)).unwrap();
        assert_eq!(actual_value.as_deref(), Some("val_b"));
        assert_eq!(actual_ts, test_ts(5));
        let (actual_value, actual_ts) = store.do_uncommitted_get_at(&"c".to_string(), test_ts(10)).unwrap();
        assert_eq!(actual_value.as_deref(), Some("val_c"));
        assert_eq!(actual_ts, test_ts(10));

        // Extract IR record BEFORE sealing (while entries are in overlay)
        ir_record = extract_ir_record(&store);

        seal_view(&mut store);
        assert_eq!(store.get_metrics().current_view, 1);

        // After seal: manifest + VLog with data
        assert_store_file_names(&path, &["UNIFIED_MANIFEST", "ir_vlog_0000.dat"]);
        assert_store_file_size_positive(&path, "UNIFIED_MANIFEST");
        assert_store_file_size_positive(&path, "ir_vlog_0000.dat");

        // Sealed values should remain readable.
        let (_value, _ts) = store.do_uncommitted_get_at(&"a".to_string(), test_ts(5)).unwrap();
    }

    // === Phase 2: Restore to completely fresh store from IR record ===

    let restored_path = MemoryIo::temp_path();
    let mut restored = TestStore::open(restored_path.clone()).unwrap();
    assert_store_file_names(&restored_path, &["ir_vlog_0000.dat"]);

    replay_committed_from_ir_record(&mut restored, &ir_record).unwrap();

    // === Phase 3: Verify ALL fields match ===

    // Values
    let (actual_value, actual_ts) = restored.do_uncommitted_get_at(&"a".to_string(), test_ts(5)).unwrap();
    assert_eq!(actual_value.as_deref(), Some("val_a"));
    assert_eq!(actual_ts, test_ts(5));
    let (actual_value, actual_ts) = restored.do_uncommitted_get_at(&"b".to_string(), test_ts(5)).unwrap();
    assert_eq!(actual_value.as_deref(), Some("val_b"));
    assert_eq!(actual_ts, test_ts(5));
    let (actual_value, actual_ts) = restored.do_uncommitted_get_at(&"c".to_string(), test_ts(10)).unwrap();
    assert_eq!(actual_value.as_deref(), Some("val_c"));
    assert_eq!(actual_ts, test_ts(10));

    // Nonexistent before write time
    let (actual_value, _) = restored.do_uncommitted_get_at(&"a".to_string(), test_ts(1)).unwrap();
    assert!(actual_value.is_none());
    let (actual_value, _) = restored.do_uncommitted_get_at(&"c".to_string(), test_ts(5)).unwrap();
    assert!(actual_value.is_none());

    // Freshly restored values should remain readable.
    let (_value, _ts) = restored.do_uncommitted_get_at(&"a".to_string(), test_ts(5)).unwrap();

    // Scan
    let scan = restored.do_uncommitted_scan(
        &"a".to_string(),
        &"z".to_string(),
        test_ts(10),
    )
    .unwrap();
    assert_eq!(scan.len(), 3, "scan should return all 3 keys");
    assert_eq!(scan[0].0, "a");
    assert_eq!(scan[0].1.as_deref(), Some("val_a"));
    assert_eq!(scan[0].2, test_ts(5));
    assert_eq!(scan[1].0, "b");
    assert_eq!(scan[1].1.as_deref(), Some("val_b"));
    assert_eq!(scan[1].2, test_ts(5));
    assert_eq!(scan[2].0, "c");
    assert_eq!(scan[2].1.as_deref(), Some("val_c"));
    assert_eq!(scan[2].2, test_ts(10));

    // After restore (no seal): data is in memtable only, VLog is still empty.
    assert_store_file_names(&restored_path, &["ir_vlog_0000.dat"]);
}

// === Helper ===

/// Extract finalized IR entries from the store.
fn extract_ir_record(
    store: &TestStore,
) -> Vec<(crate::ir::OpId, IrMemEntry<String, String>)> {
    store.extract_finalized_entries()
}
