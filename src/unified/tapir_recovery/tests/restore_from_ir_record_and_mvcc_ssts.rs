use crate::mvcc::disk::memory_io::MemoryIo;
use crate::unified::ir::record::IrMemEntry;

use super::helpers::*;
use super::super::replay_committed_from_ir_record;

/// Demonstrates how to take a backup of the IR record AND MVCC SST entries
/// and rebuild everything from those two sources.
///
/// When MVCC SSTs exist (from previous view seals), the restore process is:
/// 1. Load MVCC SST entries → get key→ts→OnDisk(ptr) mappings for sealed views
/// 2. Open VLog files → values resolve through OnDisk pointers
/// 3. Replay only the IR entries from AFTER the SST snapshot → apply new commits
///
/// This is faster than full IR replay because sealed-view data is already
/// indexed in the SST — no need to re-match Prepare↔Commit pairs.
///
/// In the current implementation, "MVCC SSTs" are represented by the
/// unified_memtable entries with OnDisk ValueLocations (after seal, all
/// InMemory entries are converted to OnDisk). Once SST flush is implemented,
/// these would come from on-disk SST files.
#[test]
fn restore_from_ir_record_and_mvcc_sst_entries() {
    let path = MemoryIo::temp_path();

    // IR entries from unsealed view (need IR replay for these)
    let unsealedview_ir: Vec<(crate::ir::OpId, IrMemEntry<String, String>)>;

    // === Phase 1: Build state across multiple views ===
    {
        let mut store = TestStore::open_with_options(path.clone(), 64).unwrap();

        // Fresh store: only active VLog segment
        assert_store_file_names(&path, &["vlog_seg_0000.dat"]);
        assert_store_file_size(&path, "vlog_seg_0000.dat", 0);

        // View 0: Commit txn 1 and 2
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

        // Seal view 0 → view 1
        // After seal, memtable entries have OnDisk pointers to VLog
        seal_view(&mut store);
        assert_eq!(store.get_metrics().current_view, 1);

        // After seal with 64-byte threshold: segment rotation expected
        let files_after_seal = list_store_files(&path);
        assert!(
            files_after_seal.len() >= 2,
            "Should have manifest + VLog files after seal: {files_after_seal:?}"
        );
        assert_store_file_size_positive(&path, "UNIFIED_MANIFEST");

        // Sealed values should remain readable.
        let (_value, _ts) = store.do_uncommitted_get_at(&"a".to_string(), test_ts(5)).unwrap();

        // View 1: Commit more data (not yet sealed)
        prepare_and_commit(
            &mut store,
            test_op_id(1, 1),
            test_op_id(1, 2),
            test_txn_id(1, 1),
            vec![("d", Some("val_d")), ("a", Some("val_a_v2"))],
            test_ts(20),
        );

        // Unsealed commits should resolve correctly.
        let (_value, _ts) = store.do_uncommitted_get_at(&"d".to_string(), test_ts(20)).unwrap();

        // === Take "IR record" for unsealed view ===
        unsealedview_ir = store.extract_finalized_entries();

        // Verify source state
        let (actual_value, actual_ts) = store.do_uncommitted_get_at(&"a".to_string(), test_ts(5)).unwrap();
        assert_eq!(actual_value.as_deref(), Some("val_a"));
        assert_eq!(actual_ts, test_ts(5));
        let (actual_value, actual_ts) = store.do_uncommitted_get_at(&"b".to_string(), test_ts(5)).unwrap();
        assert_eq!(actual_value.as_deref(), Some("val_b"));
        assert_eq!(actual_ts, test_ts(5));
        let (actual_value, actual_ts) = store.do_uncommitted_get_at(&"c".to_string(), test_ts(10)).unwrap();
        assert_eq!(actual_value.as_deref(), Some("val_c"));
        assert_eq!(actual_ts, test_ts(10));
        let (actual_value, actual_ts) = store.do_uncommitted_get_at(&"d".to_string(), test_ts(20)).unwrap();
        assert_eq!(actual_value.as_deref(), Some("val_d"));
        assert_eq!(actual_ts, test_ts(20));
        let (actual_value, actual_ts) = store.do_uncommitted_get_at(&"a".to_string(), test_ts(20)).unwrap();
        assert_eq!(actual_value.as_deref(), Some("val_a_v2"));
        assert_eq!(actual_ts, test_ts(20));
    }

    // === Phase 2: Restore to a fresh store ===
    //
    // We use the same path so the VLog files are available (MemoryIo persists).
    // In a real backup scenario, you'd copy the VLog files to the restore target.

    let mut restored = TestStore::open_with_options(path, 64).unwrap();

    // Reopen from the same path restores sealed-view MVCC data from persisted state.
    // Verify sealed view data is readable via OnDisk resolution.
    
    let (actual_value, actual_ts) = restored.do_uncommitted_get_at(&"a".to_string(), test_ts(5)).unwrap();
    assert_eq!(actual_value.as_deref(), Some("val_a"));
    assert_eq!(actual_ts, test_ts(5));
    let (actual_value, actual_ts) = restored.do_uncommitted_get_at(&"b".to_string(), test_ts(5)).unwrap();
    assert_eq!(actual_value.as_deref(), Some("val_b"));
    assert_eq!(actual_ts, test_ts(5));
    let (actual_value, actual_ts) = restored.do_uncommitted_get_at(&"c".to_string(), test_ts(10)).unwrap();
    assert_eq!(actual_value.as_deref(), Some("val_c"));
    assert_eq!(actual_ts, test_ts(10));

    // Step 2: Replay IR entries from the unsealed view.
    replay_committed_from_ir_record(&mut restored, &unsealedview_ir).unwrap();

    // === Phase 3: Verify ALL restored state ===

    // Sealed view data (from SST backup) — values resolve via OnDisk VLog reads
    let (actual_value, actual_ts) = restored.do_uncommitted_get_at(&"a".to_string(), test_ts(5)).unwrap();
    assert_eq!(actual_value.as_deref(), Some("val_a"));
    assert_eq!(actual_ts, test_ts(5));
    let (actual_value, actual_ts) = restored.do_uncommitted_get_at(&"b".to_string(), test_ts(5)).unwrap();
    assert_eq!(actual_value.as_deref(), Some("val_b"));
    assert_eq!(actual_ts, test_ts(5));
    let (actual_value, actual_ts) = restored.do_uncommitted_get_at(&"c".to_string(), test_ts(10)).unwrap();
    assert_eq!(actual_value.as_deref(), Some("val_c"));
    assert_eq!(actual_ts, test_ts(10));

    // Unsealed view data (from IR replay) — values are InMemory until seal
    let (actual_value, actual_ts) = restored.do_uncommitted_get_at(&"d".to_string(), test_ts(20)).unwrap();
    assert_eq!(actual_value.as_deref(), Some("val_d"));
    assert_eq!(actual_ts, test_ts(20));
    let (actual_value, actual_ts) = restored.do_uncommitted_get_at(&"a".to_string(), test_ts(20)).unwrap();
    assert_eq!(actual_value.as_deref(), Some("val_a_v2"));
    assert_eq!(actual_ts, test_ts(20));
    let (_value, _ts) = restored.do_uncommitted_get_at(&"d".to_string(), test_ts(20)).unwrap();

    // Multi-version reads across sealed and unsealed views
    // "a" at ts=5 → "val_a" (from SST/OnDisk)
    // "a" at ts=20 → "val_a_v2" (from IR replay/InMemory)
    let (actual_value, actual_ts) = restored.do_uncommitted_get_at(&"a".to_string(), test_ts(5)).unwrap();
    assert_eq!(actual_value.as_deref(), Some("val_a"));
    assert_eq!(actual_ts, test_ts(5));
    let (actual_value, actual_ts) = restored.do_uncommitted_get_at(&"a".to_string(), test_ts(20)).unwrap();
    assert_eq!(actual_value.as_deref(), Some("val_a_v2"));
    assert_eq!(actual_ts, test_ts(20));
    let (actual_value, actual_ts) = restored.do_uncommitted_get_at(&"a".to_string(), test_ts(15)).unwrap();
    assert_eq!(actual_value.as_deref(), Some("val_a"));
    assert_eq!(actual_ts, test_ts(5));

    // do_uncommitted_get() returns latest version
    let (val, ts) = restored.do_uncommitted_get(&"a".to_string()).unwrap();
    assert_eq!(val.as_deref(), Some("val_a_v2"), "get(a): latest value");
    assert_eq!(ts, test_ts(20), "get(a): latest timestamp");

    let (val, ts) = restored.do_uncommitted_get(&"d".to_string()).unwrap();
    assert_eq!(val.as_deref(), Some("val_d"), "get(d): value");
    assert_eq!(ts, test_ts(20), "get(d): timestamp");

    // Scan at ts=20 returns all 4 unique keys
    let scan = restored.do_uncommitted_scan(
        &"a".to_string(),
        &"z".to_string(),
        test_ts(20),
    )
    .unwrap();
    assert_eq!(scan.len(), 4, "scan at ts=20 should return 4 keys");
    assert_eq!(scan[0].0, "a");
    assert_eq!(scan[0].1.as_deref(), Some("val_a_v2"));
    assert_eq!(scan[0].2, test_ts(20));
    assert_eq!(scan[1].0, "b");
    assert_eq!(scan[1].1.as_deref(), Some("val_b"));
    assert_eq!(scan[1].2, test_ts(5));
    assert_eq!(scan[2].0, "c");
    assert_eq!(scan[2].1.as_deref(), Some("val_c"));
    assert_eq!(scan[2].2, test_ts(10));
    assert_eq!(scan[3].0, "d");
    assert_eq!(scan[3].1.as_deref(), Some("val_d"));
    assert_eq!(scan[3].2, test_ts(20));

    // Scan at ts=10 returns only sealed-view keys (before view 1 data)
    let scan_early = restored.do_uncommitted_scan(
        &"a".to_string(),
        &"z".to_string(),
        test_ts(10),
    )
    .unwrap();
    assert_eq!(
        scan_early.len(),
        3,
        "scan at ts=10 should return 3 keys (a, b, c)"
    );
    assert_eq!(scan_early[0].0, "a");
    assert_eq!(scan_early[0].1.as_deref(), Some("val_a"));
    assert_eq!(scan_early[1].0, "b");
    assert_eq!(scan_early[2].0, "c");

    // Nonexistent key
    let (actual_value, _) = restored
        .do_uncommitted_get_at(&"nonexistent".to_string(), test_ts(100))
        .unwrap();
    assert!(actual_value.is_none());

}
