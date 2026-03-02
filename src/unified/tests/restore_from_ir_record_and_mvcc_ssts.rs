use std::collections::BTreeMap;

use crate::mvcc::disk::memory_io::MemoryIo;
use crate::tapir::Timestamp;
use crate::tapirstore::TapirStore;
use crate::unified::types::*;
use crate::unified::UnifiedStore;

use super::helpers::*;

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

    // Snapshot data from sealed views (the "SST backup")
    let mvcc_sst_snapshot: Vec<(String, Timestamp, UnifiedLsmEntry)>;
    // IR entries from unsealed view (need IR replay for these)
    let unsealedview_ir: Vec<(crate::ir::OpId, IrMemEntry<String, String>)>;

    // === Phase 1: Build state across multiple views ===
    {
        let mut store =
            UnifiedStore::<String, String, MemoryIo>::open_with_options(path.clone(), 64).unwrap();

        // Fresh store: only active VLog segment
        assert_store_file_names(&store, &["vlog_seg_0000.dat"]);
        assert_store_file_size(&store, "vlog_seg_0000.dat", 0);

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
        assert_current_view(&store, 1);

        // After seal with 64-byte threshold: segment rotation expected
        let files_after_seal = list_store_files(&store);
        assert!(
            files_after_seal.len() >= 2,
            "Should have manifest + VLog files after seal: {files_after_seal:?}"
        );
        assert_store_file_size_positive(&store, "UNIFIED_MANIFEST");

        // Verify entries are OnDisk after seal
        assert_value_location_in_memory(&store, "a", test_ts(5), false);
        assert_value_location_in_memory(&store, "b", test_ts(5), false);
        assert_value_location_in_memory(&store, "c", test_ts(10), false);

        // === Take "SST backup" ===
        // This captures all MVCC memtable entries (which are now OnDisk).
        // In a real system, this would be the persisted SST file content.
        mvcc_sst_snapshot = store
            .unified_memtable()
            .iter()
            .map(|(ck, entry)| (ck.key.clone(), ck.timestamp.0, entry.clone()))
            .collect();

        // View 1: Commit more data (not yet sealed)
        prepare_and_commit(
            &mut store,
            test_op_id(1, 1),
            test_op_id(1, 2),
            test_txn_id(1, 1),
            vec![("d", Some("val_d")), ("a", Some("val_a_v2"))],
            test_ts(20),
        );

        // These are InMemory (current view, not sealed)
        assert_value_location_in_memory(&store, "d", test_ts(20), false);
        assert_value_location_in_memory(&store, "a", test_ts(20), false);

        // === Take "IR record" for unsealed view ===
        unsealedview_ir = store
            .ir_overlay_entries()
            .map(|(op, e)| (*op, e.clone()))
            .collect();

        // Verify source state
        assert_get_at(&store, "a", test_ts(5), Some("val_a"), test_ts(5));
        assert_get_at(&store, "b", test_ts(5), Some("val_b"), test_ts(5));
        assert_get_at(&store, "c", test_ts(10), Some("val_c"), test_ts(10));
        assert_get_at(&store, "d", test_ts(20), Some("val_d"), test_ts(20));
        assert_get_at(&store, "a", test_ts(20), Some("val_a_v2"), test_ts(20));
    }

    // === Phase 2: Restore to a fresh store ===
    //
    // We use the same path so the VLog files are available (MemoryIo persists).
    // In a real backup scenario, you'd copy the VLog files to the restore target.

    let mut restored =
        UnifiedStore::<String, String, MemoryIo>::open_with_options(path, 64).unwrap();

    // Step 1: Load MVCC SST entries into the unified_memtable
    // These entries have OnDisk(ptr) ValueLocations pointing to the sealed VLog.
    for (key, ts, entry) in &mvcc_sst_snapshot {
        restored
            .unified_memtable_mut()
            .insert(key.clone(), *ts, entry.clone());
    }

    // Verify SST entries loaded — sealed view data is readable via OnDisk resolution
    assert_get_at(&restored, "a", test_ts(5), Some("val_a"), test_ts(5));
    assert_get_at(&restored, "b", test_ts(5), Some("val_b"), test_ts(5));
    assert_get_at(&restored, "c", test_ts(10), Some("val_c"), test_ts(10));
    assert_value_location_in_memory(&restored, "a", test_ts(5), false);
    assert_value_location_in_memory(&restored, "b", test_ts(5), false);
    assert_value_location_in_memory(&restored, "c", test_ts(10), false);

    // Step 2: Replay IR entries from the unsealed view
    // Only process Commit entries (with their matching Prepares)
    let mut prepare_index: BTreeMap<crate::occ::TransactionId, &IrPayloadInline<String, String>> =
        BTreeMap::new();
    for (_, entry) in &unsealedview_ir {
        if entry.entry_type == VlogEntryType::Prepare
            && let IrPayloadInline::Prepare {
                transaction_id, ..
            } = &entry.payload
        {
            prepare_index.insert(*transaction_id, &entry.payload);
        }
    }

    for (_, entry) in &unsealedview_ir {
        if entry.entry_type == VlogEntryType::Commit
            && let IrPayloadInline::Commit {
                transaction_id,
                commit_ts,
                ..
            } = &entry.payload
        {
            let prepare = prepare_index.get(transaction_id).unwrap();
            if let IrPayloadInline::Prepare {
                write_set,
                read_set,
                scan_set,
                ..
            } = prepare
            {
                // Register the prepare via Transaction
                let txn = build_txn_from_parts(read_set, write_set, scan_set);
                restored.register_prepare(*transaction_id, &txn, *commit_ts);

                restored
                    .commit_prepared(*transaction_id, *commit_ts)
                    .unwrap();
            }
        }
    }

    // === Phase 3: Verify ALL restored state ===

    // Sealed view data (from SST backup) — values resolve via OnDisk VLog reads
    assert_get_at(&restored, "a", test_ts(5), Some("val_a"), test_ts(5));
    assert_get_at(&restored, "b", test_ts(5), Some("val_b"), test_ts(5));
    assert_get_at(&restored, "c", test_ts(10), Some("val_c"), test_ts(10));
    assert_value_location_in_memory(&restored, "a", test_ts(5), false);
    assert_value_location_in_memory(&restored, "b", test_ts(5), false);
    assert_value_location_in_memory(&restored, "c", test_ts(10), false);

    // Unsealed view data (from IR replay) — values are InMemory
    assert_get_at(&restored, "d", test_ts(20), Some("val_d"), test_ts(20));
    assert_get_at(&restored, "a", test_ts(20), Some("val_a_v2"), test_ts(20));
    assert_value_location_in_memory(&restored, "d", test_ts(20), false);
    assert_value_location_in_memory(&restored, "a", test_ts(20), false);

    // Multi-version reads across sealed and unsealed views
    // "a" at ts=5 → "val_a" (from SST/OnDisk)
    // "a" at ts=20 → "val_a_v2" (from IR replay/InMemory)
    assert_get_at(&restored, "a", test_ts(5), Some("val_a"), test_ts(5));
    assert_get_at(&restored, "a", test_ts(20), Some("val_a_v2"), test_ts(20));
    assert_get_at(&restored, "a", test_ts(15), Some("val_a"), test_ts(5));

    // do_uncommitted_get() returns latest version
    let (val, ts) = restored.do_uncommitted_get(&"a".to_string()).unwrap();
    assert_eq!(val.as_deref(), Some("val_a_v2"), "get(a): latest value");
    assert_eq!(ts, test_ts(20), "get(a): latest timestamp");

    let (val, ts) = restored.do_uncommitted_get(&"d".to_string()).unwrap();
    assert_eq!(val.as_deref(), Some("val_d"), "get(d): value");
    assert_eq!(ts, test_ts(20), "get(d): timestamp");

    // get_range for multi-version key
    let (write_ts, next_ts) =
        restored.get_range(&"a".to_string(), test_ts(5)).unwrap();
    assert_eq!(write_ts, test_ts(5), "get_range(a, 5): write_ts");
    assert_eq!(
        next_ts,
        Some(test_ts(20)),
        "get_range(a, 5): successor at ts=20"
    );

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

    // has_writes_in_range
    let has = restored.has_writes_in_range(
        &"a".to_string(),
        &"z".to_string(),
        test_ts(10),
        test_ts(30),
    )
    .unwrap();
    assert!(has, "Should have writes in (10, 30) from view 1");

    let no_has = restored.has_writes_in_range(
        &"b".to_string(),
        &"b".to_string(),
        test_ts(5),
        test_ts(20),
    )
    .unwrap();
    assert!(!no_has, "Key 'b' has no writes in (5, 20)");

    // Nonexistent key
    assert_get_none(&restored, "nonexistent", test_ts(100));

    // No last_read_ts (OCC state starts fresh after restore)
    assert_last_read_ts(&restored, "a", None);
    assert_last_read_ts(&restored, "b", None);
    assert_last_read_ts(&restored, "c", None);
    assert_last_read_ts(&restored, "d", None);
}
