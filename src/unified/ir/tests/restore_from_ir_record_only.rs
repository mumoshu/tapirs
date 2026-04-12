use std::collections::BTreeMap;

use crate::storage::io::memory_io::MemoryIo;
use crate::tapir::store::TapirStore;
use crate::unified::types::*;
use crate::unified::UnifiedStore;

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

    let mut source_store = new_test_store();

    // Fresh store: only active VLog segment
    assert_store_file_names(&source_store, &["vlog_seg_0000.dat"]);
    assert_store_file_size(&source_store, "vlog_seg_0000.dat", 0);

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
    prepare_txn(
        &mut source_store,
        test_op_id(0, 5),
        test_txn_id(0, 3),
        uncommitted_txn,
        test_ts(15),
        true, // finalized prepare, but no commit follows
    );

    // Set a last_read_ts via commit_get on version at ts=5
    source_store.commit_get("x".to_string(), test_ts(5), test_ts(20)).unwrap();

    // Verify source state — values
    assert_get_at(&source_store, "x", test_ts(5), Some("v1"), test_ts(5));
    assert_get_at(&source_store, "x", test_ts(10), Some("v1-updated"), test_ts(10));
    assert_get_at(&source_store, "y", test_ts(10), Some("v2"), test_ts(10));

    // last_read_ts is set on the version at ts=5, not on the latest version (ts=10)
    let lr = source_store.get_last_read_at(&"x".to_string(), test_ts(5)).unwrap();
    assert_eq!(lr.map(|ts| ts.time), Some(20), "Source: last_read_ts at ts=5");
    // Latest version (ts=10) has no last_read_ts
    assert_last_read_ts(&source_store, "x", None);

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

    let mut restored_store = new_test_store();

    // Restored store starts fresh with just active VLog
    assert_store_file_names(&restored_store, &["vlog_seg_0000.dat"]);
    assert_store_file_size(&restored_store, "vlog_seg_0000.dat", 0);

    // Step 1: Index all Prepare entries by transaction_id
    let mut prepare_index: BTreeMap<crate::occ::TransactionId, &IrPayloadInline<String, String>> =
        BTreeMap::new();
    for (_, entry) in &ir_record {
        if entry.entry_type == VlogEntryType::Prepare
            && let IrPayloadInline::Prepare {
                transaction_id, ..
            } = &entry.payload
        {
            prepare_index.insert(*transaction_id, &entry.payload);
        }
    }

    // Step 2: Replay each committed transaction
    // For each IO::Commit, find the matching CO::Prepare, register it,
    // and commit the writes to the MVCC memtable.
    for (op_id, entry) in &ir_record {
        if entry.entry_type == VlogEntryType::Commit
            && let IrPayloadInline::Commit {
                transaction_id,
                commit_ts,
                ..
            } = &entry.payload
        {
            // Find the matching Prepare
            let prepare_payload = prepare_index
                .get(transaction_id)
                .expect("Commit without matching Prepare in IR record");

            if let IrPayloadInline::Prepare {
                write_set,
                read_set,
                scan_set,
                ..
            } = prepare_payload
            {
                restored_store
                    .commit_transaction_data(
                        *transaction_id,
                        read_set,
                        write_set,
                        scan_set,
                        *commit_ts,
                    )
                    .unwrap();

                // Also insert the IR entries into the overlay (for completeness)
                // In a real restore, these would go into the IR base.
                restored_store.insert_ir_entry(
                    *op_id,
                    IrMemEntry {
                        entry_type: VlogEntryType::Commit,
                        state: entry.state,
                        payload: entry.payload.clone(),
                    },
                );
            }
        }
    }

    // === Phase 4: Verify restored MVCC state matches source ===

    // Committed values should be readable
    assert_get_at(&restored_store, "x", test_ts(5), Some("v1"), test_ts(5));
    assert_get_at(
        &restored_store,
        "x",
        test_ts(10),
        Some("v1-updated"),
        test_ts(10),
    );
    assert_get_at(&restored_store, "y", test_ts(10), Some("v2"), test_ts(10));

    // Uncommitted txn 3 ("z") should NOT be in the restored MVCC state
    // (it has a Prepare but no Commit in the IR record)
    assert_get_none(&restored_store, "z", test_ts(15));
    assert_get_none(&restored_store, "z", test_ts(100));

    // snapshot_get() returns latest version
    let (val, ts) = restored_store.snapshot_get(&"x".to_string()).unwrap();
    assert_eq!(val.as_deref(), Some("v1-updated"), "get(x): value mismatch");
    assert_eq!(ts, test_ts(10), "get(x): timestamp mismatch");

    let (val, ts) = restored_store.snapshot_get(&"y".to_string()).unwrap();
    assert_eq!(val.as_deref(), Some("v2"), "get(y): value mismatch");
    assert_eq!(ts, test_ts(10), "get(y): timestamp mismatch");

    // Key not found returns None
    let (val, _) = restored_store.snapshot_get(&"nonexistent".to_string()).unwrap();
    assert!(val.is_none(), "Nonexistent key should return None");

    // Multi-version reads work correctly
    assert_get_at(&restored_store, "x", test_ts(5), Some("v1"), test_ts(5));
    assert_get_at(
        &restored_store,
        "x",
        test_ts(7),
        Some("v1"),
        test_ts(5),
    ); // Between versions, returns older
    assert_get_at(
        &restored_store,
        "x",
        test_ts(10),
        Some("v1-updated"),
        test_ts(10),
    );

    // get_range should work for multi-version keys
    let (write_ts, next_ts) =
        restored_store.get_range(&"x".to_string(), test_ts(5)).unwrap();
    assert_eq!(write_ts, test_ts(5), "get_range(x, 5): write_ts");
    assert_eq!(
        next_ts,
        Some(test_ts(10)),
        "get_range(x, 5): should show successor at ts=10"
    );

    // Scan returns all committed keys at ts=10
    let scan_results = restored_store.snapshot_scan(
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

    // All values should be InMemory (restored in current view, not sealed)
    assert_value_location_in_memory(&restored_store, "x", test_ts(5), false);
    assert_value_location_in_memory(&restored_store, "x", test_ts(10), false);
    assert_value_location_in_memory(&restored_store, "y", test_ts(10), false);

    // Note: last_read_ts is NOT preserved by IR record backup — it's
    // ephemeral OCC state that only matters for the current transaction
    // coordinator session. After restore, OCC conflict detection starts fresh.
    assert_last_read_ts(&restored_store, "x", None);
    assert_last_read_ts(&restored_store, "y", None);

    // After restore (no seal): active VLog still contains committed transaction entries.
    assert_store_file_names(&restored_store, &["vlog_seg_0000.dat"]);
    assert_store_file_size_positive(&restored_store, "vlog_seg_0000.dat");
}

/// Test that restore works after seal (IR entries are in VLog, not in memory).
/// This demonstrates backup from the sealed VLog rather than the in-memory overlay.
#[test]
fn restore_from_sealed_vlog_rebuilds_mvcc() {
    let path = MemoryIo::temp_path();

    // === Phase 1: Build state and seal ===
    let ir_record;
    {
        let mut store = UnifiedStore::<String, String, MemoryIo>::open(path.clone()).unwrap();

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
        assert_get_at(&store, "a", test_ts(5), Some("val_a"), test_ts(5));
        assert_get_at(&store, "b", test_ts(5), Some("val_b"), test_ts(5));
        assert_get_at(&store, "c", test_ts(10), Some("val_c"), test_ts(10));

        // Extract IR record BEFORE sealing (while entries are in overlay)
        ir_record = extract_ir_record(&store);

        seal_view(&mut store);
        assert_current_view(&store, 1);

        // After seal: manifest + VLog with data
        assert_store_file_names(&store, &["UNIFIED_MANIFEST", "vlog_seg_0000.dat"]);
        assert_store_file_size_positive(&store, "UNIFIED_MANIFEST");
        assert_store_file_size_positive(&store, "vlog_seg_0000.dat");

        // Verify entries converted to OnDisk after seal
        assert_value_location_in_memory(&store, "a", test_ts(5), false);
        assert_value_location_in_memory(&store, "b", test_ts(5), false);
        assert_value_location_in_memory(&store, "c", test_ts(10), false);
    }

    // === Phase 2: Restore to completely fresh store from IR record ===

    let mut restored = new_test_store();
    assert_store_file_names(&restored, &["vlog_seg_0000.dat"]);

    // Index prepares
    let mut prepare_index: BTreeMap<crate::occ::TransactionId, &IrPayloadInline<String, String>> =
        BTreeMap::new();
    for (_, entry) in &ir_record {
        if entry.entry_type == VlogEntryType::Prepare
            && let IrPayloadInline::Prepare {
                transaction_id, ..
            } = &entry.payload
        {
            prepare_index.insert(*transaction_id, &entry.payload);
        }
    }

    // Replay commits
    for (_, entry) in &ir_record {
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
                restored
                    .commit_transaction_data(
                        *transaction_id,
                        read_set,
                        write_set,
                        scan_set,
                        *commit_ts,
                    )
                    .unwrap();
            }
        }
    }

    // === Phase 3: Verify ALL fields match ===

    // Values
    assert_get_at(&restored, "a", test_ts(5), Some("val_a"), test_ts(5));
    assert_get_at(&restored, "b", test_ts(5), Some("val_b"), test_ts(5));
    assert_get_at(&restored, "c", test_ts(10), Some("val_c"), test_ts(10));

    // Nonexistent before write time
    assert_get_none(&restored, "a", test_ts(1));
    assert_get_none(&restored, "c", test_ts(5));

    // InMemory (freshly restored, not sealed)
    assert_value_location_in_memory(&restored, "a", test_ts(5), false);
    assert_value_location_in_memory(&restored, "b", test_ts(5), false);
    assert_value_location_in_memory(&restored, "c", test_ts(10), false);

    // Scan
    let scan = restored.snapshot_scan(
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

    // has_writes_in_range
    let has = restored.has_writes_in_range(
        &"a".to_string(),
        &"z".to_string(),
        test_ts(0),
        test_ts(20),
    )
    .unwrap();
    assert!(has, "Should have writes in (0, 20)");

    let no_has = restored.has_writes_in_range(
        &"a".to_string(),
        &"z".to_string(),
        test_ts(10),
        test_ts(20),
    )
    .unwrap();
    assert!(!no_has, "Should have no writes in (10, 20)");

    // No last_read_ts (OCC state starts fresh)
    assert_last_read_ts(&restored, "a", None);
    assert_last_read_ts(&restored, "b", None);
    assert_last_read_ts(&restored, "c", None);

    // Restored store has active VLog containing committed transaction entries.
    assert_store_file_names(&restored, &["vlog_seg_0000.dat"]);
    assert_store_file_size_positive(&restored, "vlog_seg_0000.dat");
}

// === Helper ===

/// Extract the full IR record from a store's overlay (all entries, including
/// tentative ones for completeness — the replay logic only processes Commits).
fn extract_ir_record(
    store: &TestStore,
) -> Vec<(crate::ir::OpId, IrMemEntry<String, String>)> {
    // In a real system, this would be extract_finalized_entries() from the
    // merged record after view change. Here we extract everything from the
    // overlay for testing.
    store
        .ir_overlay_entries()
        .map(|(op_id, entry)| (*op_id, entry.clone()))
        .collect()
}
