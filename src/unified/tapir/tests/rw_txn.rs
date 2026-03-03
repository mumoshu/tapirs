use super::helpers::*;
use crate::tapirstore::TapirStore;
use crate::unified::tapir::CachedPrepare;
use crate::unified::types::*;

// === Test 1: RW Transaction (prepare→commit→read through inherent methods) ===

#[test]
fn rw_txn_prepare_commit_read() {
    let mut store = new_test_store();

    // After open: only the active VLog segment exists (empty, no manifest yet)
    assert_store_file_names(&store, &["vlog_seg_0000.dat"]);
    assert_store_file_size(&store, "vlog_seg_0000.dat", 0);

    // Initial data via regular txn path
    prepare_and_commit(
        &mut store,
        test_op_id(0, 1),
        test_op_id(0, 2),
        test_txn_id(0, 1),
        vec![("x", Some("v1"))],
        test_ts(1),
    );
    assert_get_at(&store, "x", test_ts(1), Some("v1"), test_ts(1));

    // ValueLocation should be OnDisk via committed transaction VLog entry
    assert_value_location_in_memory(&store, "x", test_ts(1), false);

    // do_uncommitted_get() returns latest version
    let (val, ts) = store.do_uncommitted_get(&"x".to_string()).unwrap();
    assert_eq!(val.as_deref(), Some("v1"), "get(x): value mismatch");
    assert_eq!(ts, test_ts(1), "get(x): timestamp mismatch");

    // No last_read_ts set yet
    assert_last_read_ts(&store, "x", None);

    // get_range should return the write timestamp with no successor
    let (write_ts, next_ts) =
        store.get_range(&"x".to_string(), test_ts(1)).unwrap();
    assert_eq!(write_ts, test_ts(1), "get_range: write_ts mismatch");
    assert_eq!(next_ts, None, "get_range: expected no successor version");

    // Second txn with read_set + write_set
    let txn = make_txn(vec![("x", test_ts(1))], vec![("x", Some("v2"))]);
    prepare_txn(
        &mut store,
        test_op_id(1, 1),
        test_txn_id(1, 1),
        txn.clone(),
        test_ts(5),
        true,
    );
    commit_txn(
        &mut store,
        test_op_id(1, 2),
        test_txn_id(1, 1),
        test_ts(5),
        PrepareRef::SameView(test_op_id(1, 1)),
    );

    // Both versions are stored via committed transaction VLog entries
    assert_value_location_in_memory(&store, "x", test_ts(5), false);
    assert_value_location_in_memory(&store, "x", test_ts(1), false);

    // Read back at different timestamps — both value AND timestamp
    assert_get_at(&store, "x", test_ts(5), Some("v2"), test_ts(5));
    assert_get_at(&store, "x", test_ts(1), Some("v1"), test_ts(1));

    // do_uncommitted_get() returns latest (v2 at ts=5)
    let (val, ts) = store.do_uncommitted_get(&"x".to_string()).unwrap();
    assert_eq!(val.as_deref(), Some("v2"), "get(x) latest: value mismatch");
    assert_eq!(ts, test_ts(5), "get(x) latest: timestamp mismatch");

    // get_range at ts=1 should show successor at ts=5
    let (write_ts, next_ts) =
        store.get_range(&"x".to_string(), test_ts(1)).unwrap();
    assert_eq!(write_ts, test_ts(1), "get_range ts=1: write_ts");
    assert_eq!(next_ts, Some(test_ts(5)), "get_range ts=1: successor");

    // has_writes_in_range should detect the write at ts=5
    let has_writes = store.has_writes_in_range(
        &"x".to_string(),
        &"x".to_string(),
        test_ts(1),
        test_ts(10),
    )
    .unwrap();
    assert!(has_writes, "Expected write in range (1, 10) for key 'x'");

    // Nonexistent key returns None with default timestamp
    assert_get_none(&store, "nonexistent", test_ts(100));

    // No seal happened, but commit writes still append to active VLog segment.
    assert_store_file_names(&store, &["vlog_seg_0000.dat"]);
    assert_store_file_size_positive(&store, "vlog_seg_0000.dat");
}

// === Test 5: Sync replays prepare before commit ===

#[test]
fn sync_replays_prepare_before_commit() {
    let mut store = new_test_store();

    // Construct a transaction
    let txn = make_txn(vec![], vec![("x", Some("v1"))]);

    // Simulate sync(): consensus first (CO::Prepare), then inconsistent (IO::Commit)
    // Step 1: Process CO::Prepare → register_prepare stores in memory
    let prepare_entry = IrMemEntry {
        entry_type: VlogEntryType::Prepare,
        state: IrState::Finalized(1),
        payload: IrPayloadInline::Prepare {
            transaction_id: test_txn_id(1, 1),
            commit_ts: test_ts(5),
            read_set: vec![],
            write_set: vec![("x".to_string(), Some("v1".to_string()))],
            scan_set: vec![],
        },
    };
    store.insert_ir_entry(test_op_id(1, 1), prepare_entry);
    store.register_prepare(test_txn_id(1, 1), &txn, test_ts(5));

    // Step 2: Process IO::Commit → commit_prepared
    commit_txn(
        &mut store,
        test_op_id(1, 2),
        test_txn_id(1, 1),
        test_ts(5),
        PrepareRef::SameView(test_op_id(1, 1)),
    );

    // Verify OnDisk location + correct read (both value and timestamp)
    assert_value_location_in_memory(&store, "x", test_ts(5), false);
    assert_get_at(&store, "x", test_ts(5), Some("v1"), test_ts(5));

    // No last_read_ts (no commit_get called)
    assert_last_read_ts(&store, "x", None);

    // IR overlay should contain both entries
    assert!(
        store.ir_entry(&test_op_id(1, 1)).is_some(),
        "CO::Prepare should be in IR overlay"
    );
    assert!(
        store.ir_entry(&test_op_id(1, 2)).is_some(),
        "IO::Commit should be in IR overlay"
    );

    // No seal, active VLog still contains committed transaction entry
    assert_store_file_names(&store, &["vlog_seg_0000.dat"]);
    assert_store_file_size_positive(&store, "vlog_seg_0000.dat");
}

// === Existing internal tests (test UnifiedStore directly) ===

#[test]
fn unified_store_basic_ir_overlay() {
    let store = new_test_store();

    assert_eq!(store.current_view(), 0);
    assert_eq!(store.vlog_read_count(), 0);
    assert_eq!(store.sealed_vlog_segments().len(), 0);

    // Fresh store: only the active VLog segment file exists (empty)
    assert_store_file_names(&store, &["vlog_seg_0000.dat"]);
    assert_store_file_size(&store, "vlog_seg_0000.dat", 0);
}

#[test]
fn unified_store_prepare_registry() {
    let mut store = new_test_store();

    let txn_id = test_txn_id(1, 1);
    let txn = make_txn(vec![], vec![("x", Some("v1"))]);
    store.register_prepare(txn_id, &txn, test_ts(5));

    // Resolve from registry — verify both key and value
    let entry = store.resolve_in_memory(&txn_id, 0);
    assert!(entry.is_some(), "Expected entry in prepare registry");
    let (key, value) = entry.unwrap();
    assert_eq!(key, "x", "Prepare key mismatch");
    assert_eq!(value.as_deref(), Some("v1"), "Prepare value mismatch");

    // Out of bounds index
    assert!(
        store.resolve_in_memory(&txn_id, 1).is_none(),
        "write_index 1 should be out of bounds"
    );

    // Unknown txn_id
    assert!(
        store
            .resolve_in_memory(&test_txn_id(99, 99), 0)
            .is_none(),
        "Unknown txn_id should return None"
    );

    // Unregister and verify removal
    store.unregister_prepare(&txn_id);
    assert!(
        store.resolve_in_memory(&txn_id, 0).is_none(),
        "After unregister, resolve should return None"
    );

    // No seal, only active VLog (empty, all data is in-memory)
    assert_store_file_names(&store, &["vlog_seg_0000.dat"]);
    assert_store_file_size(&store, "vlog_seg_0000.dat", 0);
}

#[test]
fn unified_store_seal_view() {
    let mut store = new_test_store();

    // Insert finalized entries directly into IR overlay
    store.insert_ir_entry(
        test_op_id(1, 1),
        IrMemEntry {
            entry_type: VlogEntryType::Prepare,
            state: IrState::Finalized(0),
            payload: IrPayloadInline::Prepare {
                transaction_id: test_txn_id(1, 1),
                commit_ts: test_ts(5),
                read_set: vec![],
                write_set: vec![("key1".to_string(), Some("val1".to_string()))],
                scan_set: vec![],
            },
        },
    );

    store.insert_ir_entry(
        test_op_id(1, 2),
        IrMemEntry {
            entry_type: VlogEntryType::Commit,
            state: IrState::Finalized(0),
            payload: IrPayloadInline::Commit {
                transaction_id: test_txn_id(1, 1),
                commit_ts: test_ts(5),
                prepare_ref: PrepareRef::SameView(test_op_id(1, 1)),
            },
        },
    );

    // Also insert a tentative entry (should NOT be persisted)
    store.insert_ir_entry(
        test_op_id(2, 1),
        IrMemEntry {
            entry_type: VlogEntryType::Prepare,
            state: IrState::Tentative,
            payload: IrPayloadInline::Prepare {
                transaction_id: test_txn_id(2, 1),
                commit_ts: test_ts(10),
                read_set: vec![],
                write_set: vec![("key2".to_string(), Some("val2".to_string()))],
                scan_set: vec![],
            },
        },
    );

    assert_eq!(store.current_view(), 0);

    // Seal view 0
    store.seal_current_view().unwrap();

    assert_eq!(store.current_view(), 1);

    // Finalized entries should be in IR base with correct entry types
    let ir_base_prepare = store
        .lookup_ir_base_entry(test_op_id(1, 1))
        .expect("Finalized Prepare should be in IR base");
    assert_eq!(
        ir_base_prepare.entry_type,
        VlogEntryType::Prepare,
        "IR base entry type mismatch for Prepare"
    );
    assert!(
        ir_base_prepare.vlog_ptr.length > 0,
        "VLog pointer length should be non-zero"
    );
    assert_eq!(
        ir_base_prepare.vlog_ptr.segment_id,
        store
            .lookup_ir_base_entry(test_op_id(1, 2))
            .unwrap()
            .vlog_ptr
            .segment_id,
        "Both entries should be in the same VLog segment"
    );

    let ir_base_commit = store
        .lookup_ir_base_entry(test_op_id(1, 2))
        .expect("Finalized Commit should be in IR base");
    assert_eq!(
        ir_base_commit.entry_type,
        VlogEntryType::Commit,
        "IR base entry type mismatch for Commit"
    );
    assert!(
        ir_base_commit.vlog_ptr.length > 0,
        "VLog pointer length should be non-zero"
    );

    // Prepare should come before Commit in VLog (lower offset)
    assert!(
        ir_base_prepare.vlog_ptr.offset < ir_base_commit.vlog_ptr.offset,
        "Prepare should be at lower VLog offset than Commit"
    );

    // Tentative entry should NOT be in IR base
    assert!(
        store
            .lookup_ir_base_entry(test_op_id(2, 1))
            .is_none(),
        "Tentative entry should not be in IR base"
    );

    // Finalized entry in base (via ir_entry which checks overlay then base)
    assert!(
        store.ir_entry(&test_op_id(1, 1)).is_some(),
        "Finalized entry should be findable via ir_entry"
    );
    // Tentative entry dropped from overlay
    assert!(
        store.ir_entry(&test_op_id(2, 1)).is_none(),
        "Tentative entry should be dropped"
    );

    // After seal: VLog segment has data, manifest exists
    // (small data < 256KB threshold → no segment rotation, still segment 0)
    assert_store_file_names(&store, &["UNIFIED_MANIFEST", "vlog_seg_0000.dat"]);
    assert_store_file_size_positive(&store, "UNIFIED_MANIFEST");
    assert_store_file_size_positive(&store, "vlog_seg_0000.dat");
}

#[test]
fn unified_store_cross_view_read() {
    let mut store = new_test_store();

    // View 0: commit a transaction so values point to committed-txn VLog entries.
    let write_set = vec![
        ("key_a".to_string(), Some("value_a".to_string())),
        ("key_b".to_string(), Some("value_b".to_string())),
    ];
    store
        .commit_transaction_data(test_txn_id(1, 1), &[], &write_set, &[], test_ts(5))
        .unwrap();

    // VLog read count should be 0 before any disk reads
    assert_eq!(store.vlog_read_count(), 0);

    // Seal view 0 -> view 1
    store.seal_current_view().unwrap();
    assert_eq!(store.current_view(), 1);

    // Read pointer from committed MVCC index entry.
    let (_, entry) = store
        .unified_memtable()
        .get_at(&"key_a".to_string(), test_ts(5))
        .unwrap();
    let ValueLocation::OnDisk(ptr) = entry.value_ref.clone().unwrap() else {
        panic!("expected OnDisk value location");
    };
    let vlog_ptr = ptr.txn_ptr;

    // Read the prepare from the sealed VLog
    let cached = store
        .resolve_on_disk(&UnifiedVlogPrepareValuePtr {
            txn_ptr: vlog_ptr,
            write_index: 0,
        })
        .unwrap();

    // Verify ALL fields of the CachedPrepare
    assert_eq!(
        cached.transaction_id,
        test_txn_id(1, 1),
        "CachedPrepare transaction_id mismatch"
    );
    assert_eq!(
        cached.commit_ts,
        test_ts(5),
        "CachedPrepare commit_ts mismatch"
    );
    assert_eq!(
        cached.read_set.len(),
        0,
        "CachedPrepare read_set should be empty"
    );
    assert_eq!(
        cached.scan_set.len(),
        0,
        "CachedPrepare scan_set should be empty"
    );
    assert_eq!(
        cached.write_set.len(),
        2,
        "CachedPrepare write_set should have 2 entries"
    );
    assert_eq!(cached.write_set[0].0, "key_a");
    assert_eq!(cached.write_set[0].1, Some("value_a".to_string()));
    assert_eq!(cached.write_set[1].0, "key_b");
    assert_eq!(cached.write_set[1].1, Some("value_b".to_string()));

    // First read should have incremented vlog_read_count
    assert_eq!(store.vlog_read_count(), 1, "Expected 1 VLog read");

    // Second read should hit cache (check vlog_read_count unchanged)
    let reads_before = store.vlog_read_count();
    let cached2 = store
        .resolve_on_disk(&UnifiedVlogPrepareValuePtr {
            txn_ptr: vlog_ptr,
            write_index: 1,
        })
        .unwrap();
    assert_eq!(
        store.vlog_read_count(),
        reads_before,
        "Expected LRU cache hit"
    );
    // Verify the cached data is the same
    assert_eq!(cached2.write_set.len(), 2);
    assert_eq!(cached2.transaction_id, test_txn_id(1, 1));

    // After seal: manifest + active VLog with data
    assert_store_file_names(&store, &["UNIFIED_MANIFEST", "vlog_seg_0000.dat"]);
    assert_store_file_size_positive(&store, "UNIFIED_MANIFEST");
    assert_store_file_size_positive(&store, "vlog_seg_0000.dat");
}

#[test]
fn prepare_cache_lru_eviction() {
    use crate::unified::wisckeylsm::prepare_cache::PrepareCache;

    let mut cache = PrepareCache::<CachedPrepare<String, String>>::new(2);
    assert_eq!(cache.len(), 0);

    let p1 = std::sync::Arc::new(CachedPrepare {
        transaction_id: test_txn_id(1, 1),
        commit_ts: test_ts(1),
        read_set: vec![],
        write_set: vec![],
        scan_set: vec![],
    });

    let p2 = std::sync::Arc::new(CachedPrepare {
        transaction_id: test_txn_id(2, 2),
        commit_ts: test_ts(2),
        read_set: vec![],
        write_set: vec![],
        scan_set: vec![],
    });

    let p3 = std::sync::Arc::new(CachedPrepare {
        transaction_id: test_txn_id(3, 3),
        commit_ts: test_ts(3),
        read_set: vec![],
        write_set: vec![],
        scan_set: vec![],
    });

    cache.insert(0, 100, p1);
    cache.insert(0, 200, p2);
    assert_eq!(cache.len(), 2);

    // Access p1 to make it recently used — verify returned data
    let got_p1 = cache.get(0, 100);
    assert!(got_p1.is_some(), "p1 should be in cache");
    let got_p1 = got_p1.unwrap();
    assert_eq!(
        got_p1.transaction_id,
        test_txn_id(1, 1),
        "p1 transaction_id mismatch"
    );
    assert_eq!(got_p1.commit_ts, test_ts(1), "p1 commit_ts mismatch");

    // Insert p3, should evict p2 (least recently used)
    cache.insert(0, 300, p3);
    assert_eq!(cache.len(), 2);

    // p1 still present — verify its data
    let got_p1 = cache.get(0, 100).expect("p1 should still be present");
    assert_eq!(got_p1.transaction_id, test_txn_id(1, 1));

    // p2 evicted
    assert!(cache.get(0, 200).is_none(), "p2 should be evicted");

    // p3 present — verify its data
    let got_p3 = cache.get(0, 300).expect("p3 should be present");
    assert_eq!(
        got_p3.transaction_id,
        test_txn_id(3, 3),
        "p3 transaction_id mismatch"
    );
    assert_eq!(got_p3.commit_ts, test_ts(3), "p3 commit_ts mismatch");
}
