use super::helpers::*;
use crate::unified::types::*;

#[test]
fn unified_store_basic_ir_overlay() {
    let mut store = new_test_store();

    assert_eq!(store.current_view(), 0);

    // Insert a CO::Prepare entry into the overlay
    let op_id = test_op_id(1, 1);
    let entry = IrMemEntry {
        entry_type: VlogEntryType::Prepare,
        state: IrState::Finalized(0),
        payload: IrPayloadInline::Prepare {
            transaction_id: test_txn_id(1, 1),
            commit_ts: test_ts(5),
            read_set: vec![],
            write_set: vec![(b"x".to_vec(), b"v1".to_vec())],
            scan_set: vec![],
        },
    };

    store.insert_ir_entry(op_id, entry);

    // Verify it's in the overlay
    let ir_entry = store.ir_entry(&op_id);
    assert!(ir_entry.is_some());
}

#[test]
fn unified_store_prepare_registry() {
    let mut store = new_test_store();

    let txn_id = test_txn_id(1, 1);
    let prepare = std::sync::Arc::new(CachedPrepare {
        transaction_id: txn_id,
        commit_ts: test_ts(5),
        read_set: vec![],
        write_set: vec![(b"x".to_vec(), b"v1".to_vec())],
        scan_set: vec![],
    });

    store.register_prepare_raw(txn_id, prepare);

    // Resolve from registry
    let entry = store.resolve_in_memory(&txn_id, 0);
    assert!(entry.is_some());
    let (key, value) = entry.unwrap();
    assert_eq!(key, b"x");
    assert_eq!(value, b"v1");

    // Out of bounds index
    assert!(store.resolve_in_memory(&txn_id, 1).is_none());

    // Unknown txn_id
    assert!(store.resolve_in_memory(&test_txn_id(99, 99), 0).is_none());

    // Unregister
    store.unregister_prepare(&txn_id);
    assert!(store.resolve_in_memory(&txn_id, 0).is_none());
}

#[test]
fn unified_store_seal_view() {
    let mut store = new_test_store();

    // Insert finalized entries
    store.insert_ir_entry(
        test_op_id(1, 1),
        IrMemEntry {
            entry_type: VlogEntryType::Prepare,
            state: IrState::Finalized(0),
            payload: IrPayloadInline::Prepare {
                transaction_id: test_txn_id(1, 1),
                commit_ts: test_ts(5),
                read_set: vec![],
                write_set: vec![(b"key1".to_vec(), b"val1".to_vec())],
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
                write_set: vec![(b"key2".to_vec(), b"val2".to_vec())],
                scan_set: vec![],
            },
        },
    );

    assert_eq!(store.current_view(), 0);

    // Seal view 0
    store.seal_current_view().unwrap();

    assert_eq!(store.current_view(), 1);

    // Finalized entries should be in IR base
    assert!(store.lookup_ir_base_entry(test_op_id(1, 1)).is_some());
    assert!(store.lookup_ir_base_entry(test_op_id(1, 2)).is_some());

    // Tentative entry should NOT be in IR base
    assert!(store.lookup_ir_base_entry(test_op_id(2, 1)).is_none());

    // Overlay should be empty
    assert!(store.ir_entry(&test_op_id(1, 1)).is_some()); // in base now
    assert!(store.ir_entry(&test_op_id(2, 1)).is_none()); // tentative, dropped
}

#[test]
fn unified_store_cross_view_read() {
    let mut store = new_test_store();

    // View 0: Write a prepare entry
    let payload = IrPayloadInline::Prepare {
        transaction_id: test_txn_id(1, 1),
        commit_ts: test_ts(5),
        read_set: vec![],
        write_set: vec![
            (b"key_a".to_vec(), b"value_a".to_vec()),
            (b"key_b".to_vec(), b"value_b".to_vec()),
        ],
        scan_set: vec![],
    };

    store.insert_ir_entry(
        test_op_id(1, 1),
        IrMemEntry {
            entry_type: VlogEntryType::Prepare,
            state: IrState::Finalized(0),
            payload,
        },
    );

    // Seal view 0 → view 1
    store.seal_current_view().unwrap();
    assert_eq!(store.current_view(), 1);

    // The prepare should be in the IR base (sealed VLog)
    let ir_sst = store.lookup_ir_base_entry(test_op_id(1, 1)).unwrap();
    assert_eq!(ir_sst.entry_type, VlogEntryType::Prepare);
    let vlog_ptr = ir_sst.vlog_ptr; // Copy before mutable borrow

    // Read the prepare from the sealed VLog
    let cached = store
        .resolve_on_disk(&UnifiedVlogPrepareValuePtr {
            prepare_ptr: vlog_ptr,
            write_index: 0,
        })
        .unwrap();

    assert_eq!(cached.write_set[0].0, b"key_a");
    assert_eq!(cached.write_set[0].1, b"value_a");
    assert_eq!(cached.write_set[1].0, b"key_b");
    assert_eq!(cached.write_set[1].1, b"value_b");

    // Second read should hit cache (check vlog_read_count)
    let reads_before = store.vlog_read_count();
    let _cached2 = store
        .resolve_on_disk(&UnifiedVlogPrepareValuePtr {
            prepare_ptr: vlog_ptr,
            write_index: 1,
        })
        .unwrap();
    assert_eq!(
        store.vlog_read_count(),
        reads_before,
        "Expected LRU cache hit"
    );
}

#[test]
fn prepare_cache_lru_eviction() {
    use crate::unified::prepare_cache::PrepareCache;

    let mut cache = PrepareCache::new(2);
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

    // Access p1 to make it recently used
    assert!(cache.get(0, 100).is_some());

    // Insert p3, should evict p2 (least recently used)
    cache.insert(0, 300, p3);
    assert_eq!(cache.len(), 2);

    assert!(cache.get(0, 100).is_some()); // p1 still present
    assert!(cache.get(0, 200).is_none()); // p2 evicted
    assert!(cache.get(0, 300).is_some()); // p3 present
}
