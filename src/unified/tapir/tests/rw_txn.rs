use super::helpers::*;
use crate::unified::tapir::CachedPrepare;
use crate::unified::tapir::storage_types::{LsmEntry, ValueLocation, VlogTransactionPtr};

#[test]
fn rw_txn_prepare_commit_read() {
    let mut store = new_test_store();

    assert_store_file_names(&store, &["vlog_seg_0000.dat"]);
    assert_store_file_size(&store, "vlog_seg_0000.dat", 0);

    prepare_and_commit(
        &mut store,
        test_op_id(0, 1),
        test_op_id(0, 2),
        test_txn_id(0, 1),
        vec![("x", Some("v1"))],
        test_ts(1),
    );
    assert_get_at(&store, "x", test_ts(1), Some("v1"), test_ts(1));
    assert_value_location_in_memory(&store, "x", test_ts(1), true);

    let (val, ts) = store.do_uncommitted_get(&"x".to_string()).unwrap();
    assert_eq!(val.as_deref(), Some("v1"));
    assert_eq!(ts, test_ts(1));

    assert_last_read_ts(&store, "x", None);

    let (write_ts, next_ts) = store.get_range(&"x".to_string(), test_ts(1)).unwrap();
    assert_eq!(write_ts, test_ts(1));
    assert_eq!(next_ts, None);

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
        txn,
        test_ts(5),
    );

    assert_value_location_in_memory(&store, "x", test_ts(5), true);
    assert_get_at(&store, "x", test_ts(5), Some("v2"), test_ts(5));
    assert_get_at(&store, "x", test_ts(1), Some("v1"), test_ts(1));

    let (write_ts, next_ts) = store.get_range(&"x".to_string(), test_ts(1)).unwrap();
    assert_eq!(write_ts, test_ts(1));
    assert_eq!(next_ts, Some(test_ts(5)));

    let has_writes = store
        .has_writes_in_range(&"x".to_string(), &"x".to_string(), test_ts(1), test_ts(10))
        .unwrap();
    assert!(has_writes);

    assert_get_none(&store, "nonexistent", test_ts(100));
    assert_store_file_names(&store, &["vlog_seg_0000.dat"]);
    assert_store_file_size_positive(&store, "vlog_seg_0000.dat");
}

#[test]
fn tapir_state_prepare_registry() {
    let mut store = new_test_store();

    let txn_id = test_txn_id(1, 1);
    let txn = make_txn(vec![], vec![("x", Some("v1"))]);
    store.register_prepare(txn_id, &txn, test_ts(5));

    let entry = store.resolve_in_memory(&txn_id, 0);
    assert!(entry.is_some());
    let (key, value) = entry.unwrap();
    assert_eq!(key, "x");
    assert_eq!(value.as_deref(), Some("v1"));

    assert!(store.resolve_in_memory(&txn_id, 1).is_none());
    assert!(store.resolve_in_memory(&test_txn_id(99, 99), 0).is_none());

    store.unregister_prepare_entry(&txn_id);
    assert!(store.resolve_in_memory(&txn_id, 0).is_none());

    assert_store_file_names(&store, &["vlog_seg_0000.dat"]);
    assert_store_file_size(&store, "vlog_seg_0000.dat", 0);
}

#[test]
fn tapir_state_cross_view_read_and_cache() {
    let mut store = new_test_store();

    let write_set = vec![
        ("key_a".to_string(), Some("value_a".to_string())),
        ("key_b".to_string(), Some("value_b".to_string())),
    ];
    store
        .commit_transaction_data(test_txn_id(1, 1), &[], &write_set, &[], test_ts(5))
        .unwrap();

    assert_eq!(store.vlog_read_count(), 0);

    store.seal_current_view(u64::MAX).unwrap();
    assert_eq!(store.current_view(), 1);

    let entry = store
        .memtable_entry_at(&"key_a".to_string(), test_ts(5))
        .unwrap();
    let ValueLocation::OnDisk(ptr) = entry.value_ref.clone().unwrap() else {
        panic!("expected OnDisk value location");
    };
    let vlog_ptr = ptr.txn_ptr;

    let value_a_entry = LsmEntry {
        value_ref: Some(ValueLocation::OnDisk(VlogTransactionPtr {
            txn_ptr: vlog_ptr,
            write_index: 0,
        })),
        last_read_ts: None,
    };
    let resolved_a = store.resolve_value(&value_a_entry).unwrap();
    assert_eq!(resolved_a.as_deref(), Some("value_a"));
    assert_eq!(store.vlog_read_count(), 1);

    let reads_before = store.vlog_read_count();
    let value_b_entry = LsmEntry {
        value_ref: Some(ValueLocation::OnDisk(VlogTransactionPtr {
            txn_ptr: vlog_ptr,
            write_index: 1,
        })),
        last_read_ts: None,
    };
    let resolved_b = store.resolve_value(&value_b_entry).unwrap();
    assert_eq!(resolved_b.as_deref(), Some("value_b"));
    assert_eq!(store.vlog_read_count(), reads_before);

    assert_store_file_names(&store, &["UNIFIED_MANIFEST", "vlog_seg_0000.dat"]);
    assert_store_file_size_positive(&store, "UNIFIED_MANIFEST");
    assert_store_file_size_positive(&store, "vlog_seg_0000.dat");
}

#[test]
fn prepare_cache_lru_eviction() {
    use crate::unified::tapir::prepare_cache::PreparedTransactions;

    let mut cache = PreparedTransactions::<CachedPrepare<String, String>>::new(2);
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

    let got_p1 = cache.get(0, 100).unwrap();
    assert_eq!(got_p1.transaction_id, test_txn_id(1, 1));

    cache.insert(0, 300, p3);
    assert_eq!(cache.len(), 2);

    let got_p1 = cache.get(0, 100).expect("p1 should still be present");
    assert_eq!(got_p1.transaction_id, test_txn_id(1, 1));

    assert!(cache.get(0, 200).is_none());

    let got_p3 = cache.get(0, 300).expect("p3 should be present");
    assert_eq!(got_p3.transaction_id, test_txn_id(3, 3));
}
