use super::helpers::*;
use crate::unified::tapir::Transaction;

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

    assert_get_at(&store, "x", test_ts(2), Some("v1"), test_ts(1));
    assert_get_at(&store, "x", test_ts(5), Some("v2"), test_ts(5));

    assert_get_none(&store, "nonexistent", test_ts(100));
    assert_store_file_names(&store, &["vlog_seg_0000.dat"]);
    assert_store_file_size_positive(&store, "vlog_seg_0000.dat");
}

#[test]
fn tapir_state_prepare_registry() {
    let mut store = new_test_store();

    let txn_id = test_txn_id(1, 1);
    let txn = make_txn(vec![], vec![("x", Some("v1"))]);
    store.prepare(txn_id, &txn, test_ts(5));

    let (value, ts) = store.do_uncommitted_get(&"x".to_string()).unwrap();
    assert!(value.is_none());
    assert_eq!(ts, Default::default());

    store.abort(&txn_id);
    let (value, ts) = store.do_uncommitted_get(&"x".to_string()).unwrap();
    assert!(value.is_none());
    assert_eq!(ts, Default::default());

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
        .commit(test_txn_id(1, 1), &[], &write_set, &[], test_ts(5))
        .unwrap();

    store.seal_current_view(u64::MAX).unwrap();
    assert_current_view(&store, 1);

    let (value_a, _) = store.do_uncommitted_get(&"key_a".to_string()).unwrap();
    assert_eq!(value_a.as_deref(), Some("value_a"));

    let (value_b, _) = store.do_uncommitted_get(&"key_b".to_string()).unwrap();
    assert_eq!(value_b.as_deref(), Some("value_b"));

    assert_store_file_names(&store, &["UNIFIED_MANIFEST", "vlog_seg_0000.dat"]);
    assert_store_file_size_positive(&store, "UNIFIED_MANIFEST");
    assert_store_file_size_positive(&store, "vlog_seg_0000.dat");
}

#[test]
fn prepare_cache_lru_eviction() {
    use crate::unified::tapir::prepare_cache::PreparedTransactions;

    let mut cache = PreparedTransactions::<Transaction<String, String>>::new(2);

    let p1 = std::sync::Arc::new(Transaction {
        transaction_id: test_txn_id(1, 1),
        commit_ts: test_ts(1),
        read_set: vec![],
        write_set: vec![],
    });

    let p2 = std::sync::Arc::new(Transaction {
        transaction_id: test_txn_id(2, 2),
        commit_ts: test_ts(2),
        read_set: vec![],
        write_set: vec![],
    });

    let p3 = std::sync::Arc::new(Transaction {
        transaction_id: test_txn_id(3, 3),
        commit_ts: test_ts(3),
        read_set: vec![],
        write_set: vec![],
    });

    cache.insert(0, 100, p1);
    cache.insert(0, 200, p2);
    assert!(cache.get(0, 100).is_some());
    assert!(cache.get(0, 200).is_some());

    let got_p1 = cache.get(0, 100).unwrap();
    assert_eq!(got_p1.transaction_id, test_txn_id(1, 1));

    cache.insert(0, 300, p3);

    let got_p1 = cache.get(0, 100).expect("p1 should still be present");
    assert_eq!(got_p1.transaction_id, test_txn_id(1, 1));

    assert!(cache.get(0, 200).is_none());

    let got_p3 = cache.get(0, 300).expect("p3 should be present");
    assert_eq!(got_p3.transaction_id, test_txn_id(3, 3));
}
