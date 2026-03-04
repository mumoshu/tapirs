use super::helpers::*;
use crate::mvcc::disk::memory_io::MemoryIo;
use crate::unified::ir::record::PrepareRef;

// === Test 3: View Change (seal + merge + MVCC reads across views) ===

#[test]
fn view_change_seal_merge_sync() {
    let path = MemoryIo::temp_path();
    let mut store = TestStore::open(path.clone()).unwrap();
    assert_eq!(store.get_metrics().current_view, 0);

    // Fresh store: only active VLog segment
    assert_store_file_names(&path, &["ir_vlog_0000.dat"]);
    assert_store_file_size(&path, "ir_vlog_0000.dat", 0);

    // Phase 1: View 0 operations
    prepare_and_commit(
        &mut store,
        test_op_id(0, 1),
        test_op_id(0, 2),
        test_txn_id(0, 1),
        vec![("a", Some("v1"))],
        test_ts(5),
    );

    // Unsealed committed value should be readable
    let (actual_value, actual_ts) = store.do_uncommitted_get_at(&"a".to_string(), test_ts(5)).unwrap();
    assert_eq!(actual_value.as_deref(), Some("v1"));
    assert_eq!(actual_ts, test_ts(5));

    // Tentative prepare (won't survive merge)
    let txn2 = make_txn(vec![], vec![("b", Some("v2"))]);
    store.prepare(
        test_op_id(0, 3),
        test_txn_id(0, 2),
        txn2,
        test_ts(10),
        false,
    );

    // Phase 2: Seal view 0 → view 1
    seal_view(&mut store);
    assert_eq!(store.get_metrics().current_view, 1);

    // After seal: manifest saved, VLog has data
    assert_store_file_names(&path, &["UNIFIED_MANIFEST", "ir_vlog_0000.dat"]);
    assert_store_file_size_positive(&path, "UNIFIED_MANIFEST");
    assert_store_file_size_positive(&path, "ir_vlog_0000.dat");

    // Phase 3: Reads across sealed views
    // Committed "a" should be readable (OnDisk after seal)
    let (actual_value, actual_ts) = store.do_uncommitted_get_at(&"a".to_string(), test_ts(5)).unwrap();
    assert_eq!(actual_value.as_deref(), Some("v1"));
    assert_eq!(actual_ts, test_ts(5));

    // Tentative "b" was never committed — not in unified_memtable
    let (actual_value, _) = store
        .do_uncommitted_get_at(&"b".to_string(), test_ts(10))
        .unwrap();
    assert!(actual_value.is_none());

    // do_uncommitted_get() returns the latest version of "a"
    let (val, ts) = store.do_uncommitted_get(&"a".to_string()).unwrap();
    assert_eq!(val.as_deref(), Some("v1"), "get(a): value mismatch");
    assert_eq!(ts, test_ts(5), "get(a): timestamp mismatch");
}

// === Test 4: Cross-View (prepare in view N, commit in view N+M) ===

#[test]
fn cross_view_prepare_commit() {
    let path = MemoryIo::temp_path();
    let mut store = TestStore::open(path.clone()).unwrap();

    // View 0: Prepare only (no commit yet)
    let txn = make_txn(vec![], vec![("x", Some("v1"))]);
    store.prepare(
        test_op_id(1, 1),
        test_txn_id(1, 1),
        txn.clone(),
        test_ts(5),
        true,
    );

    // Before seal: only active VLog segment
    assert_store_file_names(&path, &["ir_vlog_0000.dat"]);

    // Seal view 0 → view 1
    seal_view(&mut store);
    assert_eq!(store.get_metrics().current_view, 1);

    // After seal: manifest + VLog with data
    assert_store_file_names(&path, &["UNIFIED_MANIFEST", "ir_vlog_0000.dat"]);
    assert_store_file_size_positive(&path, "UNIFIED_MANIFEST");
    assert_store_file_size_positive(&path, "ir_vlog_0000.dat");

    // View 1: Commit prepared transaction.
    let txn = make_txn(vec![], vec![("x", Some("v1"))]);
    store.commit(
        test_op_id(1, 2),
        test_txn_id(1, 1),
        txn,
        test_ts(5),
        PrepareRef::SameView(test_op_id(1, 1)),
    );

    // Before seal, committed value resolves from in-memory prepare data
    let (actual_value, actual_ts) = store.do_uncommitted_get_at(&"x".to_string(), test_ts(5)).unwrap();
    assert_eq!(actual_value.as_deref(), Some("v1"));
    assert_eq!(actual_ts, test_ts(5));

    // Second read: LRU cache hit (zero additional VLog I/O)
    let (actual_value, actual_ts) = store.do_uncommitted_get_at(&"x".to_string(), test_ts(5)).unwrap();
    assert_eq!(actual_value.as_deref(), Some("v1"));
    assert_eq!(actual_ts, test_ts(5));

    // Nonexistent key should return None
    let (actual_value, _) = store.do_uncommitted_get_at(&"y".to_string(), test_ts(5)).unwrap();
    assert!(actual_value.is_none());

}

// === Test: Multi-client prepare/commit behavior across seal ===

#[test]
fn multi_client_commits_survive_seal() {
    let path = MemoryIo::temp_path();
    let mut store = TestStore::open(path).unwrap();

    // Two clients prepare entries at different timestamps
    let txn1 = make_txn(vec![], vec![("a", Some("from_c1"))]);
    store.prepare(
        test_op_id(1, 1),
        test_txn_id(1, 1),
        txn1.clone(),
        test_ts_client(5, 1),
        true,
    );

    let txn2 = make_txn(vec![], vec![("b", Some("from_c2"))]);
    store.prepare(
        test_op_id(2, 1),
        test_txn_id(2, 1),
        txn2.clone(),
        test_ts_client(10, 2),
        true,
    );

    // Commit both
    store.commit(
        test_op_id(1, 2),
        test_txn_id(1, 1),
        txn1,
        test_ts_client(5, 1),
        PrepareRef::SameView(test_op_id(1, 1)),
    );
    store.commit(
        test_op_id(2, 2),
        test_txn_id(2, 1),
        txn2,
        test_ts_client(10, 2),
        PrepareRef::SameView(test_op_id(2, 1)),
    );

    // Both readable before seal
    let (actual_value, actual_ts) = store
        .do_uncommitted_get_at(&"a".to_string(), test_ts_client(5, 1))
        .unwrap();
    assert_eq!(actual_value.as_deref(), Some("from_c1"));
    assert_eq!(actual_ts, test_ts_client(5, 1));
    let (actual_value, actual_ts) = store
        .do_uncommitted_get_at(&"b".to_string(), test_ts_client(10, 2))
        .unwrap();
    assert_eq!(actual_value.as_deref(), Some("from_c2"));
    assert_eq!(actual_ts, test_ts_client(10, 2));

    // Seal to persist VLog data and convert InMemory→OnDisk
    seal_view(&mut store);
    assert_eq!(store.get_metrics().current_view, 1);

    // Data still readable after seal (OnDisk)
    let (actual_value, actual_ts) = store
        .do_uncommitted_get_at(&"a".to_string(), test_ts_client(5, 1))
        .unwrap();
    assert_eq!(actual_value.as_deref(), Some("from_c1"));
    assert_eq!(actual_ts, test_ts_client(5, 1));
    let (actual_value, actual_ts) = store
        .do_uncommitted_get_at(&"b".to_string(), test_ts_client(10, 2))
        .unwrap();
    assert_eq!(actual_value.as_deref(), Some("from_c2"));
    assert_eq!(actual_ts, test_ts_client(10, 2));

    // Scan returns both keys
    let scan = store.do_uncommitted_scan(
        &"a".to_string(),
        &"z".to_string(),
        test_ts_client(20, 1),
    )
    .unwrap();
    assert_eq!(scan.len(), 2, "scan should return 2 keys");
    assert_eq!(scan[0].0, "a");
    assert_eq!(scan[0].1.as_deref(), Some("from_c1"));
    assert_eq!(scan[1].0, "b");
    assert_eq!(scan[1].1.as_deref(), Some("from_c2"));
}
