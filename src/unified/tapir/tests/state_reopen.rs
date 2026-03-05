use super::helpers::{make_txn, test_ts, test_txn_id};
use crate::mvcc::disk::disk_io::OpenFlags;
use crate::mvcc::disk::memory_io::MemoryIo;
use crate::unified::tapir;

#[test]
fn tapir_state_prepare_conflict_commit_seal_reopen_get_scan() {
    let path = MemoryIo::temp_path();
    let io_flags = OpenFlags {
        create: true,
        direct: false,
    };

    let mut store = tapir::store::TapirStateRunner::open(path.clone(), io_flags).unwrap();

    let files_on_open: Vec<String> = store
        .list_dir_files()
        .into_iter()
        .map(|(name, _)| name)
        .collect();
    assert_eq!(
        files_on_open,
        vec!["mvcc_vlog_0000.dat".to_string(), "prep_vlog_0000.dat".to_string(), "vlog_seg_0000.dat".to_string()]
    );

    let txn1 = make_txn(vec![], vec![("x", Some("v1"))]);
    let txn1_id = test_txn_id(1, 1);
    store.register_prepare(txn1_id, &txn1, test_ts(5));

    store
        .commit_transaction_data(
            txn1_id,
            &[],
            &[("x".to_string(), Some("v1".to_string()))],
            &[],
            test_ts(5),
        )
        .unwrap();

    let files_before_first_seal: Vec<String> = store
        .list_dir_files()
        .into_iter()
        .map(|(name, _)| name)
        .collect();
    assert_eq!(
        files_before_first_seal,
        vec!["mvcc_vlog_0000.dat".to_string(), "prep_vlog_0000.dat".to_string(), "vlog_seg_0000.dat".to_string()]
    );

    store.seal(u64::MAX).unwrap();

    let files_after_first_seal = store.list_dir_files();
    let names_after_first: Vec<&str> = files_after_first_seal.iter().map(|(n, _)| n.as_str()).collect();
    assert_eq!(
        names_after_first,
        vec!["UNIFIED_MANIFEST", "mvcc_vlog_0000.dat", "prep_sst_0000.db", "prep_vlog_0000.dat", "sst_0000.db", "vlog_seg_0000.dat"],
        "exact files after first seal"
    );
    for (name, size) in &files_after_first_seal {
        // prep_vlog/mvcc_vlog may be empty if all prepared txns were committed before seal
        // or MVCC VlogLsm is not sealed yet.
        if name.contains("prep_vlog") || name.contains("mvcc_vlog") {
            continue;
        }
        assert!(*size > 0, "persisted file {name:?} should be non-empty after first seal");
    }

    let txn3 = make_txn(vec![], vec![("y", Some("v3"))]);
    let txn3_id = test_txn_id(2, 1);
    store.register_prepare(txn3_id, &txn3, test_ts(7));
    store
        .commit_transaction_data(
            txn3_id,
            &[],
            &[("y".to_string(), Some("v3".to_string()))],
            &[],
            test_ts(7),
        )
        .unwrap();
    store.seal(1).unwrap();

    let files_after_second_seal = store.list_dir_files();
    let names_after_second: Vec<&str> = files_after_second_seal.iter().map(|(n, _)| n.as_str()).collect();
    assert!(
        names_after_second.contains(&"vlog_seg_0001.dat"),
        "second segment file should be created after threshold exceed"
    );

    let reopened = tapir::store::TapirStateRunner::open(path.clone(), io_flags).unwrap();

    let (value, write_ts) = reopened.get_at("x", test_ts(100)).unwrap();
    assert_eq!(value.as_deref(), Some("v1"));
    assert_eq!(write_ts, test_ts(5));

    let scan = reopened.scan("a", "z", test_ts(100)).unwrap();
    assert_eq!(scan.len(), 2);
    assert_eq!(scan[0].0, "x");
    assert_eq!(scan[0].1.as_deref(), Some("v1"));
    assert_eq!(scan[0].2, test_ts(5));
    assert_eq!(scan[1].0, "y");
    assert_eq!(scan[1].1.as_deref(), Some("v3"));
    assert_eq!(scan[1].2, test_ts(7));
}

#[test]
fn prepared_txn_recovered_in_index_after_reopen() {
    let path = MemoryIo::temp_path();
    let io_flags = OpenFlags {
        create: true,
        direct: false,
    };
    let mut store = tapir::store::TapirStateRunner::open(path.clone(), io_flags).unwrap();

    // Prepare txn1 (not committed — should survive in prepared index after reopen)
    let txn1 = make_txn(vec![], vec![("z", Some("v_z"))]);
    let txn1_id = test_txn_id(3, 1);
    store.register_prepare(txn1_id, &txn1, test_ts(10));

    // Prepare+commit txn2 (committed — removed from prepared before seal)
    let txn2 = make_txn(vec![], vec![("a", Some("v_a"))]);
    let txn2_id = test_txn_id(4, 1);
    store.register_prepare(txn2_id, &txn2, test_ts(5));
    store
        .commit_transaction_data(
            txn2_id,
            &[],
            &[("a".to_string(), Some("v_a".to_string()))],
            &[],
            test_ts(5),
        )
        .unwrap();

    // Seal flushes prepared memtable (txn1) and committed memtable (txn2) to vlog
    store.seal(u64::MAX).unwrap();

    // Reopen
    let reopened = tapir::store::TapirStateRunner::open(path, io_flags).unwrap();

    // txn1 should be recovered in prepared index
    assert!(
        reopened.prepared_index_contains(&txn1_id),
        "Prepared-but-not-committed txn should be in prepared index after reopen"
    );

    // Committed data still readable
    let (value, write_ts) = reopened.get_at("a", test_ts(100)).unwrap();
    assert_eq!(value.as_deref(), Some("v_a"));
    assert_eq!(write_ts, test_ts(5));
}

/// Same-view conflict: two prepares writing the same key in the same view
/// (both in memtable) should conflict.
#[test]
fn prepare_conflict_same_view_memtable() {
    let path = MemoryIo::temp_path();
    let io_flags = OpenFlags {
        create: true,
        direct: false,
    };
    let mut store = tapir::store::TapirStateRunner::open(path, io_flags).unwrap();

    // Prepare txn_a writing key "x"
    let txn_a = make_txn(vec![], vec![("x", Some("v1"))]);
    let txn_a_id = test_txn_id(1, 1);
    store.register_prepare(txn_a_id, &txn_a, test_ts(5));

    // Prepare txn_b also writing key "x" → should conflict
    let txn_b = make_txn(vec![], vec![("x", Some("v2"))]);
    let txn_b_id = test_txn_id(2, 1);
    assert!(
        store.register_prepare_expect_conflict(txn_b_id, &txn_b, test_ts(6)),
        "Second prepare writing same key should conflict (memtable scenario)"
    );

    // Prepare txn_c writing a different key "y" → should succeed
    let txn_c = make_txn(vec![], vec![("y", Some("v3"))]);
    let txn_c_id = test_txn_id(3, 1);
    store.register_prepare(txn_c_id, &txn_c, test_ts(7));
}

/// Cross-view conflict: prepare in view 0 (sealed to vlog+index), then
/// prepare in view 1 writing the same key should conflict.
#[test]
fn prepare_conflict_cross_view_vlog() {
    let path = MemoryIo::temp_path();
    let io_flags = OpenFlags {
        create: true,
        direct: false,
    };
    let mut store = tapir::store::TapirStateRunner::open(path, io_flags).unwrap();

    // View 0: Prepare txn_c writing key "y"
    let txn_c = make_txn(vec![], vec![("y", Some("v1"))]);
    let txn_c_id = test_txn_id(5, 1);
    store.register_prepare(txn_c_id, &txn_c, test_ts(10));

    // Seal → txn_c moves from memtable to prepared vlog + index
    store.seal(u64::MAX).unwrap();

    // View 1: Prepare txn_d also writing key "y" → should conflict (from index/vlog)
    let txn_d = make_txn(vec![], vec![("y", Some("v2"))]);
    let txn_d_id = test_txn_id(6, 1);
    assert!(
        store.register_prepare_expect_conflict(txn_d_id, &txn_d, test_ts(11)),
        "Prepare writing same key as sealed prepared txn should conflict (vlog scenario)"
    );

    // Prepare txn_e writing a different key "z" → should succeed
    let txn_e = make_txn(vec![], vec![("z", Some("v3"))]);
    let txn_e_id = test_txn_id(7, 1);
    store.register_prepare(txn_e_id, &txn_e, test_ts(12));
}
