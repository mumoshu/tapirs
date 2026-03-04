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
        vec!["prep_vlog_0000.dat".to_string(), "vlog_seg_0000.dat".to_string()]
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
        vec!["prep_vlog_0000.dat".to_string(), "vlog_seg_0000.dat".to_string()]
    );

    store.seal(u64::MAX).unwrap();

    let files_after_first_seal = store.list_dir_files();
    let names_after_first: Vec<&str> = files_after_first_seal.iter().map(|(n, _)| n.as_str()).collect();
    assert_eq!(
        names_after_first,
        vec!["UNIFIED_MANIFEST", "prep_vlog_0000.dat", "vlog_seg_0000.dat"],
        "exact files after first seal"
    );
    for (name, size) in &files_after_first_seal {
        // prep_vlog may be empty if all prepared txns were committed before seal.
        if name.contains("prep_vlog") {
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
