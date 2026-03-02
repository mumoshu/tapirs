use super::helpers::*;
use crate::tapirstore::TapirStore;
use crate::unified::types::*;

// === Test 3: View Change (seal + merge + MVCC reads across views) ===

#[test]
fn view_change_seal_merge_sync() {
    let mut store = new_test_store();
    assert_current_view(&store, 0);

    // Fresh store: only active VLog segment
    assert_store_file_names(&store, &["vlog_seg_0000.dat"]);
    assert_store_file_size(&store, "vlog_seg_0000.dat", 0);

    // Phase 1: View 0 operations
    prepare_and_commit(
        &mut store,
        test_op_id(0, 1),
        test_op_id(0, 2),
        test_txn_id(0, 1),
        vec![("a", Some("v1"))],
        test_ts(5),
    );

    // Committed value points to committed-transaction VLog entry
    assert_value_location_in_memory(&store, "a", test_ts(5), false);

    // Tentative prepare (won't survive merge)
    let txn2 = make_txn(vec![], vec![("b", Some("v2"))]);
    prepare_txn(
        &mut store,
        test_op_id(0, 3),
        test_txn_id(0, 2),
        txn2,
        test_ts(10),
        false,
    );

    // Phase 2: Seal view 0 → view 1
    seal_view(&mut store);
    assert_current_view(&store, 1);

    // After seal: manifest saved, VLog has data
    assert_store_file_names(&store, &["UNIFIED_MANIFEST", "vlog_seg_0000.dat"]);
    assert_store_file_size_positive(&store, "UNIFIED_MANIFEST");
    assert_store_file_size_positive(&store, "vlog_seg_0000.dat");

    // Verify VLog read count is still 0 (no disk reads yet)
    assert_eq!(store.vlog_read_count(), 0);

    // Phase 3-4: Install merged record (tentative txn_2 dropped by leader merge)
    let merged = store.extract_finalized_entries();
    // extract_finalized_entries only returns finalized overlay entries,
    // but overlay was cleared at seal. So merged should be empty.
    assert!(
        merged.is_empty(),
        "extract_finalized_entries after seal should be empty (overlay cleared)"
    );

    store
        .install_merged_record(merged, 1)
        .unwrap();
    assert_current_view(&store, 1);

    // Phase 5: Reads across sealed views
    // Committed "a" should be readable (OnDisk after seal)
    assert_get_at(&store, "a", test_ts(5), Some("v1"), test_ts(5));

    // Tentative "b" was never committed — not in unified_memtable
    assert_get_none(&store, "b", test_ts(10));

    // Verify OnDisk location (converted from InMemory at seal time)
    assert_value_location_in_memory(&store, "a", test_ts(5), false);

    // No last_read_ts set
    assert_last_read_ts(&store, "a", None);

    // do_uncommitted_get() returns the latest version of "a"
    let (val, ts) = store.do_uncommitted_get(&"a".to_string()).unwrap();
    assert_eq!(val.as_deref(), Some("v1"), "get(a): value mismatch");
    assert_eq!(ts, test_ts(5), "get(a): timestamp mismatch");
}

// === Test 4: Cross-View (prepare in view N, commit in view N+M) ===

#[test]
fn cross_view_prepare_commit() {
    let mut store = new_test_store();

    // View 0: Prepare only (no commit yet)
    let txn = make_txn(vec![], vec![("x", Some("v1"))]);
    prepare_txn(
        &mut store,
        test_op_id(1, 1),
        test_txn_id(1, 1),
        txn.clone(),
        test_ts(5),
        true,
    );

    // Before seal: only active VLog segment
    assert_store_file_names(&store, &["vlog_seg_0000.dat"]);

    // Seal view 0 → view 1
    seal_view(&mut store);
    assert_current_view(&store, 1);

    // After seal: manifest + VLog with data
    assert_store_file_names(&store, &["UNIFIED_MANIFEST", "vlog_seg_0000.dat"]);
    assert_store_file_size_positive(&store, "UNIFIED_MANIFEST");
    assert_store_file_size_positive(&store, "vlog_seg_0000.dat");

    // VLog read count should be 0 (no reads yet)
    assert_eq!(store.vlog_read_count(), 0);

    // View 1: Commit using reconstructed transaction data from merge output.
    let ir_base_entry = store
        .lookup_ir_base_entry(test_op_id(1, 1))
        .expect("Prepare should be in IR base SST");
    assert_eq!(
        ir_base_entry.entry_type,
        VlogEntryType::Prepare,
        "IR base entry should be Prepare"
    );
    let vlog_ptr = ir_base_entry.vlog_ptr;

    store.insert_ir_entry(
        test_op_id(1, 2),
        IrMemEntry {
            entry_type: VlogEntryType::Commit,
            state: IrState::Finalized(store.current_view()),
            payload: IrPayloadInline::Commit {
                transaction_id: test_txn_id(1, 1),
                commit_ts: test_ts(5),
                prepare_ref: PrepareRef::CrossView {
                    view: 0,
                    vlog_ptr,
                },
            },
        },
    );
    store
        .commit_transaction_data(
            test_txn_id(1, 1),
            &[],
            &[("x".to_string(), Some("v1".to_string()))],
            &[],
            test_ts(5),
        )
        .unwrap();

    // Read resolves through sealed VLog (OnDisk path)
    assert_get_at(&store, "x", test_ts(5), Some("v1"), test_ts(5));

    // Value should be OnDisk (cross-view commit uses committed transaction vlog)
    assert_value_location_in_memory(&store, "x", test_ts(5), false);

    // First read should have triggered exactly 1 VLog read
    let io_after_first = store.vlog_read_count();
    assert_eq!(io_after_first, 1, "Expected exactly 1 VLog read");

    // Second read: LRU cache hit (zero additional VLog I/O)
    assert_get_at(&store, "x", test_ts(5), Some("v1"), test_ts(5));
    assert_eq!(
        store.vlog_read_count(),
        io_after_first,
        "Expected LRU cache hit (no additional VLog reads)"
    );

    // Nonexistent key should return None
    assert_get_none(&store, "y", test_ts(5));

    // No last_read_ts
    assert_last_read_ts(&store, "x", None);
}

// === Test: Multi-client merged record installation ===

#[test]
fn multi_client_merged_record() {
    let mut store = new_test_store();

    // Two clients prepare entries at different timestamps
    let txn1 = make_txn(vec![], vec![("a", Some("from_c1"))]);
    prepare_txn(
        &mut store,
        test_op_id(1, 1),
        test_txn_id(1, 1),
        txn1.clone(),
        test_ts_client(5, 1),
        true,
    );

    let txn2 = make_txn(vec![], vec![("b", Some("from_c2"))]);
    prepare_txn(
        &mut store,
        test_op_id(2, 1),
        test_txn_id(2, 1),
        txn2.clone(),
        test_ts_client(10, 2),
        true,
    );

    // Commit both
    commit_txn(
        &mut store,
        test_op_id(1, 2),
        test_txn_id(1, 1),
        test_ts_client(5, 1),
        PrepareRef::SameView(test_op_id(1, 1)),
    );
    commit_txn(
        &mut store,
        test_op_id(2, 2),
        test_txn_id(2, 1),
        test_ts_client(10, 2),
        PrepareRef::SameView(test_op_id(2, 1)),
    );

    // Both readable before seal
    assert_get_at(&store, "a", test_ts_client(5, 1), Some("from_c1"), test_ts_client(5, 1));
    assert_get_at(&store, "b", test_ts_client(10, 2), Some("from_c2"), test_ts_client(10, 2));

    // Extract finalized entries and build a merged record for view 1
    let finalized = store.extract_finalized_entries();
    assert_eq!(finalized.len(), 4, "Should have 4 finalized entries (2 prepare + 2 commit)");

    let merged = build_merged_record(finalized, 1);
    assert_eq!(merged.len(), 4);
    // All entries should be Finalized(1)
    for (_, entry) in &merged {
        assert_eq!(entry.state, IrState::Finalized(1));
    }

    // Seal to persist VLog data and convert InMemory→OnDisk
    seal_view(&mut store);
    assert_current_view(&store, 1);

    // Install merged record (simulates leader's merge)
    store.install_merged_record(merged, 1).unwrap();
    assert_current_view(&store, 1);

    // Data still readable after merge (OnDisk)
    assert_get_at(&store, "a", test_ts_client(5, 1), Some("from_c1"), test_ts_client(5, 1));
    assert_get_at(&store, "b", test_ts_client(10, 2), Some("from_c2"), test_ts_client(10, 2));

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
