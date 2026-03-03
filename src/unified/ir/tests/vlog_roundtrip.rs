use super::helpers::*;
use crate::mvcc::disk::disk_io::OpenFlags;
use crate::mvcc::disk::memory_io::MemoryIo;
use crate::unified::ir::record::{IrPayloadInline, PrepareRef, VlogEntryType};
use crate::unified::ir::store;
use crate::unified::wisckeylsm::vlog::UnifiedVlogSegment;

struct LocalCachedPrepare {
    transaction_id: crate::occ::TransactionId,
    commit_ts: crate::tapir::Timestamp,
    read_set: Vec<(String, crate::tapir::Timestamp)>,
    write_set: Vec<(String, Option<String>)>,
    scan_set: Vec<(String, String, crate::tapir::Timestamp)>,
}

#[test]
fn vlog_entry_roundtrip_prepare() {
    let path = MemoryIo::temp_path().join("test_vlog.dat");
    MemoryIo::create_dir_all(path.parent().unwrap()).unwrap();

    use crate::mvcc::disk::disk_io::DiskIo;
    MemoryIo::create_dir_all(path.parent().unwrap()).unwrap();

    let flags = OpenFlags {
        create: true,
        direct: false,
    };

    let mut seg = UnifiedVlogSegment::<MemoryIo>::open(0, path, flags).unwrap();

    let op_id = test_op_id(1, 1);
    let payload = IrPayloadInline::<String, String>::Prepare {
        transaction_id: test_txn_id(1, 1),
        commit_ts: test_ts(5),
        read_set: vec![("key_a".to_string(), test_ts(3))],
        write_set: vec![("key_a".to_string(), Some("value_a".to_string()))],
        scan_set: vec![],
    };

    let ptr = store::append_entry(&mut seg, op_id, VlogEntryType::Prepare, &payload).unwrap();

    assert_eq!(ptr.segment_id, 0);
    assert_eq!(ptr.offset, 0);
    assert!(ptr.length > 0);

    // Read it back
    let (read_op_id, read_type, read_payload) = store::read_entry::<String, String, _>(&seg, &ptr).unwrap();
    assert_eq!(read_op_id, op_id);
    assert_eq!(read_type, VlogEntryType::Prepare);

    match read_payload {
        IrPayloadInline::Prepare {
            transaction_id,
            commit_ts,
            read_set,
            write_set,
            scan_set,
        } => {
            assert_eq!(transaction_id, test_txn_id(1, 1));
            assert_eq!(commit_ts, test_ts(5));
            assert_eq!(read_set.len(), 1);
            assert_eq!(read_set[0].0, "key_a");
            assert_eq!(write_set.len(), 1);
            assert_eq!(write_set[0].0, "key_a");
            assert_eq!(write_set[0].1, Some("value_a".to_string()));
            assert!(scan_set.is_empty());
        }
        _ => panic!("expected Prepare payload"),
    }
}

#[test]
fn vlog_entry_roundtrip_commit() {
    let path = MemoryIo::temp_path().join("test_vlog2.dat");

    use crate::mvcc::disk::disk_io::DiskIo;
    MemoryIo::create_dir_all(path.parent().unwrap()).unwrap();

    let flags = OpenFlags {
        create: true,
        direct: false,
    };

    let mut seg = UnifiedVlogSegment::<MemoryIo>::open(0, path, flags).unwrap();

    let prepare_op_id = test_op_id(1, 1);
    let commit_op_id = test_op_id(1, 2);

    let payload = IrPayloadInline::<String, String>::Commit {
        transaction_id: test_txn_id(1, 1),
        commit_ts: test_ts(5),
        prepare_ref: PrepareRef::SameView(prepare_op_id),
    };

    let ptr = store::append_entry(&mut seg, commit_op_id, VlogEntryType::Commit, &payload).unwrap();

    let (read_op_id, read_type, read_payload) = store::read_entry::<String, String, _>(&seg, &ptr).unwrap();
    assert_eq!(read_op_id, commit_op_id);
    assert_eq!(read_type, VlogEntryType::Commit);

    match read_payload {
        IrPayloadInline::Commit {
            transaction_id,
            commit_ts,
            prepare_ref,
        } => {
            assert_eq!(transaction_id, test_txn_id(1, 1));
            assert_eq!(commit_ts, test_ts(5));
            match prepare_ref {
                PrepareRef::SameView(op) => assert_eq!(op, prepare_op_id),
                _ => panic!("expected SameView"),
            }
        }
        _ => panic!("expected Commit payload"),
    }
}

#[test]
fn vlog_batch_append() {
    let path = MemoryIo::temp_path().join("test_vlog3.dat");

    use crate::mvcc::disk::disk_io::DiskIo;
    MemoryIo::create_dir_all(path.parent().unwrap()).unwrap();

    let flags = OpenFlags {
        create: true,
        direct: false,
    };

    let mut seg = UnifiedVlogSegment::<MemoryIo>::open(0, path, flags).unwrap();

    let payload1 = IrPayloadInline::<String, String>::Prepare {
        transaction_id: test_txn_id(1, 1),
        commit_ts: test_ts(5),
        read_set: vec![],
        write_set: vec![("x".to_string(), Some("v1".to_string()))],
        scan_set: vec![],
    };

    let payload2 = IrPayloadInline::<String, String>::Commit {
        transaction_id: test_txn_id(1, 1),
        commit_ts: test_ts(5),
        prepare_ref: PrepareRef::SameView(test_op_id(1, 1)),
    };

    let entries = vec![
        (test_op_id(1, 1), VlogEntryType::Prepare, &payload1),
        (test_op_id(1, 2), VlogEntryType::Commit, &payload2),
    ];

    let ptrs = store::append_batch(&mut seg, &entries).unwrap();
    assert_eq!(ptrs.len(), 2);
    assert_eq!(ptrs[0].offset, 0);
    assert!(ptrs[1].offset > 0);

    // Read both back
    let (_, t1, _) = store::read_entry::<String, String, _>(&seg, &ptrs[0]).unwrap();
    assert_eq!(t1, VlogEntryType::Prepare);

    let (_, t2, _) = store::read_entry::<String, String, _>(&seg, &ptrs[1]).unwrap();
    assert_eq!(t2, VlogEntryType::Commit);
}

#[test]
fn vlog_read_prepare_helper() {
    let path = MemoryIo::temp_path().join("test_vlog4.dat");

    use crate::mvcc::disk::disk_io::DiskIo;
    MemoryIo::create_dir_all(path.parent().unwrap()).unwrap();

    let flags = OpenFlags {
        create: true,
        direct: false,
    };

    let mut seg = UnifiedVlogSegment::<MemoryIo>::open(0, path, flags).unwrap();

    let payload = IrPayloadInline::<String, String>::Prepare {
        transaction_id: test_txn_id(2, 3),
        commit_ts: test_ts(10),
        read_set: vec![("r1".to_string(), test_ts(1))],
        write_set: vec![
            ("w1".to_string(), Some("val1".to_string())),
            ("w2".to_string(), Some("val2".to_string())),
        ],
        scan_set: vec![("s1".to_string(), "s2".to_string(), test_ts(5))],
    };

    let ptr = store::append_entry(&mut seg, test_op_id(2, 1), VlogEntryType::Prepare, &payload).unwrap();

    let (_, _, read_payload) = store::read_entry::<String, String, _>(&seg, &ptr).unwrap();
    let cached = match read_payload {
        IrPayloadInline::Prepare {
            transaction_id,
            commit_ts,
            read_set,
            write_set,
            scan_set,
        } => LocalCachedPrepare {
            transaction_id,
            commit_ts,
            read_set,
            write_set,
            scan_set,
        },
        _ => panic!("expected Prepare payload"),
    };
    assert_eq!(cached.transaction_id, test_txn_id(2, 3));
    assert_eq!(cached.commit_ts, test_ts(10));
    assert_eq!(cached.read_set.len(), 1);
    assert_eq!(cached.write_set.len(), 2);
    assert_eq!(cached.write_set[0].0, "w1");
    assert_eq!(cached.write_set[0].1, Some("val1".to_string()));
    assert_eq!(cached.write_set[1].0, "w2");
    assert_eq!(cached.write_set[1].1, Some("val2".to_string()));
    assert_eq!(cached.scan_set.len(), 1);
}

#[test]
fn vlog_all_entry_types_roundtrip() {
    let path = MemoryIo::temp_path().join("test_vlog5.dat");

    use crate::mvcc::disk::disk_io::DiskIo;
    MemoryIo::create_dir_all(path.parent().unwrap()).unwrap();

    let flags = OpenFlags {
        create: true,
        direct: false,
    };

    let mut seg = UnifiedVlogSegment::<MemoryIo>::open(0, path, flags).unwrap();

    // Abort
    let abort_payload = IrPayloadInline::<String, String>::Abort {
        transaction_id: test_txn_id(1, 1),
        commit_ts: Some(test_ts(5)),
    };
    let ptr = store::append_entry(&mut seg, test_op_id(1, 1), VlogEntryType::Abort, &abort_payload).unwrap();
    let (_, t, p) = store::read_entry::<String, String, _>(&seg, &ptr).unwrap();
    assert_eq!(t, VlogEntryType::Abort);
    match p {
        IrPayloadInline::Abort { transaction_id, commit_ts } => {
            assert_eq!(transaction_id, test_txn_id(1, 1));
            assert_eq!(commit_ts, Some(test_ts(5)));
        }
        _ => panic!("wrong payload type"),
    }

    // QuorumRead
    let qr_payload = IrPayloadInline::<String, String>::QuorumRead {
        key: "mykey".to_string(),
        timestamp: test_ts(7),
    };
    let ptr = store::append_entry(&mut seg, test_op_id(1, 2), VlogEntryType::QuorumRead, &qr_payload).unwrap();
    let (_, t, p) = store::read_entry::<String, String, _>(&seg, &ptr).unwrap();
    assert_eq!(t, VlogEntryType::QuorumRead);
    match p {
        IrPayloadInline::QuorumRead { key, timestamp } => {
            assert_eq!(key, "mykey");
            assert_eq!(timestamp, test_ts(7));
        }
        _ => panic!("wrong payload type"),
    }

    // QuorumScan
    let qs_payload = IrPayloadInline::<String, String>::QuorumScan {
        start_key: "a".to_string(),
        end_key: "z".to_string(),
        snapshot_ts: test_ts(15),
    };
    let ptr = store::append_entry(&mut seg, test_op_id(1, 3), VlogEntryType::QuorumScan, &qs_payload).unwrap();
    let (_, t, p) = store::read_entry::<String, String, _>(&seg, &ptr).unwrap();
    assert_eq!(t, VlogEntryType::QuorumScan);
    match p {
        IrPayloadInline::QuorumScan { start_key, end_key, snapshot_ts } => {
            assert_eq!(start_key, "a");
            assert_eq!(end_key, "z");
            assert_eq!(snapshot_ts, test_ts(15));
        }
        _ => panic!("wrong payload type"),
    }

    // RaiseMinPrepareTime
    let rmpt_payload = IrPayloadInline::<String, String>::RaiseMinPrepareTime { time: 42 };
    let ptr = store::append_entry(
        &mut seg,
        test_op_id(1, 4),
        VlogEntryType::RaiseMinPrepareTime,
        &rmpt_payload,
    )
    .unwrap();
    let (_, t, p) = store::read_entry::<String, String, _>(&seg, &ptr).unwrap();
    assert_eq!(t, VlogEntryType::RaiseMinPrepareTime);
    match p {
        IrPayloadInline::RaiseMinPrepareTime { time } => {
            assert_eq!(time, 42);
        }
        _ => panic!("wrong payload type"),
    }
}
