use super::helpers::*;
use crate::mvcc::disk::disk_io::OpenFlags;
use crate::mvcc::disk::memory_io::MemoryIo;
use crate::unified::types::*;
use crate::unified::vlog::UnifiedVlogSegment;

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
    let payload = IrPayloadInline::Prepare {
        transaction_id: test_txn_id(1, 1),
        commit_ts: test_ts(5),
        read_set: vec![(b"key_a".to_vec(), test_ts(3))],
        write_set: vec![(b"key_a".to_vec(), b"value_a".to_vec())],
        scan_set: vec![],
    };

    let ptr = seg
        .append_entry(op_id, VlogEntryType::Prepare, &payload)
        .unwrap();

    assert_eq!(ptr.segment_id, 0);
    assert_eq!(ptr.offset, 0);
    assert!(ptr.length > 0);

    // Read it back
    let (read_op_id, read_type, read_payload) = seg.read_entry(&ptr).unwrap();
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
            assert_eq!(read_set[0].0, b"key_a");
            assert_eq!(write_set.len(), 1);
            assert_eq!(write_set[0].0, b"key_a");
            assert_eq!(write_set[0].1, b"value_a");
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

    let payload = IrPayloadInline::Commit {
        transaction_id: test_txn_id(1, 1),
        commit_ts: test_ts(5),
        prepare_ref: PrepareRef::SameView(prepare_op_id),
    };

    let ptr = seg
        .append_entry(commit_op_id, VlogEntryType::Commit, &payload)
        .unwrap();

    let (read_op_id, read_type, read_payload) = seg.read_entry(&ptr).unwrap();
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

    let payload1 = IrPayloadInline::Prepare {
        transaction_id: test_txn_id(1, 1),
        commit_ts: test_ts(5),
        read_set: vec![],
        write_set: vec![(b"x".to_vec(), b"v1".to_vec())],
        scan_set: vec![],
    };

    let payload2 = IrPayloadInline::Commit {
        transaction_id: test_txn_id(1, 1),
        commit_ts: test_ts(5),
        prepare_ref: PrepareRef::SameView(test_op_id(1, 1)),
    };

    let entries = vec![
        (test_op_id(1, 1), VlogEntryType::Prepare, &payload1),
        (test_op_id(1, 2), VlogEntryType::Commit, &payload2),
    ];

    let ptrs = seg.append_batch(&entries).unwrap();
    assert_eq!(ptrs.len(), 2);
    assert_eq!(ptrs[0].offset, 0);
    assert!(ptrs[1].offset > 0);

    // Read both back
    let (_, t1, _) = seg.read_entry(&ptrs[0]).unwrap();
    assert_eq!(t1, VlogEntryType::Prepare);

    let (_, t2, _) = seg.read_entry(&ptrs[1]).unwrap();
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

    let payload = IrPayloadInline::Prepare {
        transaction_id: test_txn_id(2, 3),
        commit_ts: test_ts(10),
        read_set: vec![(b"r1".to_vec(), test_ts(1))],
        write_set: vec![
            (b"w1".to_vec(), b"val1".to_vec()),
            (b"w2".to_vec(), b"val2".to_vec()),
        ],
        scan_set: vec![(b"s1".to_vec(), b"s2".to_vec(), test_ts(5))],
    };

    let ptr = seg
        .append_entry(test_op_id(2, 1), VlogEntryType::Prepare, &payload)
        .unwrap();

    let cached = seg.read_prepare(&ptr).unwrap();
    assert_eq!(cached.transaction_id, test_txn_id(2, 3));
    assert_eq!(cached.commit_ts, test_ts(10));
    assert_eq!(cached.read_set.len(), 1);
    assert_eq!(cached.write_set.len(), 2);
    assert_eq!(cached.write_set[0].0, b"w1");
    assert_eq!(cached.write_set[0].1, b"val1");
    assert_eq!(cached.write_set[1].0, b"w2");
    assert_eq!(cached.write_set[1].1, b"val2");
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
    let abort_payload = IrPayloadInline::Abort {
        transaction_id: test_txn_id(1, 1),
        commit_ts: Some(test_ts(5)),
    };
    let ptr = seg
        .append_entry(test_op_id(1, 1), VlogEntryType::Abort, &abort_payload)
        .unwrap();
    let (_, t, p) = seg.read_entry(&ptr).unwrap();
    assert_eq!(t, VlogEntryType::Abort);
    match p {
        IrPayloadInline::Abort { transaction_id, commit_ts } => {
            assert_eq!(transaction_id, test_txn_id(1, 1));
            assert_eq!(commit_ts, Some(test_ts(5)));
        }
        _ => panic!("wrong payload type"),
    }

    // QuorumRead
    let qr_payload = IrPayloadInline::QuorumRead {
        key: b"mykey".to_vec(),
        timestamp: test_ts(7),
    };
    let ptr = seg
        .append_entry(test_op_id(1, 2), VlogEntryType::QuorumRead, &qr_payload)
        .unwrap();
    let (_, t, p) = seg.read_entry(&ptr).unwrap();
    assert_eq!(t, VlogEntryType::QuorumRead);
    match p {
        IrPayloadInline::QuorumRead { key, timestamp } => {
            assert_eq!(key, b"mykey");
            assert_eq!(timestamp, test_ts(7));
        }
        _ => panic!("wrong payload type"),
    }

    // QuorumScan
    let qs_payload = IrPayloadInline::QuorumScan {
        start_key: b"a".to_vec(),
        end_key: b"z".to_vec(),
        snapshot_ts: test_ts(15),
    };
    let ptr = seg
        .append_entry(test_op_id(1, 3), VlogEntryType::QuorumScan, &qs_payload)
        .unwrap();
    let (_, t, p) = seg.read_entry(&ptr).unwrap();
    assert_eq!(t, VlogEntryType::QuorumScan);
    match p {
        IrPayloadInline::QuorumScan { start_key, end_key, snapshot_ts } => {
            assert_eq!(start_key, b"a");
            assert_eq!(end_key, b"z");
            assert_eq!(snapshot_ts, test_ts(15));
        }
        _ => panic!("wrong payload type"),
    }

    // RaiseMinPrepareTime
    let rmpt_payload = IrPayloadInline::RaiseMinPrepareTime { time: 42 };
    let ptr = seg
        .append_entry(
            test_op_id(1, 4),
            VlogEntryType::RaiseMinPrepareTime,
            &rmpt_payload,
        )
        .unwrap();
    let (_, t, p) = seg.read_entry(&ptr).unwrap();
    assert_eq!(t, VlogEntryType::RaiseMinPrepareTime);
    match p {
        IrPayloadInline::RaiseMinPrepareTime { time } => {
            assert_eq!(time, 42);
        }
        _ => panic!("wrong payload type"),
    }
}
