use super::helpers::*;
use crate::ir::OpId;
use crate::storage::io::disk_io::{DiskIo, OpenFlags};
use crate::storage::io::memory_io::MemoryIo;
use crate::unified::ir::record::{
    IrMemEntry, IrPayloadInline, IrState, PrepareRef, VlogEntryType,
};
use crate::unified::ir::store;
use crate::unified::wisckeylsm::lsm::{IndexMode, VlogLsm};
use crate::unified::wisckeylsm::vlog::VlogSegment;
use std::collections::BTreeMap;

fn test_flags() -> OpenFlags {
    OpenFlags {
        create: true,
        direct: false,
    }
}

fn open_ir_lsm(dir: &std::path::Path) -> VlogLsm<OpId, IrMemEntry<String, String>, MemoryIo> {
    MemoryIo::create_dir_all(dir).unwrap();
    let vlog_path = dir.join("ir_vlog_0000.dat");
    let mut active_vlog = VlogSegment::<MemoryIo>::open(0, vlog_path, test_flags()).unwrap();
    active_vlog.start_view(0);
    VlogLsm::open_from_parts(
        "ir",
        dir,
        active_vlog,
        BTreeMap::new(),
        test_flags(),
        1,
        Vec::new(),
        0,
        IndexMode::InMemory,
    )
    .unwrap()
}

fn ir_header_fn(
    op_id: &OpId,
    entry: &IrMemEntry<String, String>,
) -> Option<(u8, u64, u64)> {
    if entry.state.is_finalized() {
        Some((entry.entry_type as u8, op_id.client_id.0, op_id.number))
    } else {
        None
    }
}

#[test]
fn vlog_entry_roundtrip_prepare() {
    let dir = MemoryIo::temp_path();
    let mut lsm = open_ir_lsm(&dir);

    let op_id = test_op_id(1, 1);
    let entry = IrMemEntry {
        entry_type: VlogEntryType::Prepare,
        state: IrState::Finalized(0),
        payload: IrPayloadInline::<String, String>::Prepare {
            transaction_id: test_txn_id(1, 1),
            commit_ts: test_ts(5),
            read_set: vec![("key_a".to_string(), test_ts(3))],
            write_set: vec![("key_a".to_string(), Some("value_a".to_string()))],
            scan_set: vec![],
        },
    };

    lsm.put(op_id, entry);
    lsm.seal_view(0, ir_header_fn).unwrap();

    // Read back via VlogPtr in index.
    let ptr = lsm.index().get(&op_id).expect("op_id should be in index");
    let seg = lsm.segment_ref(ptr.segment_id).unwrap();
    let (read_op_id, read_entry) = store::read_entry::<String, String, _>(seg, ptr).unwrap();
    assert_eq!(read_op_id, op_id);
    assert_eq!(read_entry.entry_type, VlogEntryType::Prepare);

    match read_entry.payload {
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
    let dir = MemoryIo::temp_path();
    let mut lsm = open_ir_lsm(&dir);

    let prepare_op_id = test_op_id(1, 1);
    let commit_op_id = test_op_id(1, 2);

    let entry = IrMemEntry {
        entry_type: VlogEntryType::Commit,
        state: IrState::Finalized(0),
        payload: IrPayloadInline::<String, String>::Commit {
            transaction_id: test_txn_id(1, 1),
            commit_ts: test_ts(5),
            prepare_ref: PrepareRef::SameView(prepare_op_id),
        },
    };

    lsm.put(commit_op_id, entry);
    lsm.seal_view(0, ir_header_fn).unwrap();

    let ptr = lsm.index().get(&commit_op_id).expect("should be in index");
    let seg = lsm.segment_ref(ptr.segment_id).unwrap();
    let (read_op_id, read_entry) = store::read_entry::<String, String, _>(seg, ptr).unwrap();
    assert_eq!(read_op_id, commit_op_id);
    assert_eq!(read_entry.entry_type, VlogEntryType::Commit);

    match read_entry.payload {
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
fn vlog_batch_put_and_seal() {
    let dir = MemoryIo::temp_path();
    let mut lsm = open_ir_lsm(&dir);

    let entry1 = IrMemEntry {
        entry_type: VlogEntryType::Prepare,
        state: IrState::Finalized(0),
        payload: IrPayloadInline::<String, String>::Prepare {
            transaction_id: test_txn_id(1, 1),
            commit_ts: test_ts(5),
            read_set: vec![],
            write_set: vec![("x".to_string(), Some("v1".to_string()))],
            scan_set: vec![],
        },
    };

    let entry2 = IrMemEntry {
        entry_type: VlogEntryType::Commit,
        state: IrState::Finalized(0),
        payload: IrPayloadInline::<String, String>::Commit {
            transaction_id: test_txn_id(1, 1),
            commit_ts: test_ts(5),
            prepare_ref: PrepareRef::SameView(test_op_id(1, 1)),
        },
    };

    lsm.put(test_op_id(1, 1), entry1);
    lsm.put(test_op_id(1, 2), entry2);
    lsm.seal_view(0, ir_header_fn).unwrap();

    assert_eq!(lsm.index().len(), 2);

    let ptr1 = lsm.index().get(&test_op_id(1, 1)).unwrap();
    let ptr2 = lsm.index().get(&test_op_id(1, 2)).unwrap();
    assert_eq!(ptr1.offset, 0);
    assert!(ptr2.offset > 0);

    let seg = lsm.segment_ref(ptr1.segment_id).unwrap();
    let (_, e1) = store::read_entry::<String, String, _>(seg, ptr1).unwrap();
    assert_eq!(e1.entry_type, VlogEntryType::Prepare);

    let (_, e2) = store::read_entry::<String, String, _>(seg, ptr2).unwrap();
    assert_eq!(e2.entry_type, VlogEntryType::Commit);
}

#[test]
fn tentative_entries_discarded_at_seal() {
    let dir = MemoryIo::temp_path();
    let mut lsm = open_ir_lsm(&dir);

    // Finalized entry
    let finalized = IrMemEntry {
        entry_type: VlogEntryType::Prepare,
        state: IrState::Finalized(0),
        payload: IrPayloadInline::<String, String>::Prepare {
            transaction_id: test_txn_id(1, 1),
            commit_ts: test_ts(5),
            read_set: vec![],
            write_set: vec![("x".to_string(), Some("v1".to_string()))],
            scan_set: vec![],
        },
    };

    // Tentative entry
    let tentative = IrMemEntry {
        entry_type: VlogEntryType::Prepare,
        state: IrState::Tentative,
        payload: IrPayloadInline::<String, String>::Prepare {
            transaction_id: test_txn_id(2, 1),
            commit_ts: test_ts(10),
            read_set: vec![],
            write_set: vec![("y".to_string(), Some("v2".to_string()))],
            scan_set: vec![],
        },
    };

    lsm.put(test_op_id(1, 1), finalized);
    lsm.put(test_op_id(2, 1), tentative);
    lsm.seal_view(0, ir_header_fn).unwrap();

    // Only finalized entry should be in the index.
    assert_eq!(lsm.index().len(), 1);
    assert!(lsm.index().contains_key(&test_op_id(1, 1)));
    assert!(!lsm.index().contains_key(&test_op_id(2, 1)));
}

#[test]
fn vlog_all_entry_types_roundtrip() {
    let dir = MemoryIo::temp_path();
    let mut lsm = open_ir_lsm(&dir);

    // Abort
    lsm.put(
        test_op_id(1, 1),
        IrMemEntry {
            entry_type: VlogEntryType::Abort,
            state: IrState::Finalized(0),
            payload: IrPayloadInline::<String, String>::Abort {
                transaction_id: test_txn_id(1, 1),
                commit_ts: Some(test_ts(5)),
            },
        },
    );

    // QuorumRead
    lsm.put(
        test_op_id(1, 2),
        IrMemEntry {
            entry_type: VlogEntryType::QuorumRead,
            state: IrState::Finalized(0),
            payload: IrPayloadInline::<String, String>::QuorumRead {
                key: "mykey".to_string(),
                timestamp: test_ts(7),
            },
        },
    );

    // QuorumScan
    lsm.put(
        test_op_id(1, 3),
        IrMemEntry {
            entry_type: VlogEntryType::QuorumScan,
            state: IrState::Finalized(0),
            payload: IrPayloadInline::<String, String>::QuorumScan {
                start_key: "a".to_string(),
                end_key: "z".to_string(),
                snapshot_ts: test_ts(15),
            },
        },
    );

    // RaiseMinPrepareTime
    lsm.put(
        test_op_id(1, 4),
        IrMemEntry {
            entry_type: VlogEntryType::RaiseMinPrepareTime,
            state: IrState::Finalized(0),
            payload: IrPayloadInline::<String, String>::RaiseMinPrepareTime { time: 42 },
        },
    );

    lsm.seal_view(0, ir_header_fn).unwrap();

    assert_eq!(lsm.index().len(), 4);

    // Verify each entry type reads back correctly.
    let check = |op_id: OpId, expected_type: VlogEntryType| {
        let ptr = lsm.index().get(&op_id).unwrap();
        let seg = lsm.segment_ref(ptr.segment_id).unwrap();
        let (_, entry) = store::read_entry::<String, String, _>(seg, ptr).unwrap();
        assert_eq!(entry.entry_type, expected_type);
        entry
    };

    let abort = check(test_op_id(1, 1), VlogEntryType::Abort);
    match abort.payload {
        IrPayloadInline::Abort { transaction_id, commit_ts } => {
            assert_eq!(transaction_id, test_txn_id(1, 1));
            assert_eq!(commit_ts, Some(test_ts(5)));
        }
        _ => panic!("wrong payload type"),
    }

    let qr = check(test_op_id(1, 2), VlogEntryType::QuorumRead);
    match qr.payload {
        IrPayloadInline::QuorumRead { key, timestamp } => {
            assert_eq!(key, "mykey");
            assert_eq!(timestamp, test_ts(7));
        }
        _ => panic!("wrong payload type"),
    }

    let qs = check(test_op_id(1, 3), VlogEntryType::QuorumScan);
    match qs.payload {
        IrPayloadInline::QuorumScan { start_key, end_key, snapshot_ts } => {
            assert_eq!(start_key, "a");
            assert_eq!(end_key, "z");
            assert_eq!(snapshot_ts, test_ts(15));
        }
        _ => panic!("wrong payload type"),
    }

    let rmpt = check(test_op_id(1, 4), VlogEntryType::RaiseMinPrepareTime);
    match rmpt.payload {
        IrPayloadInline::RaiseMinPrepareTime { time } => {
            assert_eq!(time, 42);
        }
        _ => panic!("wrong payload type"),
    }
}
