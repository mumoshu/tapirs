use super::helpers::{test_op_id, test_ts, test_txn_id};
use crate::mvcc::disk::disk_io::OpenFlags;
use crate::mvcc::disk::memory_io::MemoryIo;
use crate::unified::ir;
use crate::unified::ir::record::{IrMemEntry, IrPayloadInline, IrState, PrepareRef, VlogEntryType};

fn list_dir_files(path: &std::path::Path) -> Vec<(String, usize)> {
    let files = MemoryIo::list_files(path);
    let prefix = format!("{}/", path.display());
    files
        .into_iter()
        .map(|(p, size)| {
            let name = p
                .to_string_lossy()
                .strip_prefix(&prefix)
                .unwrap_or(&p.to_string_lossy())
                .to_string();
            (name, size)
        })
        .collect()
}

#[test]
fn ir_prepare_commit_seal_reopen_and_lookup_by_id() {
    let path = MemoryIo::temp_path();
    let io_flags = OpenFlags {
        create: true,
        direct: false,
    };

    let prepare_op = test_op_id(10, 1);
    let commit_op = test_op_id(10, 2);
    let txn_id = test_txn_id(10, 42);
    let commit_ts = test_ts(55);

    let entries = vec![
        (
            prepare_op,
            IrMemEntry {
                entry_type: VlogEntryType::Prepare,
                state: IrState::Finalized(0),
                payload: IrPayloadInline::Prepare {
                    transaction_id: txn_id,
                    commit_ts,
                    read_set: vec![("x".to_string(), test_ts(1))],
                    write_set: vec![("x".to_string(), Some("v1".to_string()))],
                    scan_set: vec![("a".to_string(), "z".to_string(), test_ts(1))],
                },
            },
        ),
        (
            commit_op,
            IrMemEntry {
                entry_type: VlogEntryType::Commit,
                state: IrState::Finalized(0),
                payload: IrPayloadInline::Commit {
                    transaction_id: txn_id,
                    commit_ts,
                    prepare_ref: PrepareRef::SameView(prepare_op),
                },
            },
        ),
    ];

    assert!(
        list_dir_files(&path).is_empty(),
        "no files should exist before first seal"
    );

    let mut record = ir::store::open_store_state::<String, String, MemoryIo>(&path, io_flags).unwrap();
    for (op_id, entry) in &entries {
        ir::store::insert_ir_entry(&mut record, *op_id, entry.clone());
    }

    ir::store::seal_current_view(&mut record, u64::MAX).unwrap();

    let files_after_first = list_dir_files(&path);
    let names_after_first: Vec<&str> = files_after_first.iter().map(|(n, _)| n.as_str()).collect();
    assert_eq!(
        names_after_first,
        vec!["UNIFIED_MANIFEST", "ir_vlog_0000.dat"],
        "exact files after first seal"
    );
    for (_, size) in &files_after_first {
        assert!(*size > 0, "persisted files should be non-empty after first seal");
    }

    let second_view = record.current_view();
    ir::store::insert_ir_entry(
        &mut record,
        test_op_id(10, 3),
        IrMemEntry {
            entry_type: VlogEntryType::Prepare,
            state: IrState::Finalized(second_view),
            payload: IrPayloadInline::Prepare {
                transaction_id: test_txn_id(10, 43),
                commit_ts: test_ts(56),
                read_set: vec![],
                write_set: vec![("y".to_string(), Some("v2".to_string()))],
                scan_set: vec![],
            },
        },
    );
    ir::store::seal_current_view(&mut record, 1).unwrap();

    let files_after_second = list_dir_files(&path);
    let names_after_second: Vec<&str> = files_after_second.iter().map(|(n, _)| n.as_str()).collect();
    assert!(
        names_after_second.contains(&"ir_vlog_0001.dat"),
        "second segment file should be created after threshold exceed"
    );

    let reopened = ir::store::open_store_state::<String, String, MemoryIo>(&path, io_flags).unwrap();

    let seg0 = reopened
        .segment_ref(0)
        .expect("segment 0 should exist after reopen");
    let seg0_entries = ir::store::iter_entries::<String, String, _>(seg0).unwrap();

    let (_prepare_offset, _prepare_op, prepare_entry) = seg0_entries
        .iter()
        .find(|(_, op_id, _)| *op_id == prepare_op)
        .cloned()
        .expect("prepare op should exist in segment 0");
    assert_eq!(prepare_entry.entry_type, VlogEntryType::Prepare);
    match prepare_entry.payload {
        IrPayloadInline::Prepare {
            transaction_id,
            commit_ts: ts,
            read_set,
            write_set,
            scan_set,
        } => {
            assert_eq!(transaction_id, txn_id);
            assert_eq!(ts, commit_ts);
            assert_eq!(read_set, vec![("x".to_string(), test_ts(1))]);
            assert_eq!(write_set, vec![("x".to_string(), Some("v1".to_string()))]);
            assert_eq!(scan_set, vec![("a".to_string(), "z".to_string(), test_ts(1))]);
        }
        _ => panic!("unexpected payload variant for prepare op"),
    }

    let (_commit_offset, _commit_op, commit_entry) = seg0_entries
        .iter()
        .find(|(_, op_id, _)| *op_id == commit_op)
        .cloned()
        .expect("commit op should exist in segment 0");
    assert_eq!(commit_entry.entry_type, VlogEntryType::Commit);
    match commit_entry.payload {
        IrPayloadInline::Commit {
            transaction_id,
            commit_ts: ts,
            prepare_ref,
        } => {
            assert_eq!(transaction_id, txn_id);
            assert_eq!(ts, commit_ts);
            match prepare_ref {
                PrepareRef::SameView(op) => assert_eq!(op, prepare_op),
                _ => panic!("unexpected prepare_ref for commit op"),
            }
        }
        _ => panic!("unexpected payload variant for commit op"),
    }
}
