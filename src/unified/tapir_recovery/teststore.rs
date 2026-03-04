use crate::ir::OpId;
use crate::mvcc::disk::disk_io::DiskIo;
use crate::mvcc::disk::disk_io::OpenFlags;
use crate::mvcc::disk::error::StorageError;
use crate::mvcc::disk::memory_io::MemoryIo;
use crate::occ::{SharedTransaction, TransactionId};
use crate::tapir::{ShardNumber, Timestamp};
use crate::unified::ir::record::{IrMemEntry, IrPayloadInline, IrRecord, IrState, PrepareRef, VlogEntryType};
use crate::unified::ir::store as ir_store;
use crate::unified::tapir::store::{self as tapir_store, TapirState};
use std::path::PathBuf;

pub(crate) struct TestStore {
    ir_state: IrRecord<String, String, MemoryIo>,
    tapir_state: TapirState<String, String, MemoryIo>,
    base_dir: PathBuf,
    io_flags: OpenFlags,
    min_view_vlog_size: u64,
}

const DEFAULT_MIN_VIEW_VLOG_SIZE: u64 = 256 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Metrics {
    pub(crate) current_view: u64,
    pub(crate) vlog_read_count: u64,
}

impl TestStore {
    pub(crate) fn open(base_dir: PathBuf) -> Result<Self, StorageError> {
        Self::open_with_options(base_dir, DEFAULT_MIN_VIEW_VLOG_SIZE)
    }

    pub(crate) fn open_with_options(base_dir: PathBuf, min_view_vlog_size: u64) -> Result<Self, StorageError> {
        let io_flags = OpenFlags {
            create: true,
            direct: false,
        };
        MemoryIo::create_dir_all(&base_dir)?;
        let ir_state = ir_store::open_store_state::<String, String, MemoryIo>(&base_dir, io_flags)?;
        let tapir_state = tapir_store::open::<String, String, MemoryIo>(&base_dir.join("tapir"), io_flags)?;
        Ok(Self {
            ir_state,
            tapir_state,
            base_dir,
            io_flags,
            min_view_vlog_size,
        })
    }

    fn current_view(&self) -> u64 {
        self.ir_state.current_view()
    }

    pub(crate) fn get_metrics(&self) -> Metrics {
        Metrics {
            current_view: self.current_view(),
            vlog_read_count: 0,
        }
    }

    fn insert_ir_entry(&mut self, op_id: OpId, entry: IrMemEntry<String, String>) {
        ir_store::insert_ir_entry(&mut self.ir_state, op_id, entry);
    }

    pub(crate) fn prepare(
        &mut self,
        op_id: OpId,
        txn_id: TransactionId,
        txn: SharedTransaction<String, String, Timestamp>,
        commit_ts: Timestamp,
        finalized: bool,
    ) {
        let current_view = self.current_view();
        self.insert_ir_entry(
            op_id,
            IrMemEntry {
                entry_type: VlogEntryType::Prepare,
                state: if finalized {
                    IrState::Finalized(current_view)
                } else {
                    IrState::Tentative
                },
                payload: IrPayloadInline::Prepare {
                    transaction_id: txn_id,
                    commit_ts,
                    read_set: txn
                        .shard_read_set(ShardNumber(0))
                        .map(|(k, ts)| (k.clone(), ts))
                        .collect(),
                    write_set: txn
                        .shard_write_set(ShardNumber(0))
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect(),
                    scan_set: vec![],
                },
            },
        );

        self.tapir_state.prepare(txn_id, &txn, commit_ts);
    }

    pub(crate) fn abort(&mut self, op_id: OpId, txn_id: TransactionId) {
        let current_view = self.current_view();
        self.insert_ir_entry(
            op_id,
            IrMemEntry {
                entry_type: VlogEntryType::Abort,
                // IR inconsistent operation upcalls to TAPIR on Finalize
                // so when abort is called, the prepare entry is already finalized.
                state: IrState::Finalized(current_view),
                payload: IrPayloadInline::Abort {
                    transaction_id: txn_id,
                    commit_ts: None,
                },
            },
        );
        self.tapir_state.abort(&txn_id);
    }

    pub(crate) fn commit(
        &mut self,
        op_id: OpId,
        txn_id: TransactionId,
        txn: SharedTransaction<String, String, Timestamp>,
        commit_ts: Timestamp,
        prepare_ref: PrepareRef,
    ) {
        let current_view = self.current_view();
        self.insert_ir_entry(
            op_id,
            IrMemEntry {
                entry_type: VlogEntryType::Commit,
                state: IrState::Finalized(current_view),
                payload: IrPayloadInline::Commit {
                    transaction_id: txn_id,
                    commit_ts,
                    prepare_ref,
                },
            },
        );

        let shard = ShardNumber(0);
        let read_set: Vec<(String, Timestamp)> = txn
            .shard_read_set(shard)
            .map(|(k, ts)| (k.clone(), ts))
            .collect();
        let write_set: Vec<(String, Option<String>)> = txn
            .shard_write_set(shard)
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        let scan_set: Vec<(String, String, Timestamp)> = txn
            .shard_scan_set(shard)
            .map(|entry| (entry.start_key.clone(), entry.end_key.clone(), entry.timestamp))
            .collect();

        self.tapir_state
            .commit(txn_id, &read_set, &write_set, &scan_set, commit_ts)
            .unwrap();
    }

    pub(crate) fn seal_current_view(&mut self) -> Result<(), StorageError> {
        ir_store::seal_current_view(
            &mut self.ir_state,
            &self.base_dir,
            self.io_flags,
            self.min_view_vlog_size,
        )?;
        self.tapir_state.seal_current_view(self.min_view_vlog_size)?;
        ir_store::clear_overlay(&mut self.ir_state);
        Ok(())
    }

    pub(crate) fn do_uncommitted_get(&self, key: &String) -> Result<(Option<String>, Timestamp), StorageError> {
        self.tapir_state.do_uncommitted_get(key)
    }

    pub(crate) fn do_uncommitted_get_at(&self, key: &String, ts: Timestamp) -> Result<(Option<String>, Timestamp), StorageError> {
        self.tapir_state.do_uncommitted_get_at(key, ts)
    }

    pub(crate) fn do_uncommitted_scan(
        &self,
        start: &String,
        end: &String,
        ts: Timestamp,
    ) -> Result<Vec<(String, Option<String>, Timestamp)>, StorageError> {
        self.tapir_state.do_uncommitted_scan(start, end, ts)
    }

    pub(crate) fn extract_finalized_entries(&self) -> Vec<(OpId, IrMemEntry<String, String>)> {
        let current_view = self.current_view();
        ir_store::collect_finalized_for_seal(&self.ir_state)
            .iter()
            .map(|(op_id, entry_type, payload)| {
                (
                    *op_id,
                    IrMemEntry {
                        entry_type: *entry_type,
                        state: IrState::Finalized(current_view),
                        payload: payload.clone(),
                    },
                )
            })
            .collect()
    }
}
