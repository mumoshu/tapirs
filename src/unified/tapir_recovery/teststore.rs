use crate::ir::OpId;
use crate::mvcc::disk::disk_io::DiskIo;
use crate::mvcc::disk::disk_io::OpenFlags;
use crate::mvcc::disk::error::StorageError;
use crate::mvcc::disk::memory_io::MemoryIo;
use crate::occ::{Transaction, TransactionId};
use crate::tapir::Timestamp;
use crate::unified::ir::record::{IrEntryRef, IrMemEntry, IrPayloadInline, IrRecord, IrSstEntry, VlogEntryType};
use crate::unified::ir::store as ir_store;
use crate::unified::tapir::storage_types::LsmEntry;
use crate::unified::tapir::store::{self as tapir_store, TapirState};
use crate::unified::tapir::VlogSegment;
use std::path::{Path, PathBuf};

pub(crate) struct TestStore {
    ir_state: IrRecord<String, String, MemoryIo>,
    tapir_state: TapirState<String, String, MemoryIo>,
    base_dir: PathBuf,
    io_flags: OpenFlags,
    min_view_vlog_size: u64,
}

const DEFAULT_MIN_VIEW_VLOG_SIZE: u64 = 256 * 1024;

#[allow(dead_code)]
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

    pub(crate) fn base_dir(&self) -> &Path {
        &self.base_dir
    }

    pub(crate) fn current_view(&self) -> u64 {
        self.ir_state.current_view()
    }

    pub(crate) fn vlog_read_count(&self) -> u64 {
        self.tapir_state.vlog_read_count()
    }

    pub(crate) fn sealed_vlog_segments(&self) -> &std::collections::BTreeMap<u64, VlogSegment<MemoryIo>> {
        self.ir_state.sealed_vlog_segments()
    }

    pub(crate) fn insert_ir_entry(&mut self, op_id: OpId, entry: IrMemEntry<String, String>) {
        ir_store::insert_ir_entry(&mut self.ir_state, op_id, entry);
    }

    pub(crate) fn ir_entry(&self, op_id: &OpId) -> Option<IrEntryRef<'_, String, String>> {
        ir_store::ir_entry(&self.ir_state, op_id)
    }

    pub(crate) fn lookup_ir_base_entry(&self, op_id: OpId) -> Option<&IrSstEntry> {
        ir_store::lookup_ir_base_entry(&self.ir_state, op_id)
    }

    pub(crate) fn ir_overlay_entries(&self) -> impl Iterator<Item = (&OpId, &IrMemEntry<String, String>)> {
        ir_store::ir_overlay_entries(&self.ir_state)
    }

    pub(crate) fn register_prepare(
        &mut self,
        txn_id: TransactionId,
        txn: &Transaction<String, String, Timestamp>,
        commit_ts: Timestamp,
    ) {
        self.tapir_state.register_prepare(txn_id, txn, commit_ts);
    }

    pub(crate) fn unregister_prepare(&mut self, txn_id: &TransactionId) {
        self.tapir_state.unregister_prepare(txn_id);
    }

    pub(crate) fn resolve_in_memory(&self, txn_id: &TransactionId, write_index: u16) -> Option<&(String, Option<String>)> {
        self.tapir_state.resolve_in_memory(txn_id, write_index)
    }

    pub(crate) fn commit_transaction_data(
        &mut self,
        txn_id: TransactionId,
        read_set: &[(String, Timestamp)],
        write_set: &[(String, Option<String>)],
        scan_set: &[(String, String, Timestamp)],
        commit: Timestamp,
    ) -> Result<(), StorageError> {
        self.tapir_state
            .commit_transaction_data(txn_id, read_set, write_set, scan_set, commit)
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

    pub(crate) fn resolve_value(&self, entry: &LsmEntry) -> Result<Option<String>, StorageError> {
        tapir_store::resolve_value(&self.tapir_state, entry)
    }

    pub(crate) fn insert_memtable_entry(&mut self, key: String, ts: Timestamp, entry: LsmEntry) {
        self.tapir_state.insert_memtable_entry(key, ts, entry);
    }

    pub(crate) fn memtable_entry_at(&self, key: &String, ts: Timestamp) -> Option<&LsmEntry> {
        self.tapir_state.memtable_entry_at(key, ts)
    }

    pub(crate) fn memtable_snapshot(&self) -> Vec<(String, Timestamp, LsmEntry)> {
        self.tapir_state.memtable_snapshot()
    }

    pub(crate) fn get_last_read(&self, key: &String) -> Result<Option<Timestamp>, StorageError> {
        self.tapir_state.get_last_read(key)
    }

    pub(crate) fn get_last_read_at(&self, key: &String, ts: Timestamp) -> Result<Option<Timestamp>, StorageError> {
        self.tapir_state.get_last_read_at(key, ts)
    }

    pub(crate) fn get_range(&self, key: &String, ts: Timestamp) -> Result<(Timestamp, Option<Timestamp>), StorageError> {
        self.tapir_state.get_range(key, ts)
    }

    pub(crate) fn has_writes_in_range(
        &self,
        start: &String,
        end: &String,
        after_ts: Timestamp,
        before_ts: Timestamp,
    ) -> Result<bool, StorageError> {
        self.tapir_state
            .has_writes_in_range(start, end, after_ts, before_ts)
    }

    pub(crate) fn commit_get(&mut self, key: String, read: Timestamp, commit: Timestamp) -> Result<(), StorageError> {
        self.tapir_state.commit_get(key, read, commit)
    }

    pub(crate) fn extract_finalized_entries(&self) -> Vec<(OpId, IrMemEntry<String, String>)> {
        ir_store::extract_finalized_entries(&self.ir_state)
    }

    pub(crate) fn install_merged_record(
        &mut self,
        entries: Vec<(OpId, IrMemEntry<String, String>)>,
        target_view: u64,
    ) -> Result<(), StorageError> {
        let entry_refs: Vec<(OpId, VlogEntryType, &IrPayloadInline<String, String>)> = entries
            .iter()
            .map(|(op, entry)| (*op, entry.entry_type, &entry.payload))
            .collect();

        let ptrs = if !entry_refs.is_empty() {
            self.ir_state.append_batch_to_active(&entry_refs)?
        } else {
            Vec::new()
        };

        ir_store::install_base_from_ptrs(&mut self.ir_state, &entries, &ptrs);
        self.ir_state.set_current_view_for_install(target_view);
        self.tapir_state.clear_prepare_registry();
        Ok(())
    }
}
