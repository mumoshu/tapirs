use crate::ir::OpId;
use crate::mvcc::disk::disk_io::{DiskIo, OpenFlags};
use crate::mvcc::disk::error::StorageError;
use std::collections::BTreeMap;
use std::path::Path;

use super::record::{IrEntryRef, IrMemEntry, IrPayloadInline, IrRecord, IrSstEntry, VlogEntryType};
use crate::unified::types::UnifiedVlogPtr;
use crate::unified::wisckeylsm::manifest::UnifiedManifest;
use crate::unified::wisckeylsm::vlog::UnifiedVlogSegment;

const RAW_ENTRY_OVERHEAD: u32 = 25;
const TAPIR_COMMITTED_TXN_ENTRY_TYPE: u8 = 0x80;

fn rebuild_ir_base_from_vlog<K: Ord, V, IO: DiskIo>(record: &mut IrRecord<K, V, IO>) -> Result<(), StorageError> {
    record.clear_ir_base();

    let mut rebuilt_entries: Vec<(OpId, IrSstEntry)> = Vec::new();

    fn absorb_segment<IO: DiskIo>(
        rebuilt_entries: &mut Vec<(OpId, IrSstEntry)>,
        seg: &UnifiedVlogSegment<IO>,
    ) -> Result<(), StorageError> {
        for (offset, raw) in seg.iter_raw_entries()? {
            if raw.entry_type == TAPIR_COMMITTED_TXN_ENTRY_TYPE {
                continue;
            }

            let entry_type = crate::unified::ir::vlog_codec::entry_type_from_byte(raw.entry_type)?;
            let op_id = OpId {
                client_id: crate::IrClientId(raw.id_client),
                number: raw.id_number,
            };

            rebuilt_entries.push((
                op_id,
                IrSstEntry {
                    entry_type,
                    vlog_ptr: UnifiedVlogPtr {
                        segment_id: seg.id,
                        offset,
                        length: RAW_ENTRY_OVERHEAD + raw.payload.len() as u32,
                    },
                },
            ));
        }
        Ok(())
    }

    for seg in record.sealed_vlog_segment_values() {
        absorb_segment(&mut rebuilt_entries, seg)?;
    }
    absorb_segment(&mut rebuilt_entries, record.active_vlog_ref())?;

    for (op_id, entry) in rebuilt_entries {
        record.insert_ir_base_entry(op_id, entry);
    }

    Ok(())
}

pub(crate) fn open_store_state<K: Ord, V, IO: DiskIo>(
    base_dir: &Path,
    io_flags: OpenFlags,
) -> Result<IrRecord<K, V, IO>, StorageError> {
    let manifest = match UnifiedManifest::load::<IO>(base_dir)? {
        Some(m) => m,
        None => UnifiedManifest::new(),
    };

    let mut sealed_vlog_segments = BTreeMap::new();
    for seg_meta in &manifest.sealed_vlog_segments {
        let seg = UnifiedVlogSegment::<IO>::open_at(
            seg_meta.segment_id,
            seg_meta.path.clone(),
            seg_meta.total_size,
            seg_meta.views.clone(),
            io_flags,
        )?;
        sealed_vlog_segments.insert(seg_meta.segment_id, seg);
    }

    let active_path = base_dir.join(format!("vlog_seg_{:04}.dat", manifest.active_segment_id));
    let mut active_vlog = UnifiedVlogSegment::<IO>::open_at(
        manifest.active_segment_id,
        active_path,
        manifest.active_write_offset,
        Vec::new(),
        io_flags,
    )?;

    let current_view = manifest.current_view;
    active_vlog.start_view(current_view);

    let mut record = IrRecord::new(
        active_vlog,
        sealed_vlog_segments,
        current_view,
        manifest,
    );
    rebuild_ir_base_from_vlog(&mut record)?;
    Ok(record)
}

pub(crate) fn seal_current_view<K: Ord + Clone + serde::Serialize, V: Clone + serde::Serialize, IO: DiskIo>(
    record: &mut IrRecord<K, V, IO>,
    base_dir: &Path,
    io_flags: OpenFlags,
    min_view_vlog_size: u64,
) -> Result<(), StorageError> {
    let finalized_entries = collect_finalized_for_seal(record);

    let vlog_ptrs = record.append_ir_entries_for_seal(&finalized_entries)?;

    apply_sealed_ptrs(record, &finalized_entries, &vlog_ptrs);
    record.seal_active_view_and_rotate_if_needed(
        base_dir,
        io_flags,
        min_view_vlog_size,
        finalized_entries.len(),
    )?;

    Ok(())
}

pub(crate) fn ir_overlay_entries<'a, K: Ord, V, IO: DiskIo>(
    record: &'a IrRecord<K, V, IO>,
) -> impl Iterator<Item = (&'a OpId, &'a IrMemEntry<K, V>)> + 'a {
    record.ir_overlay_entries()
}

pub(crate) fn insert_ir_entry<K: Ord, V, IO: DiskIo>(
    record: &mut IrRecord<K, V, IO>,
    op_id: OpId,
    entry: IrMemEntry<K, V>,
) {
    record.insert_ir_entry(op_id, entry);
}

pub(crate) fn ir_entry<'a, K: Ord, V, IO: DiskIo>(
    record: &'a IrRecord<K, V, IO>,
    op_id: &OpId,
) -> Option<IrEntryRef<'a, K, V>> {
    record.ir_entry(op_id)
}

pub(crate) fn lookup_ir_base_entry<K: Ord, V, IO: DiskIo>(
    record: &IrRecord<K, V, IO>,
    op_id: OpId,
) -> Option<&IrSstEntry> {
    record.lookup_ir_base_entry(op_id)
}

pub(crate) fn collect_finalized_for_seal<K: Ord + Clone, V: Clone, IO: DiskIo>(
    record: &IrRecord<K, V, IO>,
) -> Vec<(OpId, VlogEntryType, IrPayloadInline<K, V>)> {
    record.collect_finalized_for_seal()
}

pub(crate) fn apply_sealed_ptrs<K: Ord, V, IO: DiskIo>(
    record: &mut IrRecord<K, V, IO>,
    finalized: &[(OpId, VlogEntryType, IrPayloadInline<K, V>)],
    vlog_ptrs: &[UnifiedVlogPtr],
) {
    record.apply_sealed_ptrs(finalized, vlog_ptrs);
}

pub(crate) fn clear_overlay<K: Ord, V, IO: DiskIo>(record: &mut IrRecord<K, V, IO>) {
    record.clear_overlay();
}

pub(crate) fn extract_finalized_entries<K: Ord + Clone, V: Clone, IO: DiskIo>(
    record: &IrRecord<K, V, IO>,
) -> Vec<(OpId, IrMemEntry<K, V>)> {
    record.extract_finalized_entries()
}

pub(crate) fn install_base_from_ptrs<K: Ord, V, IO: DiskIo>(
    record: &mut IrRecord<K, V, IO>,
    entries: &[(OpId, IrMemEntry<K, V>)],
    ptrs: &[UnifiedVlogPtr],
) {
    record.install_base_from_ptrs(entries, ptrs);
}
