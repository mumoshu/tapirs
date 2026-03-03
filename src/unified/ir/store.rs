use crate::ir::OpId;
use crate::mvcc::disk::disk_io::{DiskIo, OpenFlags};
use crate::mvcc::disk::error::StorageError;
use crate::IrClientId;
use std::collections::BTreeMap;
use std::path::Path;

#[cfg(test)]
use super::record::IrEntryRef;
use super::record::{IrMemEntry, IrPayloadInline, IrRecord, IrSstEntry, VlogEntryType};
use crate::unified::wisckeylsm::manifest::UnifiedManifest;
use crate::unified::wisckeylsm::types::VlogPtr;
use crate::unified::wisckeylsm::vlog::UnifiedVlogSegment;

const RAW_ENTRY_OVERHEAD: u32 = 25;
const TAPIR_COMMITTED_TXN_ENTRY_TYPE: u8 = 0x80;

#[cfg(test)]
pub(crate) fn append_entry<K: serde::Serialize, V: serde::Serialize, IO: DiskIo>(
    seg: &mut UnifiedVlogSegment<IO>,
    op_id: OpId,
    entry_type: VlogEntryType,
    payload: &IrPayloadInline<K, V>,
) -> Result<VlogPtr, StorageError> {
    let payload_bytes = crate::unified::ir::vlog_codec::serialize_payload(payload)?;
    seg.append_raw_entry(entry_type as u8, op_id.client_id.0, op_id.number, &payload_bytes)
}

pub(crate) fn append_batch<K: serde::Serialize, V: serde::Serialize, IO: DiskIo>(
    seg: &mut UnifiedVlogSegment<IO>,
    entries: &[(OpId, VlogEntryType, &IrPayloadInline<K, V>)],
) -> Result<Vec<VlogPtr>, StorageError> {
    if entries.is_empty() {
        return Ok(Vec::new());
    }

    let mut serialized = Vec::with_capacity(entries.len());
    for (op_id, entry_type, payload) in entries {
        let payload_bytes = crate::unified::ir::vlog_codec::serialize_payload(payload)?;
        serialized.push((*entry_type as u8, op_id.client_id.0, op_id.number, payload_bytes));
    }
    let raw_entries: Vec<(u8, u64, u64, &[u8])> = serialized
        .iter()
        .map(|(entry_type, client_id, number, bytes)| {
            (*entry_type, *client_id, *number, bytes.as_slice())
        })
        .collect();
    seg.append_raw_batch(&raw_entries)
}

pub(crate) fn read_entry<K: serde::de::DeserializeOwned, V: serde::de::DeserializeOwned, IO: DiskIo>(
    seg: &UnifiedVlogSegment<IO>,
    ptr: &VlogPtr,
) -> Result<(OpId, VlogEntryType, IrPayloadInline<K, V>), StorageError> {
    let raw = seg.read_raw_entry(ptr)?;
    let entry_type = crate::unified::ir::vlog_codec::entry_type_from_byte(raw.entry_type)?;
    let op_id = OpId {
        client_id: IrClientId(raw.id_client),
        number: raw.id_number,
    };
    let payload = crate::unified::ir::vlog_codec::deserialize_payload(entry_type, &raw.payload)?;
    Ok((op_id, entry_type, payload))
}

pub(crate) fn iter_entries<K: serde::de::DeserializeOwned, V: serde::de::DeserializeOwned, IO: DiskIo>(
    seg: &UnifiedVlogSegment<IO>,
) -> Result<Vec<(u64, OpId, VlogEntryType, IrPayloadInline<K, V>)>, StorageError> {
    let mut out = Vec::new();
    for (offset, raw) in seg.iter_raw_entries()? {
        if raw.entry_type == TAPIR_COMMITTED_TXN_ENTRY_TYPE {
            continue;
        }

        let ptr = VlogPtr {
            segment_id: seg.id,
            offset,
            length: RAW_ENTRY_OVERHEAD + raw.payload.len() as u32,
        };
        let (op_id, entry_type, payload) = read_entry(seg, &ptr)?;
        out.push((offset, op_id, entry_type, payload));
    }
    Ok(out)
}

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
                    vlog_ptr: VlogPtr {
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

#[cfg(test)]
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

#[cfg(test)]
pub(crate) fn ir_entry<'a, K: Ord, V, IO: DiskIo>(
    record: &'a IrRecord<K, V, IO>,
    op_id: &OpId,
) -> Option<IrEntryRef<'a, K, V>> {
    record.ir_entry(op_id)
}

#[cfg(test)]
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
    vlog_ptrs: &[VlogPtr],
) {
    record.apply_sealed_ptrs(finalized, vlog_ptrs);
}

pub(crate) fn clear_overlay<K: Ord, V, IO: DiskIo>(record: &mut IrRecord<K, V, IO>) {
    record.clear_overlay();
}

#[cfg(test)]
pub(crate) fn extract_finalized_entries<K: Ord + Clone, V: Clone, IO: DiskIo>(
    record: &IrRecord<K, V, IO>,
) -> Vec<(OpId, IrMemEntry<K, V>)> {
    record.extract_finalized_entries()
}

#[cfg(test)]
pub(crate) fn install_base_from_ptrs<K: Ord, V, IO: DiskIo>(
    record: &mut IrRecord<K, V, IO>,
    entries: &[(OpId, IrMemEntry<K, V>)],
    ptrs: &[VlogPtr],
) {
    record.install_base_from_ptrs(entries, ptrs);
}
