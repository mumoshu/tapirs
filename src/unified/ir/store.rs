use crate::ir::OpId;
use crate::mvcc::disk::disk_io::{DiskIo, OpenFlags};
use crate::mvcc::disk::error::StorageError;
use crate::IrClientId;
use std::collections::BTreeMap;
use std::path::Path;

use super::record::{IrMemEntry, IrRecord};
use crate::unified::wisckeylsm::lsm::VlogLsm;
use crate::unified::wisckeylsm::manifest::UnifiedManifest;
use crate::unified::wisckeylsm::types::VlogPtr;
use crate::unified::wisckeylsm::vlog::VlogSegment;

/// Raw entry overhead: header(21) + crc(4) = 25 bytes.
const RAW_ENTRY_OVERHEAD: u32 = 25;

/// TAPIR committed transaction entries use entry_type >= 0x80;
/// skip them when iterating IR entries.
const TAPIR_ENTRY_TYPE_MIN: u8 = 0x80;

pub(crate) fn open_store_state<
    K: Ord + serde::de::DeserializeOwned,
    V: serde::de::DeserializeOwned,
    IO: DiskIo,
>(
    base_dir: &Path,
    io_flags: OpenFlags,
) -> Result<IrRecord<K, V, IO>, StorageError> {
    let manifest = match UnifiedManifest::load::<IO>(base_dir)? {
        Some(m) => m,
        None => UnifiedManifest::new(),
    };

    let mut sealed_segments = BTreeMap::new();
    for seg_meta in &manifest.ir.sealed_vlog_segments {
        let seg = VlogSegment::<IO>::open_at(
            seg_meta.segment_id,
            seg_meta.path.clone(),
            seg_meta.total_size,
            seg_meta.views.clone(),
            io_flags,
        )?;
        sealed_segments.insert(seg_meta.segment_id, seg);
    }

    let active_path = base_dir.join(format!("ir_vlog_{:04}.dat", manifest.ir.active_segment_id));
    let mut active_vlog = VlogSegment::<IO>::open_at(
        manifest.ir.active_segment_id,
        active_path,
        manifest.ir.active_write_offset,
        Vec::new(),
        io_flags,
    )?;

    let current_view = manifest.current_view;
    active_vlog.start_view(current_view);

    let mut lsm = VlogLsm::open_from_parts(
        "ir",
        base_dir,
        active_vlog,
        sealed_segments,
        io_flags,
        manifest.ir.next_segment_id,
        usize::MAX, // never flush index to SST
    );

    // Recovery: rebuild index from vlog segments.
    rebuild_index_from_vlog(&mut lsm)?;

    Ok(IrRecord::new(
        lsm,
        current_view,
        manifest,
        base_dir.to_path_buf(),
    ))
}

/// Rebuild the VlogLsm index (OpId → VlogPtr) by scanning all vlog segments.
fn rebuild_index_from_vlog<K: Ord, V, IO: DiskIo>(
    lsm: &mut VlogLsm<OpId, IrMemEntry<K, V>, IO>,
) -> Result<(), StorageError> {
    let mut entries: Vec<(OpId, VlogPtr)> = Vec::new();

    fn absorb_segment<IO: DiskIo>(
        entries: &mut Vec<(OpId, VlogPtr)>,
        seg: &VlogSegment<IO>,
    ) -> Result<(), StorageError> {
        for (offset, raw) in seg.iter_raw_entries()? {
            if raw.entry_type >= TAPIR_ENTRY_TYPE_MIN {
                continue;
            }
            let op_id = OpId {
                client_id: IrClientId(raw.id_client),
                number: raw.id_number,
            };
            let ptr = VlogPtr {
                segment_id: seg.id,
                offset,
                length: RAW_ENTRY_OVERHEAD + raw.payload.len() as u32,
            };
            entries.push((op_id, ptr));
        }
        Ok(())
    }

    // Collect from sealed segments first, then active.
    let sealed_ids: Vec<u64> = lsm.sealed_segments_ref().keys().copied().collect();
    for seg_id in sealed_ids {
        let seg = lsm.sealed_segments_ref().get(&seg_id).unwrap();
        absorb_segment(&mut entries, seg)?;
    }
    absorb_segment(&mut entries, lsm.active_vlog_ref())?;

    for (op_id, ptr) in entries {
        lsm.index_insert(op_id, ptr);
    }

    Ok(())
}

pub(crate) fn seal_current_view<
    K: Ord + Clone + serde::Serialize + serde::de::DeserializeOwned,
    V: Clone + serde::Serialize,
    IO: DiskIo,
>(
    record: &mut IrRecord<K, V, IO>,
    min_vlog_size: u64,
) -> Result<(), StorageError> {
    record.seal_current_view(min_vlog_size)
}

pub(crate) fn insert_ir_entry<K: Ord, V, IO: DiskIo>(
    record: &mut IrRecord<K, V, IO>,
    op_id: OpId,
    entry: IrMemEntry<K, V>,
) {
    record.insert_ir_entry(op_id, entry);
}

pub(crate) fn read_entry<
    K: serde::de::DeserializeOwned,
    V: serde::de::DeserializeOwned,
    IO: DiskIo,
>(
    seg: &VlogSegment<IO>,
    ptr: &VlogPtr,
) -> Result<(OpId, IrMemEntry<K, V>), StorageError> {
    let raw = seg.read_raw_entry(ptr)?;
    let op_id = OpId {
        client_id: IrClientId(raw.id_client),
        number: raw.id_number,
    };
    let entry: IrMemEntry<K, V> =
        bitcode::deserialize(&raw.payload).map_err(|e| StorageError::Codec(e.to_string()))?;
    Ok((op_id, entry))
}

pub(crate) fn iter_entries<
    K: serde::de::DeserializeOwned,
    V: serde::de::DeserializeOwned,
    IO: DiskIo,
>(
    seg: &VlogSegment<IO>,
) -> Result<Vec<(u64, OpId, IrMemEntry<K, V>)>, StorageError> {
    let mut out = Vec::new();
    for (offset, raw) in seg.iter_raw_entries()? {
        if raw.entry_type >= TAPIR_ENTRY_TYPE_MIN {
            continue;
        }

        let ptr = VlogPtr {
            segment_id: seg.id,
            offset,
            length: RAW_ENTRY_OVERHEAD + raw.payload.len() as u32,
        };
        let (op_id, entry) = read_entry(seg, &ptr)?;
        out.push((offset, op_id, entry));
    }
    Ok(out)
}
