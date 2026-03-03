#[cfg(test)]
use std::collections::BTreeMap;

#[cfg(test)]
use crate::ir::OpId;
#[cfg(test)]
use crate::mvcc::disk::disk_io::DiskIo;
#[cfg(test)]
use crate::mvcc::disk::error::StorageError;
#[cfg(test)]
use crate::occ::TransactionId;

#[cfg(test)]
use super::types::{IrMemEntry, IrPayloadInline, VlogEntryType};
#[cfg(test)]
use super::UnifiedStore;

#[cfg(test)]
pub(crate) fn replay_committed_from_ir_record<K, V, IO>(
    store: &mut UnifiedStore<K, V, IO>,
    ir_record: &[(OpId, IrMemEntry<K, V>)],
) -> Result<(), StorageError>
where
    K: Ord + Clone + serde::Serialize + serde::de::DeserializeOwned,
    V: Clone + serde::Serialize + serde::de::DeserializeOwned,
    IO: DiskIo,
{
    let mut prepare_index: BTreeMap<TransactionId, &IrPayloadInline<K, V>> = BTreeMap::new();
    for (_, entry) in ir_record {
        if entry.entry_type == VlogEntryType::Prepare
            && let IrPayloadInline::Prepare {
                transaction_id, ..
            } = &entry.payload
        {
            prepare_index.insert(*transaction_id, &entry.payload);
        }
    }

    for (_, entry) in ir_record {
        if entry.entry_type == VlogEntryType::Commit
            && let IrPayloadInline::Commit {
                transaction_id,
                commit_ts,
                ..
            } = &entry.payload
            && let Some(IrPayloadInline::Prepare {
                write_set,
                read_set,
                scan_set,
                ..
            }) = prepare_index.get(transaction_id)
        {
            store.commit_transaction_data(*transaction_id, read_set, write_set, scan_set, *commit_ts)?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests;
