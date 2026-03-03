#[cfg(test)]
use crate::ir::OpId;
#[cfg(test)]
use crate::mvcc::disk::error::StorageError;
#[cfg(test)]
use crate::occ::TransactionId as OccTransactionId;
#[cfg(test)]
use crate::unified::ir::record::IrMemEntry;
#[cfg(test)]
use crate::unified::ir::record::{IrPayloadInline, IrState};
#[cfg(test)]
use std::collections::BTreeMap;

#[cfg(test)]
pub(crate) mod teststore;

#[cfg(test)]
pub(crate) fn replay_committed_from_ir_record(
    store: &mut crate::unified::tapir_recovery::teststore::TestStore,
    ir_record: &[(OpId, IrMemEntry<String, String>)],
) -> Result<(), StorageError> {
    let mut prepares: BTreeMap<
        OccTransactionId,
        (
            &[(String, crate::tapir::Timestamp)],
            &[(String, Option<String>)],
            &[(String, String, crate::tapir::Timestamp)],
        ),
    > = BTreeMap::new();

    for (_op_id, entry) in ir_record {
        if !matches!(entry.state, IrState::Finalized(_)) {
            continue;
        }

        if let IrPayloadInline::Prepare {
            transaction_id,
            commit_ts,
            read_set,
            write_set,
            scan_set,
        } = &entry.payload
        {
            let _ = commit_ts;
            prepares.insert(*transaction_id, (read_set.as_slice(), write_set.as_slice(), scan_set.as_slice()));
        }
    }

    for (_op_id, entry) in ir_record {
        if !matches!(entry.state, IrState::Finalized(_)) {
            continue;
        }

        let IrPayloadInline::Commit {
            transaction_id,
            commit_ts,
            ..
        } = &entry.payload
        else {
            continue;
        };

        let (read_set, write_set, scan_set) = prepares.get(transaction_id).copied().ok_or_else(|| {
            StorageError::Codec(format!(
                "missing finalized IR prepare for committed txn {:?}",
                transaction_id
            ))
        })?;

        store.commit_transaction_data(
            *transaction_id,
            read_set,
            write_set,
            scan_set,
            *commit_ts,
        )?;
    }

    Ok(())
}

#[cfg(test)]
mod tests;
