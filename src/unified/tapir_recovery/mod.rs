#[cfg(test)]
use crate::ir::OpId;
#[cfg(test)]
use crate::mvcc::disk::error::StorageError;
#[cfg(test)]
use crate::occ::TransactionId as OccTransactionId;
#[cfg(test)]
use crate::occ::{ScanEntry, Transaction};
#[cfg(test)]
use crate::tapir::{ShardNumber, Sharded};
#[cfg(test)]
use crate::unified::ir::record::IrMemEntry;
#[cfg(test)]
use crate::unified::ir::record::{IrPayloadInline, IrState, PrepareRef};
#[cfg(test)]
use std::collections::BTreeMap;
#[cfg(test)]
use std::sync::Arc;

#[cfg(test)]
pub(crate) mod teststore;

#[cfg(test)]
fn replay_committed_from_ir_record(
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

    for (op_id, entry) in ir_record {
        if !matches!(entry.state, IrState::Finalized(_)) {
            continue;
        }

        let IrPayloadInline::Commit {
            transaction_id,
            commit_ts,
            prepare_ref,
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

        let mut txn = Transaction::<String, String, crate::tapir::Timestamp>::default();
        for (key, ts) in read_set {
            txn.add_read(
                Sharded {
                    shard: ShardNumber(0),
                    key: key.clone(),
                },
                *ts,
            );
        }
        for (key, value) in write_set {
            txn.add_write(
                Sharded {
                    shard: ShardNumber(0),
                    key: key.clone(),
                },
                value.clone(),
            );
        }
        for (start, end, ts) in scan_set {
            txn.scan_set.push(ScanEntry {
                shard: ShardNumber(0),
                start_key: start.clone(),
                end_key: end.clone(),
                timestamp: *ts,
            });
        }

        store.commit(
            *op_id,
            *transaction_id,
            Arc::new(txn),
            *commit_ts,
            match prepare_ref {
                PrepareRef::SameView(op_id) => PrepareRef::SameView(*op_id),
                PrepareRef::CrossView { view, vlog_ptr } => PrepareRef::CrossView {
                    view: *view,
                    vlog_ptr: *vlog_ptr,
                },
            },
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests;
