use super::types::Transaction;
use crate::mvcc::disk::error::StorageError;
use crate::occ::TransactionId as OccTransactionId;
use crate::tapir::Timestamp;
use crate::IrClientId;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

pub(crate) fn serialize_committed_txn_payload<K: Serialize, V: Serialize>(
    transaction_id: OccTransactionId,
    commit_ts: Timestamp,
    read_set: &[(K, Timestamp)],
    write_set: &[(K, Option<V>)],
    scan_set: &[(K, K, Timestamp)],
) -> Result<Vec<u8>, StorageError> {
    let ser_read_set: Vec<(Vec<u8>, Timestamp)> = read_set
        .iter()
        .map(|(k, ts)| {
            let kb = bitcode::serialize(k).map_err(|e| StorageError::Codec(e.to_string()))?;
            Ok((kb, *ts))
        })
        .collect::<Result<_, StorageError>>()?;
    let ser_write_set: Vec<(Vec<u8>, Vec<u8>)> = write_set
        .iter()
        .map(|(k, v)| {
            let kb = bitcode::serialize(k).map_err(|e| StorageError::Codec(e.to_string()))?;
            let vb = v
                .as_ref()
                .map(|val| bitcode::serialize(val))
                .transpose()
                .map_err(|e| StorageError::Codec(e.to_string()))?
                .unwrap_or_default();
            Ok((kb, vb))
        })
        .collect::<Result<_, StorageError>>()?;
    let ser_scan_set: Vec<(Vec<u8>, Vec<u8>, Timestamp)> = scan_set
        .iter()
        .map(|(sk, ek, ts)| {
            let skb = bitcode::serialize(sk).map_err(|e| StorageError::Codec(e.to_string()))?;
            let ekb = bitcode::serialize(ek).map_err(|e| StorageError::Codec(e.to_string()))?;
            Ok((skb, ekb, *ts))
        })
        .collect::<Result<_, StorageError>>()?;
    let encodable = PreparePayloadSer {
        txn_client_id: transaction_id.client_id.0,
        txn_number: transaction_id.number,
        commit_time: commit_ts.time,
        commit_client_id: commit_ts.client_id.0,
        read_set: ser_read_set,
        write_set: ser_write_set,
        scan_set: ser_scan_set,
    };
    bitcode::serialize(&encodable).map_err(|e| StorageError::Codec(e.to_string()))
}

pub(crate) fn deserialize_committed_txn_payload<K: DeserializeOwned, V: DeserializeOwned>(
    bytes: &[u8],
) -> Result<Transaction<K, V>, StorageError> {
    let p: PreparePayloadSer =
        bitcode::deserialize(bytes).map_err(|e| StorageError::Codec(e.to_string()))?;
    let read_set: Vec<(K, Timestamp)> = p
        .read_set
        .iter()
        .map(|(kb, ts)| {
            let k: K = bitcode::deserialize(kb).map_err(|e| StorageError::Codec(e.to_string()))?;
            Ok((k, *ts))
        })
        .collect::<Result<_, StorageError>>()?;
    let write_set: Vec<(K, Option<V>)> = p
        .write_set
        .iter()
        .map(|(kb, vb)| {
            let k: K = bitcode::deserialize(kb).map_err(|e| StorageError::Codec(e.to_string()))?;
            let v = if vb.is_empty() {
                None
            } else {
                Some(bitcode::deserialize::<V>(vb).map_err(|e| StorageError::Codec(e.to_string()))?)
            };
            Ok((k, v))
        })
        .collect::<Result<_, StorageError>>()?;
    let scan_set: Vec<(K, K, Timestamp)> = p
        .scan_set
        .iter()
        .map(|(skb, ekb, ts)| {
            let sk: K =
                bitcode::deserialize(skb).map_err(|e| StorageError::Codec(e.to_string()))?;
            let ek: K =
                bitcode::deserialize(ekb).map_err(|e| StorageError::Codec(e.to_string()))?;
            Ok((sk, ek, *ts))
        })
        .collect::<Result<_, StorageError>>()?;
    Ok(Transaction {
        transaction_id: OccTransactionId {
            client_id: IrClientId(p.txn_client_id),
            number: p.txn_number,
        },
        commit_ts: Timestamp {
            time: p.commit_time,
            client_id: IrClientId(p.commit_client_id),
        },
        read_set,
        write_set,
        scan_set,
    })
}

#[derive(Serialize, Deserialize)]
struct PreparePayloadSer {
    txn_client_id: u64,
    txn_number: u64,
    commit_time: u64,
    commit_client_id: u64,
    read_set: Vec<(Vec<u8>, Timestamp)>,
    write_set: Vec<(Vec<u8>, Vec<u8>)>,
    scan_set: Vec<(Vec<u8>, Vec<u8>, Timestamp)>,
}