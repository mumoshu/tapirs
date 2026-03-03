use super::record::{IrPayloadInline, PrepareRef, VlogEntryType};
use crate::mvcc::disk::error::StorageError;
use crate::occ::TransactionId as OccTransactionId;
use crate::tapir::Timestamp;
use crate::IrClientId;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

pub(crate) fn entry_type_from_byte(entry_type_byte: u8) -> Result<VlogEntryType, StorageError> {
    match entry_type_byte {
        0x01 => Ok(VlogEntryType::Prepare),
        0x02 => Ok(VlogEntryType::Commit),
        0x03 => Ok(VlogEntryType::Abort),
        0x04 => Ok(VlogEntryType::QuorumRead),
        0x05 => Ok(VlogEntryType::QuorumScan),
        0x06 => Ok(VlogEntryType::RaiseMinPrepareTime),
        _ => Err(StorageError::Codec(format!(
            "unknown VLog entry type: {entry_type_byte:#04x}"
        ))),
    }
}

pub(crate) fn serialize_payload<K: Serialize, V: Serialize>(
    payload: &IrPayloadInline<K, V>,
) -> Result<Vec<u8>, StorageError> {
    let bytes = match payload {
        IrPayloadInline::Prepare {
            transaction_id,
            commit_ts,
            read_set,
            write_set,
            scan_set,
        } => {
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
            bitcode::serialize(&encodable).map_err(|e| StorageError::Codec(e.to_string()))?
        }
        IrPayloadInline::Commit {
            transaction_id,
            commit_ts,
            prepare_ref,
        } => {
            let encodable = CommitPayloadSer {
                txn_client_id: transaction_id.client_id.0,
                txn_number: transaction_id.number,
                commit_time: commit_ts.time,
                commit_client_id: commit_ts.client_id.0,
                prepare_ref: prepare_ref.clone(),
            };
            bitcode::serialize(&encodable).map_err(|e| StorageError::Codec(e.to_string()))?
        }
        IrPayloadInline::Abort {
            transaction_id,
            commit_ts,
        } => {
            let encodable = AbortPayloadSer {
                txn_client_id: transaction_id.client_id.0,
                txn_number: transaction_id.number,
                commit_ts: commit_ts.map(|ts| (ts.time, ts.client_id.0)),
            };
            bitcode::serialize(&encodable).map_err(|e| StorageError::Codec(e.to_string()))?
        }
        IrPayloadInline::QuorumRead { key, timestamp } => {
            let key_bytes = bitcode::serialize(key).map_err(|e| StorageError::Codec(e.to_string()))?;
            let encodable = QuorumReadPayloadSer {
                key: key_bytes,
                time: timestamp.time,
                client_id: timestamp.client_id.0,
            };
            bitcode::serialize(&encodable).map_err(|e| StorageError::Codec(e.to_string()))?
        }
        IrPayloadInline::QuorumScan {
            start_key,
            end_key,
            snapshot_ts,
        } => {
            let start_bytes = bitcode::serialize(start_key).map_err(|e| StorageError::Codec(e.to_string()))?;
            let end_bytes = bitcode::serialize(end_key).map_err(|e| StorageError::Codec(e.to_string()))?;
            let encodable = QuorumScanPayloadSer {
                start_key: start_bytes,
                end_key: end_bytes,
                time: snapshot_ts.time,
                client_id: snapshot_ts.client_id.0,
            };
            bitcode::serialize(&encodable).map_err(|e| StorageError::Codec(e.to_string()))?
        }
        IrPayloadInline::RaiseMinPrepareTime { time } => {
            bitcode::serialize(time).map_err(|e| StorageError::Codec(e.to_string()))?
        }
    };
    Ok(bytes)
}

pub(crate) fn deserialize_payload<K: DeserializeOwned, V: DeserializeOwned>(
    entry_type: VlogEntryType,
    bytes: &[u8],
) -> Result<IrPayloadInline<K, V>, StorageError> {
    match entry_type {
        VlogEntryType::Prepare => {
            let p: PreparePayloadSer =
                bitcode::deserialize(bytes).map_err(|e| StorageError::Codec(e.to_string()))?;
            let read_set: Vec<(K, Timestamp)> = p
                .read_set
                .iter()
                .map(|(kb, ts)| {
                    let k: K =
                        bitcode::deserialize(kb).map_err(|e| StorageError::Codec(e.to_string()))?;
                    Ok((k, *ts))
                })
                .collect::<Result<_, StorageError>>()?;
            let write_set: Vec<(K, Option<V>)> = p
                .write_set
                .iter()
                .map(|(kb, vb)| {
                    let k: K =
                        bitcode::deserialize(kb).map_err(|e| StorageError::Codec(e.to_string()))?;
                    let v = if vb.is_empty() {
                        None
                    } else {
                        Some(
                            bitcode::deserialize::<V>(vb)
                                .map_err(|e| StorageError::Codec(e.to_string()))?,
                        )
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
            Ok(IrPayloadInline::Prepare {
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
        VlogEntryType::Commit => {
            let c: CommitPayloadSer =
                bitcode::deserialize(bytes).map_err(|e| StorageError::Codec(e.to_string()))?;
            Ok(IrPayloadInline::Commit {
                transaction_id: OccTransactionId {
                    client_id: IrClientId(c.txn_client_id),
                    number: c.txn_number,
                },
                commit_ts: Timestamp {
                    time: c.commit_time,
                    client_id: IrClientId(c.commit_client_id),
                },
                prepare_ref: c.prepare_ref,
            })
        }
        VlogEntryType::Abort => {
            let a: AbortPayloadSer =
                bitcode::deserialize(bytes).map_err(|e| StorageError::Codec(e.to_string()))?;
            Ok(IrPayloadInline::Abort {
                transaction_id: OccTransactionId {
                    client_id: IrClientId(a.txn_client_id),
                    number: a.txn_number,
                },
                commit_ts: a.commit_ts.map(|(time, cid)| Timestamp {
                    time,
                    client_id: IrClientId(cid),
                }),
            })
        }
        VlogEntryType::QuorumRead => {
            let q: QuorumReadPayloadSer =
                bitcode::deserialize(bytes).map_err(|e| StorageError::Codec(e.to_string()))?;
            let key: K =
                bitcode::deserialize(&q.key).map_err(|e| StorageError::Codec(e.to_string()))?;
            Ok(IrPayloadInline::QuorumRead {
                key,
                timestamp: Timestamp {
                    time: q.time,
                    client_id: IrClientId(q.client_id),
                },
            })
        }
        VlogEntryType::QuorumScan => {
            let q: QuorumScanPayloadSer =
                bitcode::deserialize(bytes).map_err(|e| StorageError::Codec(e.to_string()))?;
            let start_key: K = bitcode::deserialize(&q.start_key)
                .map_err(|e| StorageError::Codec(e.to_string()))?;
            let end_key: K =
                bitcode::deserialize(&q.end_key).map_err(|e| StorageError::Codec(e.to_string()))?;
            Ok(IrPayloadInline::QuorumScan {
                start_key,
                end_key,
                snapshot_ts: Timestamp {
                    time: q.time,
                    client_id: IrClientId(q.client_id),
                },
            })
        }
        VlogEntryType::RaiseMinPrepareTime => {
            let time: u64 =
                bitcode::deserialize(bytes).map_err(|e| StorageError::Codec(e.to_string()))?;
            Ok(IrPayloadInline::RaiseMinPrepareTime { time })
        }
    }
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

#[derive(Serialize, Deserialize)]
struct CommitPayloadSer {
    txn_client_id: u64,
    txn_number: u64,
    commit_time: u64,
    commit_client_id: u64,
    prepare_ref: PrepareRef,
}

#[derive(Serialize, Deserialize)]
struct AbortPayloadSer {
    txn_client_id: u64,
    txn_number: u64,
    commit_ts: Option<(u64, u64)>,
}

#[derive(Serialize, Deserialize)]
struct QuorumReadPayloadSer {
    key: Vec<u8>,
    time: u64,
    client_id: u64,
}

#[derive(Serialize, Deserialize)]
struct QuorumScanPayloadSer {
    start_key: Vec<u8>,
    end_key: Vec<u8>,
    time: u64,
    client_id: u64,
}