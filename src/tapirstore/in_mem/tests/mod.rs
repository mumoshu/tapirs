mod cdc_deltas;
mod fast_path;
mod min_prepare_time;
mod prepare_abort;
mod prepare_commit;
mod prepared_queries;
mod quorum_read_scan;
mod resharding;
mod transaction_log;

use crate::mvcc::disk::{DiskStore, disk_io::BufferedIo};
use crate::occ::{ScanEntry, SharedTransaction, Transaction, TransactionId};
use crate::tapir::{ShardNumber, Sharded, Timestamp};
use crate::tapirstore::InMemTapirStore;
use crate::IrClientId;
use std::sync::Arc;
use tempfile::TempDir;

type TS = Timestamp;
type Backend = DiskStore<String, String, TS, BufferedIo>;
type TestStore = InMemTapirStore<String, String, Backend>;

fn ts(time: u64, client_id: u64) -> TS {
    Timestamp {
        time,
        client_id: IrClientId(client_id),
    }
}

fn txn_id(client: u64, num: u64) -> TransactionId {
    TransactionId {
        client_id: IrClientId(client),
        number: num,
    }
}

fn sharded(key: &str) -> Sharded<String> {
    Sharded {
        shard: ShardNumber(0),
        key: key.to_string(),
    }
}

fn new_store() -> (TempDir, TestStore) {
    let dir = TempDir::new().unwrap();
    let backend = DiskStore::open(dir.path().to_path_buf()).unwrap();
    (dir, InMemTapirStore::new_with_backend(ShardNumber(0), true, backend))
}

fn make_txn(
    reads: Vec<(&str, TS)>,
    writes: Vec<(&str, Option<&str>)>,
    scans: Vec<ScanEntry<String, TS>>,
) -> SharedTransaction<String, String, TS> {
    let mut txn = Transaction::<String, String, TS>::default();
    for (key, timestamp) in reads {
        txn.add_read(sharded(key), timestamp);
    }
    for (key, value) in writes {
        txn.add_write(sharded(key), value.map(|v| v.to_string()));
    }
    txn.scan_set = scans;
    Arc::new(txn)
}
