use serde::{Deserialize, Serialize};
use crate::{IrRecord, IrSharedView, TapirReplica, TcpAddress};

/// A shard backup containing the IR record and view.
///
/// The record holds all IR operations (consensus: Prepare with results;
/// inconsistent: Commit/Abort). When fed to a fresh replica via
/// IrClient::bootstrap_record() -> BootstrapRecord -> StartView -> sync(),
/// TAPIR replays all operations to reconstruct OCC + MVCC state.
///
/// The backup reflects state as of the last completed view change.
/// In TAPIR's leaderless design, the IR record is NOT consistent
/// across intra-shard replicas until a view change merges their
/// divergent records. Operations committed after the last view change
/// may not be captured. Force a view change before backup to ensure
/// the most up-to-date state.
#[derive(Serialize, Deserialize)]
pub struct ShardBackup {
    pub record: IrRecord<TapirReplica<String, String>>,
    pub view: IrSharedView<TcpAddress>,
}
