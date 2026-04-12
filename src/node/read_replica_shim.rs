//! Thin IR message handler for read-only replicas.
//!
//! Handles `RequestUnlogged` messages (GetAt, ScanAt, Get, Scan) by reading
//! from the `ReadReplica`'s `ArcSwap<TapirHandle>`. All other IR message
//! types return `None` — no consensus participation.

use std::sync::Arc;

use crate::ir::message::{MessageImpl, ReplyUnlogged, RequestUnlogged};
use crate::ir::{Membership, SharedView, View, ViewNumber};
use crate::storage::remote::read_replica::ReadReplica;
use crate::storage::defaults::ProductionTapirReplica;
use crate::tapir::store::TapirStore;
use crate::tapir::{Timestamp, UO, UR};
use crate::transport::tokio_bitcode_tcp::wire::TcpIrMessage;
use crate::{IrClientId, TcpAddress};

pub struct ReadReplicaShim {
    replica: Arc<ReadReplica>,
    /// Dummy view returned in ReplyUnlogged. Clients use view.number to
    /// track view progression; we set it to 0 since read replicas don't
    /// do view changes.
    view: SharedView<TcpAddress>,
}

impl ReadReplicaShim {
    pub fn new(replica: Arc<ReadReplica>, listen_addr: TcpAddress) -> Self {
        let membership = Membership::new(vec![listen_addr]);
        let view = SharedView::new(View {
            membership,
            number: ViewNumber(0),
            app_config: None,
        });
        Self { replica, view }
    }

    /// Transport receive callback. Handles RequestUnlogged only.
    /// Returns None for all other message types (no consensus).
    pub fn receive(
        &self,
        _from: TcpAddress,
        message: TcpIrMessage<ProductionTapirReplica>,
    ) -> Option<TcpIrMessage<ProductionTapirReplica>> {
        match message {
            MessageImpl::RequestUnlogged(RequestUnlogged { op }) => {
                let h = self.replica.load_handle();
                let result = match op {
                    UO::GetAt { key, timestamp } => {
                        let (v, ts) = h.do_uncommitted_get_at(&key, timestamp).ok()?;
                        UR::GetAt(v, ts)
                    }
                    UO::Get { key } => {
                        let (v, ts) = h.do_uncommitted_get(&key).ok()?;
                        UR::Get(v, ts)
                    }
                    UO::ScanAt {
                        start_key,
                        end_key,
                        timestamp,
                    } => {
                        let results =
                            h.do_uncommitted_scan(&start_key, &end_key, timestamp).ok()?;
                        let max_ts =
                            results.iter().map(|(_, _, t)| *t).max().unwrap_or_default();
                        let pairs = results.into_iter().map(|(k, v, _)| (k, v)).collect();
                        UR::ScanAt(pairs, max_ts)
                    }
                    UO::Scan {
                        start_key,
                        end_key,
                    } => {
                        let ts = Timestamp {
                            time: u64::MAX,
                            client_id: IrClientId(u64::MAX),
                        };
                        let results =
                            h.do_uncommitted_scan(&start_key, &end_key, ts).ok()?;
                        let max_ts =
                            results.iter().map(|(_, _, t)| *t).max().unwrap_or_default();
                        let pairs = results.into_iter().map(|(k, v, _)| (k, v)).collect();
                        UR::Scan(pairs, max_ts)
                    }
                    // All other UO variants: not supported on read replicas.
                    _ => return None,
                };
                Some(MessageImpl::ReplyUnlogged(ReplyUnlogged {
                    result,
                    view: self.view.clone(),
                }))
            }
            // All non-unlogged messages: no consensus, no reply.
            _ => None,
        }
    }
}
