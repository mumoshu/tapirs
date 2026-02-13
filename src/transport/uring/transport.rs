use super::address::UringAddress;
use super::conn_pool::ConnectionPool;
use super::wire::UringIrMessage;
use crate::ir::ReplicaUpcalls;
use crate::{IrMembership, ShardNumber};
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::task::Waker;

/// Pending reply state for request-reply exchange.
pub(crate) struct PendingReply<U: ReplicaUpcalls> {
    pub result: Option<UringIrMessage<U>>,
    pub waker: Option<Waker>,
}

/// Per-thread transport state (not Send, wrapped in Rc<RefCell>).
pub(crate) struct TransportState<U: ReplicaUpcalls> {
    pub conn_pool: ConnectionPool,
    pub receive_callback: Option<
        Box<dyn Fn(UringAddress, UringIrMessage<U>) -> Option<UringIrMessage<U>>>,
    >,
    pub pending_replies: HashMap<u64, PendingReply<U>>,
    pub next_request_id: u64,
    pub shard_directory: HashMap<ShardNumber, IrMembership<UringAddress>>,
    pub persist_dir: String,
}

/// io_uring-based Transport implementation.
pub struct UringTransport<U: ReplicaUpcalls> {
    pub(crate) address: UringAddress,
    pub(crate) state: Rc<RefCell<TransportState<U>>>,
    #[cfg(debug_assertions)]
    thread_id: std::thread::ThreadId,
}

// SAFETY: UringTransport is created and used exclusively on one core
// thread. The thread-per-core architecture prevents cross-thread access.
unsafe impl<U: ReplicaUpcalls> Send for UringTransport<U> {}
unsafe impl<U: ReplicaUpcalls> Sync for UringTransport<U> {}

impl<U: ReplicaUpcalls> Clone for UringTransport<U> {
    fn clone(&self) -> Self {
        Self {
            address: self.address,
            state: Rc::clone(&self.state),
            #[cfg(debug_assertions)]
            thread_id: self.thread_id,
        }
    }
}

impl<U: ReplicaUpcalls> UringTransport<U> {
    pub fn new(address: UringAddress, persist_dir: String) -> Self {
        Self {
            address,
            state: Rc::new(RefCell::new(TransportState {
                conn_pool: ConnectionPool::new(),
                receive_callback: None,
                pending_replies: HashMap::new(),
                next_request_id: 0,
                shard_directory: HashMap::new(),
                persist_dir,
            })),
            #[cfg(debug_assertions)]
            thread_id: std::thread::current().id(),
        }
    }

    #[cfg(debug_assertions)]
    pub(crate) fn assert_thread(&self) {
        assert_eq!(
            std::thread::current().id(),
            self.thread_id,
            "UringTransport used from wrong thread"
        );
    }

    #[cfg(not(debug_assertions))]
    pub(crate) fn assert_thread(&self) {}
}
