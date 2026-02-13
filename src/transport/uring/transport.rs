use super::address::UringAddress;
use super::codec::{FrameCodec, FrameReader};
use super::conn_pool::ConnectionPool;
use super::reactor;
use super::tcp::TcpListener;
use super::wire::{UringIrMessage, WireMessage};
use crate::ir::ReplicaUpcalls;
use crate::{IrMembership, ShardNumber};
use serde::{Serialize, de::DeserializeOwned};
use std::cell::RefCell;
use std::collections::HashMap;
use std::net::SocketAddr;
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

    /// Bind a TCP listener and spawn the accept loop on the reactor.
    pub fn listen(&self, addr: SocketAddr)
    where
        U::UO: Serialize + DeserializeOwned,
        U::UR: Serialize + DeserializeOwned,
        U::IO: Serialize + DeserializeOwned,
        U::CO: Serialize + DeserializeOwned,
        U::CR: Serialize + DeserializeOwned,
    {
        let listener = TcpListener::bind(addr).expect("bind failed");
        let t = self.clone();
        reactor::with_reactor(|r| {
            r.executor.spawn(accept_loop(listener, t));
        });
    }

    /// Dispatch an incoming wire message. Returns a reply frame if any.
    fn dispatch(&self, wire: WireMessage<U>) -> Option<Vec<u8>>
    where
        U::UO: Serialize + DeserializeOwned,
        U::UR: Serialize + DeserializeOwned,
        U::IO: Serialize + DeserializeOwned,
        U::CO: Serialize + DeserializeOwned,
        U::CR: Serialize + DeserializeOwned,
    {
        let state = self.state.borrow();
        match wire {
            WireMessage::Request {
                from,
                request_id,
                payload,
            } => {
                if let Some(ref cb) = state.receive_callback {
                    if let Some(reply) = cb(from, payload) {
                        let reply_wire = WireMessage::<U>::Reply {
                            request_id,
                            payload: reply,
                        };
                        return Some(FrameCodec::encode(&reply_wire));
                    }
                }
                None
            }
            WireMessage::Reply {
                request_id,
                payload,
            } => {
                drop(state);
                let mut state = self.state.borrow_mut();
                if let Some(pending) = state.pending_replies.get_mut(&request_id) {
                    pending.result = Some(payload);
                    if let Some(waker) = pending.waker.take() {
                        waker.wake();
                    }
                }
                None
            }
            WireMessage::FireAndForget { from, payload } => {
                if let Some(ref cb) = state.receive_callback {
                    cb(from, payload);
                }
                None
            }
        }
    }
}

async fn accept_loop<U: ReplicaUpcalls>(
    listener: TcpListener,
    transport: UringTransport<U>,
)
where
    U::UO: Serialize + DeserializeOwned,
    U::UR: Serialize + DeserializeOwned,
    U::IO: Serialize + DeserializeOwned,
    U::CO: Serialize + DeserializeOwned,
    U::CR: Serialize + DeserializeOwned,
{
    loop {
        match listener.accept().await {
            Ok((stream, _peer)) => {
                let t = transport.clone();
                reactor::with_reactor(|r| {
                    r.executor.spawn(read_loop(stream, t));
                });
            }
            Err(e) => {
                eprintln!("accept error: {e}");
            }
        }
    }
}

async fn read_loop<U: ReplicaUpcalls>(
    stream: super::tcp::TcpStream,
    transport: UringTransport<U>,
)
where
    U::UO: Serialize + DeserializeOwned,
    U::UR: Serialize + DeserializeOwned,
    U::IO: Serialize + DeserializeOwned,
    U::CO: Serialize + DeserializeOwned,
    U::CR: Serialize + DeserializeOwned,
{
    let mut reader = FrameReader::new();
    loop {
        let buf = reader.recv_buf();
        let n = match stream.recv(buf).await {
            Ok(0) => break,
            Ok(n) => n,
            Err(_) => break,
        };
        reader.advance(n);

        while let Ok(Some(payload)) = reader.try_read_frame() {
            let wire: WireMessage<U> = match FrameCodec::decode(&payload) {
                Ok(m) => m,
                Err(_) => continue,
            };
            if let Some(reply_frame) = transport.dispatch(wire) {
                if stream.send(&reply_frame).await.is_err() {
                    return;
                }
            }
        }
    }
}
