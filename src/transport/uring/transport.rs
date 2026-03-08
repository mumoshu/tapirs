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
use std::collections::BTreeMap;
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
    pub pending_replies: BTreeMap<u64, PendingReply<U>>,
    pub next_request_id: u64,
    pub shard_directory: BTreeMap<ShardNumber, IrMembership<UringAddress>>,
    pub connect_timeout_ms: u64,
    pub request_timeout_ms: u64,
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
    pub fn new(address: UringAddress) -> Self {
        Self::new_with_config(address, 5000, 30000)
    }

    pub fn new_with_config(
        address: UringAddress,
        connect_timeout_ms: u64,
        request_timeout_ms: u64,
    ) -> Self {
        Self {
            address,
            state: Rc::new(RefCell::new(TransportState {
                conn_pool: ConnectionPool::new(),
                receive_callback: None,
                pending_replies: BTreeMap::new(),
                next_request_id: 0,
                shard_directory: BTreeMap::new(),
                connect_timeout_ms,
                request_timeout_ms,
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
        U::IR: Serialize + DeserializeOwned,
        U::CO: Serialize + DeserializeOwned,
        U::CR: Serialize + DeserializeOwned,
        U::Payload: Serialize + DeserializeOwned,
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
        U::IR: Serialize + DeserializeOwned,
        U::CO: Serialize + DeserializeOwned,
        U::CR: Serialize + DeserializeOwned,
        U::Payload: Serialize + DeserializeOwned,
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
                        return match FrameCodec::encode(&reply_wire) {
                            Ok(frame) => Some(frame),
                            Err(e) => {
                                eprintln!("reply encode error: {e}");
                                None // Drop reply on encode error
                            }
                        };
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
    U::IR: Serialize + DeserializeOwned,
    U::CO: Serialize + DeserializeOwned,
    U::CR: Serialize + DeserializeOwned,
    U::Payload: Serialize + DeserializeOwned,
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
    U::IR: Serialize + DeserializeOwned,
    U::CO: Serialize + DeserializeOwned,
    U::CR: Serialize + DeserializeOwned,
    U::Payload: Serialize + DeserializeOwned,
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
                // Send reply - loop until all bytes sent
                let mut sent = 0;
                while sent < reply_frame.len() {
                    match stream.send(&reply_frame[sent..]).await {
                        Ok(n) => sent += n,
                        Err(_) => return,
                    }
                }
            }
        }
    }
}

/// Read loop for borrowed streams (outbound connections).
/// Only reads - does not send replies (outbound connections don't reply to requests).
async fn read_loop_borrowed<U: ReplicaUpcalls>(
    stream: super::tcp::BorrowedTcpStream,
    transport: UringTransport<U>,
)
where
    U::UO: Serialize + DeserializeOwned,
    U::UR: Serialize + DeserializeOwned,
    U::IO: Serialize + DeserializeOwned,
    U::IR: Serialize + DeserializeOwned,
    U::CO: Serialize + DeserializeOwned,
    U::CR: Serialize + DeserializeOwned,
    U::Payload: Serialize + DeserializeOwned,
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
            // Dispatch the message (will handle Reply messages for pending_replies)
            // Outbound connections don't send replies back, so ignore return value
            let _ = transport.dispatch(wire);
        }
    }
}

pub(crate) async fn connect_and_write<U: ReplicaUpcalls>(
    addr: SocketAddr,
    transport: UringTransport<U>,
)
where
    U::UO: Serialize + DeserializeOwned,
    U::UR: Serialize + DeserializeOwned,
    U::IO: Serialize + DeserializeOwned,
    U::IR: Serialize + DeserializeOwned,
    U::CO: Serialize + DeserializeOwned,
    U::CR: Serialize + DeserializeOwned,
    U::Payload: Serialize + DeserializeOwned,
{
    use std::time::Duration;

    // 1. Attempt connection with backoff and timeout
    let connect_start = std::time::Instant::now();
    let timeout_ms = {
        let state = transport.state.borrow();
        state.connect_timeout_ms
    };

    let stream = loop {
        match super::tcp::TcpStream::connect(addr).await {
            Ok(s) => break s,
            Err(_) => {
                // Check timeout
                if connect_start.elapsed().as_millis() as u64 > timeout_ms {
                    eprintln!("connect timeout after {timeout_ms}ms to {addr}");
                    // Clean up connecting state
                    let mut state = transport.state.borrow_mut();
                    state.conn_pool.connecting.remove(&addr);
                    return; // Give up
                }

                let backoff_ms = {
                    let mut state = transport.state.borrow_mut();
                    state.conn_pool.reconnect_backoff_ms(&addr)
                };
                super::timer::UringSleep::new(Duration::from_millis(backoff_ms)).await;
                continue;
            }
        }
    };

    // 2. Connection established - mark connected and drain connecting queue
    let queued_frames = {
        let mut state = transport.state.borrow_mut();
        let frames = state.conn_pool.finish_connecting(addr);
        // Insert outbound connection metadata (NO stream - we own it)
        state.conn_pool.insert(addr, super::conn_pool::PooledConnection::new_outbound());
        // Reset reconnect attempts on successful connect
        state.conn_pool.reconnect_attempts.remove(&addr);
        frames
    };

    // 3. Push queued frames to write_queue
    for frame in queued_frames {
        let mut state = transport.state.borrow_mut();
        if let Some(conn) = state.conn_pool.get_mut(&addr) {
            conn.write_queue.push_back(frame);
        }
    }

    // 4. Spawn read_loop on this outbound connection to receive replies
    // Use BorrowedTcpStream so it doesn't double-close the fd
    let read_stream = super::tcp::BorrowedTcpStream { fd: stream.fd };
    let t_read = transport.clone();
    reactor::with_reactor(|r| {
        r.executor.spawn(read_loop_borrowed(read_stream, t_read));
    });

    // 5. Enter write loop - drain write_queue continuously
    // We own 'stream', so no aliasing issues
    loop {
        // Get next frame from queue (short borrow)
        let frame_opt = {
            let mut state = transport.state.borrow_mut();
            state.conn_pool.get_mut(&addr)
                .and_then(|conn| conn.write_queue.pop_front())
        };

        match frame_opt {
            Some(data) => {
                // Send using our owned stream - loop until all bytes sent
                let mut sent = 0;
                while sent < data.len() {
                    match stream.send(&data[sent..]).await {
                        Ok(n) => {
                            sent += n;
                        }
                        Err(_) => {
                            // Send failed - connection dead
                            {
                                let mut state = transport.state.borrow_mut();
                                state.conn_pool.remove(&addr);
                            }

                            // Respawn connect_and_write with backoff
                            let t = transport.clone();
                            reactor::with_reactor(|r| {
                                r.executor.spawn(connect_and_write(addr, t));
                            });
                            return;
                            // stream drops here, closing the fd
                            // read_loop will get error and exit
                        }
                    }
                }
                // All bytes sent successfully - continue
            }
            None => {
                // Queue empty - check if still connected
                let still_connected = {
                    let state = transport.state.borrow();
                    state.conn_pool.is_connected(&addr)
                };
                if !still_connected {
                    // Connection was removed - exit
                    return;
                }
                // Sleep briefly to avoid busy spin
                super::timer::UringSleep::new(Duration::from_millis(1)).await;
            }
        }
    }
    // When loop exits, stream drops and closes fd
}
