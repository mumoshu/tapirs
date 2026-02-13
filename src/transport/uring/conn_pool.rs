use super::codec::{FrameCodec, FrameReader};
use super::tcp::TcpStream;
use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;

/// A pooled TCP connection with read/write state.
/// For inbound connections, stream and reader are used.
/// For outbound connections, only write_queue is used (stream owned by connect_and_write task).
pub(crate) struct PooledConnection {
    pub stream: Option<TcpStream>,
    pub reader: FrameReader,
    pub write_queue: VecDeque<Vec<u8>>,
    pub send_in_flight: bool,
}

impl PooledConnection {
    /// Create a pooled connection for inbound (with owned stream).
    pub fn new(stream: TcpStream) -> Self {
        Self {
            stream: Some(stream),
            reader: FrameReader::new(),
            write_queue: VecDeque::new(),
            send_in_flight: false,
        }
    }

    /// Create metadata-only connection for outbound (stream owned by connect_and_write).
    pub fn new_outbound() -> Self {
        Self {
            stream: None,
            reader: FrameReader::new(),
            write_queue: VecDeque::new(),
            send_in_flight: false,
        }
    }

    /// Queue a serialized frame for sending.
    pub fn enqueue_frame<T: serde::Serialize>(&mut self, msg: &T) {
        let frame = FrameCodec::encode(msg);
        self.write_queue.push_back(frame);
    }
}

/// Per-address connection pool.
pub(crate) struct ConnectionPool {
    connections: HashMap<SocketAddr, PooledConnection>,
    /// Addresses currently being connected, with queued frames.
    connecting: HashMap<SocketAddr, Vec<Vec<u8>>>,
    reconnect_attempts: HashMap<SocketAddr, u32>,
}

const MAX_BACKOFF_MS: u64 = 5000;
const BASE_BACKOFF_MS: u64 = 100;

impl ConnectionPool {
    pub fn new() -> Self {
        Self {
            connections: HashMap::new(),
            connecting: HashMap::new(),
            reconnect_attempts: HashMap::new(),
        }
    }

    /// Get an existing connection, or None if not connected.
    pub fn get_mut(
        &mut self,
        addr: &SocketAddr,
    ) -> Option<&mut PooledConnection> {
        self.connections.get_mut(addr)
    }

    /// Insert a newly-established connection.
    pub fn insert(&mut self, addr: SocketAddr, conn: PooledConnection) {
        self.reconnect_attempts.remove(&addr);
        self.connections.insert(addr, conn);
    }

    /// Check if we are currently connecting to this address.
    pub fn is_connecting(&self, addr: &SocketAddr) -> bool {
        self.connecting.contains_key(addr)
    }

    /// Mark address as connecting and initialize the queue.
    pub fn start_connecting(&mut self, addr: SocketAddr) {
        self.connecting.entry(addr).or_default();
    }

    /// Queue a frame to send once connection is established.
    pub fn queue_while_connecting(
        &mut self,
        addr: SocketAddr,
        frame: Vec<u8>,
    ) {
        if let Some(queue) = self.connecting.get_mut(&addr) {
            queue.push(frame);
        }
    }

    /// Finalize connecting: drain queued frames into the connection.
    pub fn finish_connecting(
        &mut self,
        addr: SocketAddr,
    ) -> Vec<Vec<u8>> {
        self.connecting.remove(&addr).unwrap_or_default()
    }

    /// Remove a dead connection.
    pub fn remove(&mut self, addr: &SocketAddr) {
        self.connections.remove(addr);
    }

    /// Get the backoff duration for reconnecting (exponential backoff).
    pub fn reconnect_backoff_ms(&mut self, addr: &SocketAddr) -> u64 {
        let attempts = self
            .reconnect_attempts
            .entry(*addr)
            .or_insert(0);
        *attempts += 1;
        let backoff = BASE_BACKOFF_MS * (1u64 << (*attempts - 1).min(6));
        backoff.min(MAX_BACKOFF_MS)
    }

    /// Check if an address has a live connection.
    pub fn is_connected(&self, addr: &SocketAddr) -> bool {
        self.connections.contains_key(addr)
    }
}
