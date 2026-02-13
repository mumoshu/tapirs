use super::codec::{FrameCodec, FrameReader};
use super::tcp::TcpStream;
use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;

/// Maximum queued frames per connection before backpressure.
const MAX_WRITE_QUEUE_SIZE: usize = 1000;

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
    pub fn enqueue_frame<T: serde::Serialize>(&mut self, msg: &T) -> Result<(), bitcode::Error> {
        let frame = FrameCodec::encode(msg)?;
        self.write_queue.push_back(frame);
        Ok(())
    }

    /// Queue a frame if capacity available.
    /// Returns Ok(()) on success, Err(()) if queue is full (backpressure).
    pub fn try_enqueue_frame(&mut self, frame: Vec<u8>) -> Result<(), ()> {
        if self.write_queue.len() >= MAX_WRITE_QUEUE_SIZE {
            return Err(()); // Queue full
        }
        self.write_queue.push_back(frame);
        Ok(())
    }

    /// Queue a frame without capacity check (for internal use).
    pub(crate) fn enqueue_frame_unchecked(&mut self, frame: Vec<u8>) {
        self.write_queue.push_back(frame);
    }
}

/// Per-address connection pool.
pub(crate) struct ConnectionPool {
    connections: HashMap<SocketAddr, PooledConnection>,
    /// Addresses currently being connected, with queued frames.
    pub(crate) connecting: HashMap<SocketAddr, Vec<Vec<u8>>>,
    pub(crate) reconnect_attempts: HashMap<SocketAddr, u32>,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_backoff_exponential_growth() {
        let mut pool = ConnectionPool::new();
        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();

        // Verify exponential backoff: 100ms, 200ms, 400ms, 800ms, 1600ms, 3200ms, 5000ms (capped)
        assert_eq!(pool.reconnect_backoff_ms(&addr), 100);
        assert_eq!(pool.reconnect_backoff_ms(&addr), 200);
        assert_eq!(pool.reconnect_backoff_ms(&addr), 400);
        assert_eq!(pool.reconnect_backoff_ms(&addr), 800);
        assert_eq!(pool.reconnect_backoff_ms(&addr), 1600);
        assert_eq!(pool.reconnect_backoff_ms(&addr), 3200);
        assert_eq!(pool.reconnect_backoff_ms(&addr), 5000); // Capped at MAX_BACKOFF_MS
        assert_eq!(pool.reconnect_backoff_ms(&addr), 5000); // Stays capped
    }

    #[test]
    fn test_backoff_reset_on_insert() {
        let mut pool = ConnectionPool::new();
        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();

        // Increment backoff 3 times
        assert_eq!(pool.reconnect_backoff_ms(&addr), 100);
        assert_eq!(pool.reconnect_backoff_ms(&addr), 200);
        assert_eq!(pool.reconnect_backoff_ms(&addr), 400);

        // Simulate successful connection - insert() should reset reconnect_attempts
        pool.insert(addr, PooledConnection::new_outbound());

        // Next failure should start from 100ms again
        assert_eq!(pool.reconnect_backoff_ms(&addr), 100);
    }

    #[test]
    fn test_queue_while_connecting() {
        let mut pool = ConnectionPool::new();
        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();

        // Start connecting
        pool.start_connecting(addr);
        assert!(pool.is_connecting(&addr));

        // Queue frames while connecting
        pool.queue_while_connecting(addr, vec![1, 2, 3]);
        pool.queue_while_connecting(addr, vec![4, 5, 6]);

        // Finish connecting - should return queued frames in FIFO order
        let frames = pool.finish_connecting(addr);
        assert_eq!(frames, vec![vec![1, 2, 3], vec![4, 5, 6]]);
        assert!(!pool.is_connecting(&addr));
    }

    #[test]
    fn test_try_enqueue_backpressure() {
        let mut conn = PooledConnection::new_outbound();

        // Fill to capacity (MAX_WRITE_QUEUE_SIZE = 1000)
        for i in 0..1000 {
            assert!(
                conn.try_enqueue_frame(vec![i as u8; 100]).is_ok(),
                "Frame {i} should enqueue successfully"
            );
        }

        // 1001st frame should fail due to backpressure
        assert!(
            conn.try_enqueue_frame(vec![0u8; 100]).is_err(),
            "Frame 1001 should fail with backpressure"
        );
    }

    #[test]
    fn test_is_connected_lifecycle() {
        let mut pool = ConnectionPool::new();
        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();

        // Initially not connected
        assert!(!pool.is_connected(&addr));

        // Insert connection
        pool.insert(addr, PooledConnection::new_outbound());
        assert!(pool.is_connected(&addr));

        // Remove connection
        pool.remove(&addr);
        assert!(!pool.is_connected(&addr));
    }
}
