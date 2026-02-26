use super::codec::{FrameCodec, FrameReader};
use super::tcp::TcpStream;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::{BTreeMap, VecDeque};
use std::net::SocketAddr;
use std::time::Instant;

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
    connections: BTreeMap<SocketAddr, PooledConnection>,
    /// Addresses currently being connected, with queued frames.
    pub(crate) connecting: BTreeMap<SocketAddr, Vec<Vec<u8>>>,
    pub(crate) reconnect_attempts: BTreeMap<SocketAddr, u32>,
    /// Circuit breaker: addresses with open circuits and when they opened.
    circuit_open: BTreeMap<SocketAddr, Instant>,
    /// Circuit breaker: consecutive connection failures per address.
    consecutive_failures: BTreeMap<SocketAddr, u32>,
    /// Optional RNG for jitter (deterministic testing when seeded).
    jitter_rng: Option<StdRng>,
}

const MAX_BACKOFF_MS: u64 = 5000;
const BASE_BACKOFF_MS: u64 = 100;
const CIRCUIT_BREAKER_THRESHOLD: u32 = 10;
const CIRCUIT_BREAKER_COOLDOWN_MS: u64 = 30_000;

impl ConnectionPool {
    pub fn new() -> Self {
        Self {
            connections: BTreeMap::new(),
            connecting: BTreeMap::new(),
            reconnect_attempts: BTreeMap::new(),
            circuit_open: BTreeMap::new(),
            consecutive_failures: BTreeMap::new(),
            jitter_rng: None,
        }
    }

    /// Create a connection pool with jitter enabled (uses seeded RNG for deterministic testing).
    pub fn new_with_jitter(seed: u64) -> Self {
        Self {
            connections: BTreeMap::new(),
            connecting: BTreeMap::new(),
            reconnect_attempts: BTreeMap::new(),
            circuit_open: BTreeMap::new(),
            consecutive_failures: BTreeMap::new(),
            jitter_rng: Some(StdRng::seed_from_u64(seed)),
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
        self.record_connect_success(&addr);
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

    /// Get the backoff duration for reconnecting (exponential backoff with jitter).
    pub fn reconnect_backoff_ms(&mut self, addr: &SocketAddr) -> u64 {
        let attempts = self
            .reconnect_attempts
            .entry(*addr)
            .or_insert(0);
        *attempts += 1;
        let base_backoff = BASE_BACKOFF_MS * (1u64 << (*attempts - 1).min(6));
        let capped_backoff = base_backoff.min(MAX_BACKOFF_MS);

        // Add jitter: ±25% random variance
        if let Some(rng) = &mut self.jitter_rng {
            let jitter_range = (capped_backoff / 4) as i64; // 25% of backoff
            let jitter = rng.gen_range(-jitter_range..=jitter_range);
            let jittered = (capped_backoff as i64 + jitter).max(0) as u64;
            jittered
        } else {
            capped_backoff
        }
    }

    /// Check if an address has a live connection.
    pub fn is_connected(&self, addr: &SocketAddr) -> bool {
        self.connections.contains_key(addr)
    }

    /// Check if the circuit breaker is open for an address (in cooldown period).
    pub fn is_circuit_open(&self, addr: &SocketAddr) -> bool {
        if let Some(&opened_at) = self.circuit_open.get(addr) {
            let elapsed = Instant::now().duration_since(opened_at);
            elapsed.as_millis() < CIRCUIT_BREAKER_COOLDOWN_MS as u128
        } else {
            false
        }
    }

    /// Record a connection failure. Opens circuit breaker after threshold failures.
    pub fn record_connect_failure(&mut self, addr: &SocketAddr) {
        let failures = self.consecutive_failures.entry(*addr).or_insert(0);
        *failures += 1;
        if *failures >= CIRCUIT_BREAKER_THRESHOLD {
            self.circuit_open.insert(*addr, Instant::now());
        }
    }

    /// Record a successful connection. Resets circuit breaker state.
    pub fn record_connect_success(&mut self, addr: &SocketAddr) {
        self.consecutive_failures.remove(addr);
        self.circuit_open.remove(addr);
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

    #[test]
    fn test_pool_growth_multiple_addresses() {
        let mut pool = ConnectionPool::new();
        let addr1: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let addr2: SocketAddr = "127.0.0.1:8081".parse().unwrap();
        let addr3: SocketAddr = "127.0.0.1:8082".parse().unwrap();

        // Connect to addr1 (succeeds)
        pool.insert(addr1, PooledConnection::new_outbound());
        assert!(pool.is_connected(&addr1));

        // Connect to addr2 (succeeds)
        pool.insert(addr2, PooledConnection::new_outbound());
        assert!(pool.is_connected(&addr2));

        // Connect to addr3 (fails - simulate by starting connect and incrementing backoff)
        pool.start_connecting(addr3);
        assert!(pool.is_connecting(&addr3));
        assert_eq!(pool.reconnect_backoff_ms(&addr3), 100); // First attempt

        // Verify all three addresses tracked independently
        assert!(pool.is_connected(&addr1));
        assert!(pool.is_connected(&addr2));
        assert!(pool.is_connecting(&addr3));
        assert!(!pool.is_connected(&addr3));

        // Verify backoff for addr3 is independent (doesn't affect addr1, addr2)
        assert!(!pool.reconnect_attempts.contains_key(&addr1));
        assert!(!pool.reconnect_attempts.contains_key(&addr2));
        assert_eq!(pool.reconnect_attempts.get(&addr3), Some(&1));
    }

    #[test]
    fn test_dead_connection_removal_and_respawn() {
        let mut pool = ConnectionPool::new();
        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();

        // Establish connection
        pool.insert(addr, PooledConnection::new_outbound());
        assert!(pool.is_connected(&addr));
        assert!(!pool.is_connecting(&addr));

        // Simulate connection failure - remove connection
        pool.remove(&addr);
        assert!(!pool.is_connected(&addr));

        // Simulate reconnect attempt - start connecting and increment backoff
        pool.start_connecting(addr);
        assert!(pool.is_connecting(&addr));
        assert_eq!(pool.reconnect_backoff_ms(&addr), 100); // First reconnect attempt

        // Verify state after reconnect initiation
        assert!(!pool.is_connected(&addr)); // Not yet connected
        assert!(pool.is_connecting(&addr)); // But connecting
    }

    #[test]
    fn test_concurrent_send_single_connect() {
        let mut pool = ConnectionPool::new();
        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();

        // Simulate first send - initiates connect
        pool.start_connecting(addr);
        pool.queue_while_connecting(addr, vec![1, 2, 3]);

        // Simulate concurrent sends to same address (while still connecting)
        pool.queue_while_connecting(addr, vec![4, 5, 6]);
        pool.queue_while_connecting(addr, vec![7, 8, 9]);

        // Verify only one connect task (simulated by single connecting entry)
        assert!(pool.is_connecting(&addr));
        assert_eq!(pool.connecting.get(&addr).map(|q| q.len()), Some(3));

        // Verify backoff counter only incremented once (single connect task)
        assert_eq!(pool.reconnect_attempts.get(&addr), None); // Not failed yet

        // Finish connecting - all frames should be queued in FIFO order
        let frames = pool.finish_connecting(addr);
        assert_eq!(
            frames,
            vec![vec![1, 2, 3], vec![4, 5, 6], vec![7, 8, 9]]
        );
    }

    #[test]
    fn test_timeout_gives_up_on_connect() {
        let mut pool = ConnectionPool::new();
        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let connect_timeout_ms = 3000u64;

        // Simulate connection attempts with backoff until timeout
        // Attempt 1: 100ms (total: 100ms < 3000ms) - continue
        pool.start_connecting(addr);
        let backoff1 = pool.reconnect_backoff_ms(&addr);
        assert_eq!(backoff1, 100);
        let elapsed_after_1 = backoff1;
        assert!(elapsed_after_1 < connect_timeout_ms);

        // Attempt 2: 200ms (total: 300ms < 3000ms) - continue
        let backoff2 = pool.reconnect_backoff_ms(&addr);
        assert_eq!(backoff2, 200);
        let elapsed_after_2 = elapsed_after_1 + backoff2;
        assert!(elapsed_after_2 < connect_timeout_ms);

        // Attempt 3: 400ms (total: 700ms < 3000ms) - continue
        let backoff3 = pool.reconnect_backoff_ms(&addr);
        assert_eq!(backoff3, 400);
        let elapsed_after_3 = elapsed_after_2 + backoff3;
        assert!(elapsed_after_3 < connect_timeout_ms);

        // Attempt 4: 800ms (total: 1500ms < 3000ms) - continue
        let backoff4 = pool.reconnect_backoff_ms(&addr);
        assert_eq!(backoff4, 800);
        let elapsed_after_4 = elapsed_after_3 + backoff4;
        assert!(elapsed_after_4 < connect_timeout_ms);

        // Attempt 5: 1600ms (total: 3100ms > 3000ms) - should give up
        let backoff5 = pool.reconnect_backoff_ms(&addr);
        assert_eq!(backoff5, 1600);
        let elapsed_after_5 = elapsed_after_4 + backoff5;
        assert!(elapsed_after_5 > connect_timeout_ms);

        // Simulate giving up - clean up connecting state
        pool.connecting.remove(&addr);
        assert!(!pool.is_connecting(&addr));
        assert!(!pool.is_connected(&addr));
    }

    #[test]
    fn test_jitter_adds_variance() {
        let mut pool = ConnectionPool::new_with_jitter(42);
        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();

        // Collect multiple backoffs for same attempt level
        let mut backoffs = Vec::new();
        for _ in 0..10 {
            let mut test_pool = ConnectionPool::new_with_jitter(42 + backoffs.len() as u64);
            let backoff = test_pool.reconnect_backoff_ms(&addr);
            backoffs.push(backoff);
        }

        // Verify jitter adds variance (not all the same)
        let min = *backoffs.iter().min().unwrap();
        let max = *backoffs.iter().max().unwrap();
        assert!(max > min, "Jitter should create variance, got min={}, max={}", min, max);

        // Verify jitter stays within ±25% of base (100ms)
        // With jitter, range is [75ms, 125ms]
        assert!(min >= 75, "Min backoff {} should be >= 75ms", min);
        assert!(max <= 125, "Max backoff {} should be <= 125ms", max);
    }

    #[test]
    fn test_jitter_disabled_by_default() {
        let mut pool = ConnectionPool::new(); // No jitter
        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();

        // Without jitter, backoff should be deterministic
        let backoff1 = pool.reconnect_backoff_ms(&addr);
        let mut pool2 = ConnectionPool::new();
        let backoff2 = pool2.reconnect_backoff_ms(&addr);

        assert_eq!(backoff1, backoff2, "Without jitter, backoffs should be identical");
        assert_eq!(backoff1, 100, "First backoff should be 100ms");
    }

    #[test]
    fn test_circuit_breaker_opens_after_threshold() {
        let mut pool = ConnectionPool::new();
        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();

        // Circuit should be closed initially
        assert!(!pool.is_circuit_open(&addr));

        // Record 9 failures - circuit should still be closed
        for _ in 0..9 {
            pool.record_connect_failure(&addr);
            assert!(!pool.is_circuit_open(&addr), "Circuit should remain closed before threshold");
        }

        // 10th failure - circuit should open
        pool.record_connect_failure(&addr);
        assert!(pool.is_circuit_open(&addr), "Circuit should open after 10 failures");
    }

    #[tokio::test] // No start_paused - Instant::now() uses wall clock
    async fn test_circuit_breaker_cooldown() {
        let mut pool = ConnectionPool::new();
        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();

        // Open circuit breaker
        for _ in 0..10 {
            pool.record_connect_failure(&addr);
        }
        assert!(pool.is_circuit_open(&addr));

        // Wait 100ms - circuit should still be open (cooldown is 30s)
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        assert!(pool.is_circuit_open(&addr), "Circuit should remain open during cooldown");

        // Manually advance time by modifying circuit_open timestamp (simulate 31s elapsed)
        let opened_at = pool.circuit_open.get(&addr).unwrap();
        let past = *opened_at - tokio::time::Duration::from_secs(31);
        pool.circuit_open.insert(addr, past);

        // Now circuit should be closed
        assert!(!pool.is_circuit_open(&addr), "Circuit should close after cooldown");
    }

    #[test]
    fn test_circuit_breaker_resets_on_success() {
        let mut pool = ConnectionPool::new();
        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();

        // Record 9 failures
        for _ in 0..9 {
            pool.record_connect_failure(&addr);
        }

        // Verify circuit is still closed but has failures tracked
        assert!(!pool.is_circuit_open(&addr));
        assert_eq!(pool.consecutive_failures.get(&addr), Some(&9));

        // Successful connection should reset circuit breaker state
        pool.record_connect_success(&addr);
        assert!(!pool.is_circuit_open(&addr));
        assert_eq!(pool.consecutive_failures.get(&addr), None);

        // Next failure should start from 0 again
        pool.record_connect_failure(&addr);
        assert_eq!(pool.consecutive_failures.get(&addr), Some(&1));
        assert!(!pool.is_circuit_open(&addr));
    }

    #[test]
    fn test_insert_resets_circuit_breaker() {
        let mut pool = ConnectionPool::new();
        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();

        // Open circuit breaker
        for _ in 0..10 {
            pool.record_connect_failure(&addr);
        }
        assert!(pool.is_circuit_open(&addr));

        // insert() should reset circuit breaker (successful connection)
        pool.insert(addr, PooledConnection::new_outbound());
        assert!(!pool.is_circuit_open(&addr));
        assert_eq!(pool.consecutive_failures.get(&addr), None);
    }
}
