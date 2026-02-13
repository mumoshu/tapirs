use super::{Channel, TapirTransport, Transport};
use crate::{
    tapir::{Key, Value},
    IrMembership, IrMessage, IrReplicaUpcalls, ShardNumber, TapirReplica,
};
use rand::{seq::SliceRandom, Rng, SeedableRng};
use rand::rngs::StdRng;
use rand::distributions::{Distribution, Uniform};
use rand_distr::Normal;
use serde::{de::DeserializeOwned, Serialize};
use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
    future::Future,
    sync::{Arc, RwLock},
    time::Duration,
};

/// Wrapper around ChannelTransport that provides configurable, seeded fault injection
/// for deterministic network simulation.
pub struct FaultyChannelTransport<U: IrReplicaUpcalls> {
    inner: Channel<U>,
    state: Arc<RwLock<FaultState<U>>>,
}

impl<U: IrReplicaUpcalls> Clone for FaultyChannelTransport<U> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            state: Arc::clone(&self.state),
        }
    }
}

struct FaultState<U: IrReplicaUpcalls> {
    config: NetworkFaultConfig,
    rng: StdRng,
    reorder_buffers: HashMap<(usize, usize), ReorderBuffer<U>>,
}

/// Configuration for network fault injection
#[derive(Clone, Debug)]
pub struct NetworkFaultConfig {
    /// Global message drop probability (0.0-1.0)
    pub drop_rate: f64,
    /// Probability of sending a message twice (0.0-1.0)
    pub duplicate_rate: f64,
    /// Buffer size for message reordering (0 = disabled)
    pub reorder_buffer_size: usize,
    /// Latency injection configuration
    pub latency: LatencyConfig,
    /// Blocked node pairs (symmetric)
    pub partition_pairs: HashSet<(usize, usize)>,
    /// Per-node clock skew in nanoseconds
    pub clock_skew_nanos: i64,
}

impl Default for NetworkFaultConfig {
    fn default() -> Self {
        Self {
            drop_rate: 0.0,
            duplicate_rate: 0.0,
            reorder_buffer_size: 0,
            latency: LatencyConfig::None,
            partition_pairs: HashSet::new(),
            clock_skew_nanos: 0,
        }
    }
}

/// Latency injection configuration
#[derive(Clone, Debug)]
pub enum LatencyConfig {
    None,
    Fixed(Duration),
    Uniform { min: Duration, max: Duration },
    Normal { mean: Duration, stddev: Duration },
}

struct ReorderBuffer<U: IrReplicaUpcalls> {
    messages: Vec<IrMessage<U, Channel<U>>>,
}

impl<U: IrReplicaUpcalls> ReorderBuffer<U> {
    fn new() -> Self {
        Self {
            messages: Vec::new(),
        }
    }

    fn push(&mut self, message: IrMessage<U, Channel<U>>) {
        self.messages.push(message);
    }

    fn should_flush(&self, max_size: usize) -> bool {
        self.messages.len() >= max_size
    }

    fn flush(&mut self, rng: &mut StdRng) -> Vec<IrMessage<U, Channel<U>>> {
        self.messages.shuffle(rng);
        self.messages.drain(..).collect()
    }
}

impl<U: IrReplicaUpcalls> FaultyChannelTransport<U> {
    /// Create a new FaultyChannelTransport with the given configuration and seed
    pub fn new(inner: Channel<U>, config: NetworkFaultConfig, seed: u64) -> Self {
        Self {
            inner,
            state: Arc::new(RwLock::new(FaultState {
                config,
                rng: StdRng::seed_from_u64(seed),
                reorder_buffers: HashMap::new(),
            })),
        }
    }

    /// Create a new FaultyChannelTransport with default configuration
    pub fn with_seed(inner: Channel<U>, seed: u64) -> Self {
        Self::new(inner, NetworkFaultConfig::default(), seed)
    }

    // Runtime mutation methods

    pub fn set_drop_rate(&self, rate: f64) {
        assert!((0.0..=1.0).contains(&rate), "drop_rate must be in [0.0, 1.0]");
        self.state.write().unwrap().config.drop_rate = rate;
    }

    pub fn set_duplicate_rate(&self, rate: f64) {
        assert!(
            (0.0..=1.0).contains(&rate),
            "duplicate_rate must be in [0.0, 1.0]"
        );
        self.state.write().unwrap().config.duplicate_rate = rate;
    }

    pub fn set_latency(&self, latency: LatencyConfig) {
        self.state.write().unwrap().config.latency = latency;
    }

    pub fn set_reorder_buffer_size(&self, size: usize) {
        self.state.write().unwrap().config.reorder_buffer_size = size;
    }

    pub fn partition_node(&self, node: usize) {
        let mut state = self.state.write().unwrap();
        // Block all pairs involving this node (assume dense node IDs 0..100)
        for other in 0..100 {
            if other != node {
                state.config.partition_pairs.insert((node, other));
                state.config.partition_pairs.insert((other, node));
            }
        }
    }

    pub fn heal_node(&self, node: usize) {
        let mut state = self.state.write().unwrap();
        state
            .config
            .partition_pairs
            .retain(|(a, b)| *a != node && *b != node);
    }

    pub fn partition_pair(&self, a: usize, b: usize) {
        let mut state = self.state.write().unwrap();
        state.config.partition_pairs.insert((a, b));
        state.config.partition_pairs.insert((b, a));
    }

    pub fn heal_pair(&self, a: usize, b: usize) {
        let mut state = self.state.write().unwrap();
        state.config.partition_pairs.remove(&(a, b));
        state.config.partition_pairs.remove(&(b, a));
    }

    pub fn set_config(&self, config: NetworkFaultConfig) {
        self.state.write().unwrap().config = config;
    }

    pub fn config(&self) -> NetworkFaultConfig {
        self.state.read().unwrap().config.clone()
    }

    pub fn set_clock_skew(&self, skew_nanos: i64) {
        self.state.write().unwrap().config.clock_skew_nanos = skew_nanos;
    }

    // Helper methods for fault injection

    fn should_drop(state: &Arc<RwLock<FaultState<U>>>) -> bool {
        let mut s = state.write().unwrap();
        let rate = s.config.drop_rate;
        s.rng.gen_bool(rate)
    }

    fn should_duplicate(state: &Arc<RwLock<FaultState<U>>>) -> bool {
        let mut s = state.write().unwrap();
        let rate = s.config.duplicate_rate;
        s.rng.gen_bool(rate)
    }

    fn should_buffer(state: &Arc<RwLock<FaultState<U>>>, _from: usize, _to: usize) -> bool {
        let s = state.read().unwrap();
        let buffer_size = s.config.reorder_buffer_size;
        if buffer_size == 0 {
            return false;
        }
        drop(s);

        // 50% chance to buffer if reordering enabled
        let mut s = state.write().unwrap();
        s.rng.gen_bool(0.5)
    }

    fn is_partitioned(state: &Arc<RwLock<FaultState<U>>>, from: usize, to: usize) -> bool {
        state
            .read()
            .unwrap()
            .config
            .partition_pairs
            .contains(&(from, to))
    }

    async fn apply_latency(state: &Arc<RwLock<FaultState<U>>>) {
        let delay = Self::sample_latency(state);
        if !delay.is_zero() {
            <Self as Transport<U>>::sleep(delay).await;
        }
    }

    fn sample_latency(state: &Arc<RwLock<FaultState<U>>>) -> Duration {
        let mut s = state.write().unwrap();
        match &s.config.latency {
            LatencyConfig::None => Duration::ZERO,
            LatencyConfig::Fixed(d) => *d,
            LatencyConfig::Uniform { min, max } => {
                let min_ms = min.as_millis() as u64;
                let max_ms = max.as_millis() as u64;
                if min_ms >= max_ms {
                    return *min;
                }
                let dist = Uniform::new(min_ms, max_ms);
                Duration::from_millis(dist.sample(&mut s.rng))
            }
            LatencyConfig::Normal { mean, stddev } => {
                let mean_ms = mean.as_millis() as f64;
                let stddev_ms = stddev.as_millis() as f64;
                if let Ok(dist) = Normal::new(mean_ms, stddev_ms) {
                    let sample = dist.sample(&mut s.rng).max(0.0);
                    Duration::from_millis(sample as u64)
                } else {
                    *mean
                }
            }
        }
    }

    fn buffer_message(
        state: &Arc<RwLock<FaultState<U>>>,
        from: usize,
        to: usize,
        message: IrMessage<U, Channel<U>>,
    ) {
        let mut s = state.write().unwrap();
        s.reorder_buffers
            .entry((from, to))
            .or_insert_with(ReorderBuffer::new)
            .push(message);
    }

    async fn flush_if_ready(
        state: &Arc<RwLock<FaultState<U>>>,
        inner: &Channel<U>,
        from: usize,
        to: usize,
    ) {
        let should_flush = {
            let s = state.read().unwrap();
            s.reorder_buffers
                .get(&(from, to))
                .map(|buf| buf.should_flush(s.config.reorder_buffer_size))
                .unwrap_or(false)
        };

        if should_flush {
            Self::flush_buffer(state, inner, from, to).await;
        }
    }

    async fn flush_buffer(
        state: &Arc<RwLock<FaultState<U>>>,
        inner: &Channel<U>,
        from: usize,
        to: usize,
    ) {
        let mut messages: Vec<IrMessage<U, Channel<U>>> = {
            let mut s = state.write().unwrap();
            if let Some(buffer) = s.reorder_buffers.get_mut(&(from, to)) {
                std::mem::take(&mut buffer.messages)
            } else {
                Vec::new()
            }
        };

        // Shuffle outside the lock
        if !messages.is_empty() {
            let mut s = state.write().unwrap();
            messages.shuffle(&mut s.rng);
        }

        for msg in messages {
            inner.do_send(to, msg);
        }
    }

    fn spawn_flush_task(
        state: &Arc<RwLock<FaultState<U>>>,
        inner: &Channel<U>,
        _from: usize,
        to: usize,
    ) {
        let state = Arc::clone(state);
        let inner = inner.clone();
        <Self as Transport<U>>::spawn(async move {
            <Self as Transport<U>>::sleep(Duration::from_millis(10)).await;
            Self::flush_buffer(&state, &inner, _from, to).await;
        });
    }

    fn spawn_duplicate(inner: &Channel<U>, address: usize, message: IrMessage<U, Channel<U>>) {
        let inner = inner.clone();
        <Self as Transport<U>>::spawn(async move {
            <Self as Transport<U>>::sleep(Duration::from_millis(1)).await;
            inner.do_send(address, message);
        });
    }

    // Message conversion functions.
    // Both transports have Address = usize, so all message variants are structurally
    // identical. Pattern matching ensures compiler-verified exhaustiveness.
    fn convert_to_inner(msg: IrMessage<U, Self>) -> IrMessage<U, Channel<U>> {
        use crate::ir::message::MessageImpl::*;
        match msg {
            RequestUnlogged(m) => RequestUnlogged(m),
            ReplyUnlogged(m) => ReplyUnlogged(m),
            ProposeInconsistent(m) => ProposeInconsistent(m),
            ProposeConsensus(m) => ProposeConsensus(m),
            ReplyInconsistent(m) => ReplyInconsistent(m),
            ReplyConsensus(m) => ReplyConsensus(m),
            FinalizeInconsistent(m) => FinalizeInconsistent(m),
            FinalizeConsensus(m) => FinalizeConsensus(m),
            Confirm(m) => Confirm(m),
            DoViewChange(m) => DoViewChange(m),
            StartView(m) => StartView(m),
            AddMember(m) => AddMember(m),
            RemoveMember(m) => RemoveMember(m),
            Reconfigure(m) => Reconfigure(m),
        }
    }

    fn convert_from_inner(msg: IrMessage<U, Channel<U>>) -> IrMessage<U, Self> {
        use crate::ir::message::MessageImpl::*;
        match msg {
            RequestUnlogged(m) => RequestUnlogged(m),
            ReplyUnlogged(m) => ReplyUnlogged(m),
            ProposeInconsistent(m) => ProposeInconsistent(m),
            ProposeConsensus(m) => ProposeConsensus(m),
            ReplyInconsistent(m) => ReplyInconsistent(m),
            ReplyConsensus(m) => ReplyConsensus(m),
            FinalizeInconsistent(m) => FinalizeInconsistent(m),
            FinalizeConsensus(m) => FinalizeConsensus(m),
            Confirm(m) => Confirm(m),
            DoViewChange(m) => DoViewChange(m),
            StartView(m) => StartView(m),
            AddMember(m) => AddMember(m),
            RemoveMember(m) => RemoveMember(m),
            Reconfigure(m) => Reconfigure(m),
        }
    }
}

impl<U: IrReplicaUpcalls> Transport<U> for FaultyChannelTransport<U> {
    type Address = usize;
    type Sleep = tokio::time::Sleep;

    fn address(&self) -> Self::Address {
        self.inner.address()
    }

    fn time(&self) -> u64 {
        let base = self.inner.time();
        let skew = self.state.read().unwrap().config.clock_skew_nanos;
        base.saturating_add_signed(skew)
    }

    fn time_offset(&self, offset: i64) -> u64 {
        let base = self.inner.time_offset(offset);
        let skew = self.state.read().unwrap().config.clock_skew_nanos;
        base.saturating_add_signed(skew)
    }

    fn sleep(duration: Duration) -> Self::Sleep {
        Channel::<U>::sleep(duration)
    }

    fn persist<T: Serialize>(&self, key: &str, value: Option<&T>) {
        self.inner.persist(key, value)
    }

    fn persisted<T: DeserializeOwned>(&self, key: &str) -> Option<T> {
        self.inner.persisted(key)
    }

    fn send<R: TryFrom<IrMessage<U, Self>> + Send + Debug>(
        &self,
        address: Self::Address,
        message: impl Into<IrMessage<U, Self>> + Debug,
    ) -> impl Future<Output = R> + Send + 'static {
        let from = self.address();
        let inner = self.inner.clone();
        let state = Arc::clone(&self.state);

        let message_inner = Self::convert_to_inner(message.into());

        async move {
            loop {
                // 1. Check partition
                if Self::is_partitioned(&state, from, address) {
                    <Self as Transport<U>>::sleep(Duration::from_millis(100)).await;
                    continue;
                }

                // 2. Apply latency
                Self::apply_latency(&state).await;

                // 3. Check drop
                if Self::should_drop(&state) {
                    continue; // Retry
                }

                // 4. Handle reordering
                if Self::should_buffer(&state, from, address) {
                    Self::buffer_message(&state, from, address, message_inner.clone());
                    continue;
                }

                // 5. Flush any buffered messages
                Self::flush_if_ready(&state, &inner, from, address).await;

                // 6. Send to inner channel
                let reply: IrMessage<U, Channel<U>> = inner.send(address, message_inner.clone()).await;

                // 7. Handle duplication
                if Self::should_duplicate(&state) {
                    Self::spawn_duplicate(&inner, address, message_inner.clone());
                }

                // 8. Convert reply back and return
                let reply_converted = Self::convert_from_inner(reply);
                break reply_converted
                    .try_into()
                    .unwrap_or_else(|_| panic!("Reply type mismatch"));
            }
        }
    }

    fn do_send(&self, address: Self::Address, message: impl Into<IrMessage<U, Self>> + Debug) {
        let from = self.address();
        let inner = self.inner.clone();
        let state = Arc::clone(&self.state);

        let message_inner = Self::convert_to_inner(message.into());

        <Self as Transport<U>>::spawn(async move {
            // 1. Check partition
            if Self::is_partitioned(&state, from, address) {
                return; // Drop silently
            }

            // 2. Apply latency
            Self::apply_latency(&state).await;

            // 3. Check drop
            if Self::should_drop(&state) {
                return;
            }

            // 4. Handle reordering
            if Self::should_buffer(&state, from, address) {
                Self::buffer_message(&state, from, address, message_inner.clone());
                Self::spawn_flush_task(&state, &inner, from, address);
                return;
            }

            // 5. Send message
            inner.do_send(address, message_inner.clone());

            // 6. Handle duplicate
            if Self::should_duplicate(&state) {
                Self::spawn_duplicate(&inner, address, message_inner);
            }
        });
    }

    fn spawn(future: impl Future<Output = ()> + Send + 'static) {
        Channel::<U>::spawn(future)
    }
}

impl<K: Key, V: Value> TapirTransport<K, V> for FaultyChannelTransport<TapirReplica<K, V>> {
    fn shard_addresses(
        &self,
        shard: ShardNumber,
    ) -> impl Future<Output = IrMembership<Self::Address>> + Send + 'static {
        self.inner.shard_addresses(shard)
    }

    fn shards_for_range(&self, start: &K, end: &K) -> Vec<ShardNumber> {
        self.inner.shards_for_range(start, end)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::ChannelRegistry;

    type TestUpcalls = TapirReplica<i64, i64>;

    fn setup() -> (
        ChannelRegistry<TestUpcalls>,
        FaultyChannelTransport<TestUpcalls>,
        FaultyChannelTransport<TestUpcalls>,
    ) {
        let registry = ChannelRegistry::<TestUpcalls>::default();

        let ch0 = registry.channel(|_from, _msg| None);
        let ch1 = registry.channel(|_from, _msg| None);

        let faulty0 = FaultyChannelTransport::with_seed(ch0, 42);
        let faulty1 = FaultyChannelTransport::with_seed(ch1, 43);

        (registry, faulty0, faulty1)
    }

    #[test]
    fn test_drop_rate_determinism() {
        // Create two transports with same seed and drop_rate
        let registry = ChannelRegistry::<TestUpcalls>::default();
        let ch1 = registry.channel(|_from, _msg| None);
        let ch2 = registry.channel(|_from, _msg| None);

        let mut config = NetworkFaultConfig::default();
        config.drop_rate = 0.5;

        let faulty1 = FaultyChannelTransport::new(ch1, config.clone(), 12345);
        let faulty2 = FaultyChannelTransport::new(ch2, config, 12345);

        // Sample drops 100 times
        let drops1: Vec<bool> = (0..100)
            .map(|_| FaultyChannelTransport::<TestUpcalls>::should_drop(&faulty1.state))
            .collect();

        let drops2: Vec<bool> = (0..100)
            .map(|_| FaultyChannelTransport::<TestUpcalls>::should_drop(&faulty2.state))
            .collect();

        // Verify identical patterns
        assert_eq!(drops1, drops2);
        // Verify some drops actually happened
        assert!(drops1.iter().filter(|&&x| x).count() > 0);
    }

    #[tokio::test]
    async fn test_partition_heal() {
        let (_registry, faulty0, faulty1) = setup();

        // Initially no partition
        assert!(!FaultyChannelTransport::<TestUpcalls>::is_partitioned(
            &faulty0.state,
            faulty0.address(),
            faulty1.address()
        ));

        // Partition node 0
        faulty0.partition_node(faulty0.address());

        // Verify partition
        assert!(FaultyChannelTransport::<TestUpcalls>::is_partitioned(
            &faulty0.state,
            faulty0.address(),
            faulty1.address()
        ));

        // Heal node 0
        faulty0.heal_node(faulty0.address());

        // Verify healed
        assert!(!FaultyChannelTransport::<TestUpcalls>::is_partitioned(
            &faulty0.state,
            faulty0.address(),
            faulty1.address()
        ));
    }

    #[tokio::test]
    async fn test_latency_injection() {
        let (_registry, faulty, _) = setup();

        // Set fixed latency of 50ms
        // Note: Channel::sleep divides duration by 10, so actual delay will be ~5ms
        faulty.set_latency(LatencyConfig::Fixed(Duration::from_millis(50)));

        let start = std::time::Instant::now();
        FaultyChannelTransport::<TestUpcalls>::apply_latency(&faulty.state).await;
        let elapsed = start.elapsed();

        // Verify delay was applied (Channel divides by 10, so expect ~5ms, allow slack for timer precision)
        assert!(
            elapsed >= Duration::from_millis(4),
            "Expected >= 4ms, got {:?}",
            elapsed
        );
    }

    #[test]
    fn test_clock_skew() {
        let registry = ChannelRegistry::<TestUpcalls>::default();
        let ch = registry.channel(|_from, _msg| None);
        let faulty = FaultyChannelTransport::with_seed(ch.clone(), 42);

        let base_time = ch.time();

        // Set clock skew of 1 second
        faulty.set_clock_skew(1_000_000_000);

        let skewed_time = faulty.time();

        // Verify offset applied (approximately 1s = 1e9 ns)
        let diff = (skewed_time as i64) - (base_time as i64);
        assert!(
            (diff - 1_000_000_000).abs() < 100_000_000,
            "Expected ~1s offset, got {}ns",
            diff
        );
    }

    #[tokio::test]
    async fn test_config_mutation() {
        let (_registry, faulty, _) = setup();

        // Start with drop_rate = 0.0
        faulty.set_drop_rate(0.0);

        // Verify no drops
        let drops_before: Vec<bool> = (0..10)
            .map(|_| FaultyChannelTransport::<TestUpcalls>::should_drop(&faulty.state))
            .collect();
        assert_eq!(drops_before.iter().filter(|&&x| x).count(), 0);

        // Change to drop_rate = 1.0
        faulty.set_drop_rate(1.0);

        // Verify all drops
        let drops_after: Vec<bool> = (0..10)
            .map(|_| FaultyChannelTransport::<TestUpcalls>::should_drop(&faulty.state))
            .collect();
        assert_eq!(drops_after.iter().filter(|&&x| x).count(), 10);
    }

    #[test]
    fn test_reorder_buffer_determinism() {
        // Create two transports with same seed
        let registry = ChannelRegistry::<TestUpcalls>::default();
        let ch1 = registry.channel(|_from, _msg| None);
        let ch2 = registry.channel(|_from, _msg| None);

        let mut config = NetworkFaultConfig::default();
        config.reorder_buffer_size = 5;

        let faulty1 = FaultyChannelTransport::new(ch1, config.clone(), 999);
        let faulty2 = FaultyChannelTransport::new(ch2, config, 999);

        // Sample should_buffer 100 times
        let buffer1: Vec<bool> = (0..100)
            .map(|_| FaultyChannelTransport::<TestUpcalls>::should_buffer(&faulty1.state, 0, 1))
            .collect();

        let buffer2: Vec<bool> = (0..100)
            .map(|_| FaultyChannelTransport::<TestUpcalls>::should_buffer(&faulty2.state, 0, 1))
            .collect();

        // Verify identical patterns
        assert_eq!(buffer1, buffer2);
    }

    #[test]
    fn test_duplicate_rate() {
        let registry = ChannelRegistry::<TestUpcalls>::default();
        let ch = registry.channel(|_from, _msg| None);

        let mut config = NetworkFaultConfig::default();
        config.duplicate_rate = 1.0; // Always duplicate

        let faulty = FaultyChannelTransport::new(ch, config, 42);

        // Verify duplicates always happen
        let duplicates: Vec<bool> = (0..10)
            .map(|_| FaultyChannelTransport::<TestUpcalls>::should_duplicate(&faulty.state))
            .collect();
        assert_eq!(duplicates.iter().filter(|&&x| x).count(), 10);
    }
}
