use super::{Channel, TapirTransport, Transport};
use crate::{
    tapir::{Key, Value},
    IrMembership, IrMessage, IrReplicaUpcalls, ShardNumber, TapirReplica,
};
use rand::{seq::SliceRandom, Rng, RngCore, SeedableRng};
use rand::rngs::StdRng;
use rand::distributions::{Distribution, Uniform};
use rand_distr::Normal;
use std::{
    collections::{BTreeMap, BTreeSet},
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
    reorder_buffers: BTreeMap<(usize, usize), ReorderBuffer<U>>,
    partitioned_nodes: BTreeSet<usize>,
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
    pub partition_pairs: BTreeSet<(usize, usize)>,
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
            partition_pairs: BTreeSet::new(),
            clock_skew_nanos: 0,
        }
    }
}

/// Latency injection configuration
#[allow(dead_code)]
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

    #[allow(dead_code)]
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
                reorder_buffers: BTreeMap::new(),
                partitioned_nodes: BTreeSet::new(),
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

    #[allow(dead_code)]
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

    #[allow(dead_code)]
    pub fn set_reorder_buffer_size(&self, size: usize) {
        self.state.write().unwrap().config.reorder_buffer_size = size;
    }

    pub fn partition_node(&self, node: usize) {
        let mut state = self.state.write().unwrap();
        state.partitioned_nodes.insert(node);
        Self::rebuild_partition_pairs(&mut state);
    }

    pub fn heal_node(&self, node: usize) {
        let mut state = self.state.write().unwrap();
        state.partitioned_nodes.remove(&node);
        Self::rebuild_partition_pairs(&mut state);
    }

    fn rebuild_partition_pairs(state: &mut FaultState<U>) {
        state.config.partition_pairs.clear();
        for &partitioned in &state.partitioned_nodes {
            // Block all pairs involving this partitioned node (assume dense node IDs 0..100)
            for other in 0..100 {
                if other != partitioned {
                    state.config.partition_pairs.insert((partitioned, other));
                    state.config.partition_pairs.insert((other, partitioned));
                }
            }
        }
    }

    #[allow(dead_code)]
    pub fn partition_pair(&self, a: usize, b: usize) {
        let mut state = self.state.write().unwrap();
        state.config.partition_pairs.insert((a, b));
        state.config.partition_pairs.insert((b, a));
    }

    #[allow(dead_code)]
    pub fn heal_pair(&self, a: usize, b: usize) {
        let mut state = self.state.write().unwrap();
        state.config.partition_pairs.remove(&(a, b));
        state.config.partition_pairs.remove(&(b, a));
    }

    #[allow(dead_code)]
    pub fn set_config(&self, config: NetworkFaultConfig) {
        self.state.write().unwrap().config = config;
    }

    #[allow(dead_code)]
    pub fn config(&self) -> NetworkFaultConfig {
        self.state.read().unwrap().config.clone()
    }

    pub fn set_shard(&self, shard: ShardNumber) {
        self.inner.set_shard(shard);
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

    // Per-message RNG variants: read config from shared state but use a
    // per-message StdRng for randomness. Each message's RNG is seeded from
    // the shared RNG at send()/do_send() creation time (deterministic sync
    // context), not at async poll time (non-deterministic FuturesUnordered
    // order).

    fn should_drop_rng(state: &Arc<RwLock<FaultState<U>>>, rng: &mut StdRng) -> bool {
        let rate = state.read().unwrap().config.drop_rate;
        rng.gen_bool(rate)
    }

    fn should_duplicate_rng(state: &Arc<RwLock<FaultState<U>>>, rng: &mut StdRng) -> bool {
        let rate = state.read().unwrap().config.duplicate_rate;
        rng.gen_bool(rate)
    }

    fn should_buffer_rng(state: &Arc<RwLock<FaultState<U>>>, rng: &mut StdRng) -> bool {
        let buffer_size = state.read().unwrap().config.reorder_buffer_size;
        if buffer_size == 0 {
            return false;
        }
        rng.gen_bool(0.5)
    }

    fn sample_latency_rng(state: &Arc<RwLock<FaultState<U>>>, rng: &mut StdRng) -> Duration {
        let s = state.read().unwrap();
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
                Duration::from_millis(dist.sample(rng))
            }
            LatencyConfig::Normal { mean, stddev } => {
                let mean_ms = mean.as_millis() as f64;
                let stddev_ms = stddev.as_millis() as f64;
                if let Ok(dist) = Normal::new(mean_ms, stddev_ms) {
                    let sample = dist.sample(rng).max(0.0);
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
        rng: &mut StdRng,
    ) {
        let should_flush = {
            let s = state.read().unwrap();
            s.reorder_buffers
                .get(&(from, to))
                .map(|buf| buf.should_flush(s.config.reorder_buffer_size))
                .unwrap_or(false)
        };

        if should_flush {
            Self::flush_buffer(state, inner, from, to, rng).await;
        }
    }

    async fn flush_buffer(
        state: &Arc<RwLock<FaultState<U>>>,
        inner: &Channel<U>,
        from: usize,
        to: usize,
        rng: &mut StdRng,
    ) {
        let mut messages: Vec<IrMessage<U, Channel<U>>> = {
            let mut s = state.write().unwrap();
            if let Some(buffer) = s.reorder_buffers.get_mut(&(from, to)) {
                std::mem::take(&mut buffer.messages)
            } else {
                Vec::new()
            }
        };

        // Shuffle using per-message RNG (no shared state lock needed)
        if !messages.is_empty() {
            messages.shuffle(rng);
        }

        // Re-check partition state before sending each buffered message
        for msg in messages {
            if !Self::is_partitioned(state, from, to) {
                inner.do_send(to, msg);
            }
            // If partitioned, drop the message silently
        }
    }

    fn spawn_flush_task(
        state: &Arc<RwLock<FaultState<U>>>,
        inner: &Channel<U>,
        _from: usize,
        to: usize,
        flush_seed: u64,
    ) {
        let state = Arc::clone(state);
        let inner = inner.clone();
        <Self as Transport<U>>::spawn(async move {
            let mut flush_rng = StdRng::seed_from_u64(flush_seed);
            <Self as Transport<U>>::sleep(Duration::from_millis(10)).await;
            Self::flush_buffer(&state, &inner, _from, to, &mut flush_rng).await;
        });
    }

    fn spawn_duplicate(
        state: &Arc<RwLock<FaultState<U>>>,
        inner: &Channel<U>,
        from: usize,
        address: usize,
        message: IrMessage<U, Channel<U>>,
    ) {
        let inner = inner.clone();
        let state = Arc::clone(state);
        <Self as Transport<U>>::spawn(async move {
            <Self as Transport<U>>::sleep(Duration::from_millis(1)).await;
            // Re-check partition state before sending duplicate
            if !Self::is_partitioned(&state, from, address) {
                inner.do_send(address, message);
            }
            // If partitioned, drop the duplicate silently
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
            FinalizeInconsistentReply(m) => FinalizeInconsistentReply(m),
            FinalizeConsensus(m) => FinalizeConsensus(m),
            Confirm(m) => Confirm(m),
            DoViewChange(m) => DoViewChange(m),
            StartView(m) => StartView(m),
            AddMember(m) => AddMember(m),
            RemoveMember(m) => RemoveMember(m),
            Reconfigure(m) => Reconfigure(m),
            FetchLeaderRecord(m) => FetchLeaderRecord(m),
            LeaderRecordReply(m) => LeaderRecordReply(m),
            BootstrapRecord(m) => BootstrapRecord(m),
            StatusBroadcast(m) => StatusBroadcast(m),
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
            FinalizeInconsistentReply(m) => FinalizeInconsistentReply(m),
            FinalizeConsensus(m) => FinalizeConsensus(m),
            Confirm(m) => Confirm(m),
            DoViewChange(m) => DoViewChange(m),
            StartView(m) => StartView(m),
            AddMember(m) => AddMember(m),
            RemoveMember(m) => RemoveMember(m),
            Reconfigure(m) => Reconfigure(m),
            FetchLeaderRecord(m) => FetchLeaderRecord(m),
            LeaderRecordReply(m) => LeaderRecordReply(m),
            BootstrapRecord(m) => BootstrapRecord(m),
            StatusBroadcast(m) => StatusBroadcast(m),
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

    fn send<R: TryFrom<IrMessage<U, Self>> + Send + Debug>(
        &self,
        address: Self::Address,
        message: impl Into<IrMessage<U, Self>> + Debug,
    ) -> impl Future<Output = R> + Send + 'static {
        let from = self.address();
        let inner = self.inner.clone();
        let state = Arc::clone(&self.state);

        let message_inner = Self::convert_to_inner(message.into());

        // Sample per-message seed from shared RNG in deterministic sync context.
        // send() is called from Membership::iter() loops (Vec index order), so
        // the sampling order is deterministic regardless of async poll ordering.
        let msg_seed = state.write().unwrap().rng.next_u64();

        async move {
            let mut msg_rng = StdRng::seed_from_u64(msg_seed);
            loop {
                // 1. Check partition
                if Self::is_partitioned(&state, from, address) {
                    <Self as Transport<U>>::sleep(Duration::from_millis(100)).await;
                    continue;
                }

                // 2. Apply latency
                let delay = Self::sample_latency_rng(&state, &mut msg_rng);
                if !delay.is_zero() {
                    <Self as Transport<U>>::sleep(delay).await;
                }

                // 3. Check drop
                if Self::should_drop_rng(&state, &mut msg_rng) {
                    continue; // Retry
                }

                // 4. Handle reordering
                if Self::should_buffer_rng(&state, &mut msg_rng) {
                    Self::buffer_message(&state, from, address, message_inner.clone());
                    continue;
                }

                // 5. Flush any buffered messages
                Self::flush_if_ready(&state, &inner, from, address, &mut msg_rng).await;

                // 6. Send to inner channel
                let reply: IrMessage<U, Channel<U>> = inner.send(address, message_inner.clone()).await;

                // 7. Handle duplication
                if Self::should_duplicate_rng(&state, &mut msg_rng) {
                    Self::spawn_duplicate(&state, &inner, from, address, message_inner.clone());
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

        // Sample per-message seed from shared RNG in deterministic sync context.
        let msg_seed = state.write().unwrap().rng.next_u64();

        <Self as Transport<U>>::spawn(async move {
            let mut msg_rng = StdRng::seed_from_u64(msg_seed);

            // 1. Check partition
            if Self::is_partitioned(&state, from, address) {
                return; // Drop silently
            }

            // 2. Apply latency
            let delay = Self::sample_latency_rng(&state, &mut msg_rng);
            if !delay.is_zero() {
                <Self as Transport<U>>::sleep(delay).await;
            }

            // 3. Check drop
            if Self::should_drop_rng(&state, &mut msg_rng) {
                return;
            }

            // 4. Handle reordering
            if Self::should_buffer_rng(&state, &mut msg_rng) {
                Self::buffer_message(&state, from, address, message_inner.clone());
                let flush_seed = msg_rng.next_u64();
                Self::spawn_flush_task(&state, &inner, from, address, flush_seed);
                return;
            }

            // 5. Send message
            inner.do_send(address, message_inner.clone());

            // 6. Handle duplicate
            if Self::should_duplicate_rng(&state, &mut msg_rng) {
                Self::spawn_duplicate(&state, &inner, from, address, message_inner);
            }
        });
    }

    fn spawn(future: impl Future<Output = ()> + Send + 'static) {
        Channel::<U>::spawn(future)
    }

    fn on_membership_changed(&self, membership: &IrMembership<Self::Address>, view: u64) {
        self.inner.on_membership_changed(membership, view);
    }
}

impl<K: Key, V: Value> TapirTransport<K, V> for FaultyChannelTransport<TapirReplica<K, V>> {
    fn shard_addresses(
        &self,
        shard: ShardNumber,
    ) -> impl Future<Output = IrMembership<Self::Address>> + Send + 'static {
        self.inner.shard_addresses(shard)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::discovery::InMemoryShardDirectory;
    use crate::transport::ChannelRegistry;
    use crate::ir::message::FinalizeInconsistent;
    use crate::ir::{OpId, ClientId};
    use std::sync::Mutex;

    type TestUpcalls = TapirReplica<i64, i64>;

    fn setup() -> (
        ChannelRegistry<TestUpcalls>,
        FaultyChannelTransport<TestUpcalls>,
        FaultyChannelTransport<TestUpcalls>,
    ) {
        let registry = ChannelRegistry::<TestUpcalls>::default();
        let dir = Arc::new(InMemoryShardDirectory::new());

        let ch0 = registry.channel(|_from, _msg| None, Arc::clone(&dir));
        let ch1 = registry.channel(|_from, _msg| None, Arc::clone(&dir));

        let faulty0 = FaultyChannelTransport::with_seed(ch0, 42);
        let faulty1 = FaultyChannelTransport::with_seed(ch1, 43);

        (registry, faulty0, faulty1)
    }

    #[test]
    fn test_drop_rate_determinism() {
        // Create two transports with same seed and drop_rate
        let registry = ChannelRegistry::<TestUpcalls>::default();
        let dir = Arc::new(InMemoryShardDirectory::new());
        let ch1 = registry.channel(|_from, _msg| None, Arc::clone(&dir));
        let ch2 = registry.channel(|_from, _msg| None, Arc::clone(&dir));

        let config = NetworkFaultConfig { drop_rate: 0.5, ..Default::default() };

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
        let dir = Arc::new(InMemoryShardDirectory::new());
        let ch = registry.channel(|_from, _msg| None, Arc::clone(&dir));
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
        let dir = Arc::new(InMemoryShardDirectory::new());
        let ch1 = registry.channel(|_from, _msg| None, Arc::clone(&dir));
        let ch2 = registry.channel(|_from, _msg| None, Arc::clone(&dir));

        let config = NetworkFaultConfig { reorder_buffer_size: 5, ..Default::default() };

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
        let dir = Arc::new(InMemoryShardDirectory::new());
        let ch = registry.channel(|_from, _msg| None, Arc::clone(&dir));

        let config = NetworkFaultConfig { duplicate_rate: 1.0, ..Default::default() }; // Always duplicate

        let faulty = FaultyChannelTransport::new(ch, config, 42);

        // Verify duplicates always happen
        let duplicates: Vec<bool> = (0..10)
            .map(|_| FaultyChannelTransport::<TestUpcalls>::should_duplicate(&faulty.state))
            .collect();
        assert_eq!(duplicates.iter().filter(|&&x| x).count(), 10);
    }

    #[tokio::test]
    async fn fuzz_transport_simulator() {
        let seed: u64 = std::env::var("TAPI_TEST_SEED")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or_else(|| {
                use std::time::SystemTime;
                SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos() as u64
            });
        eprintln!("seed={}", seed);

        // All protocol randomness goes through seeded crate::Rng (since b4d9630),
        // so with start_paused=true and BTreeMap iteration, this is fully deterministic.
        run_fuzz_iteration(seed).await;
    }

    async fn run_fuzz_iteration(seed: u64) {
        let mut rng = StdRng::seed_from_u64(seed);

        // 1. Generate random number of nodes
        let num_nodes = rng.gen_range(3..=7);

        // 2. Generate random NetworkFaultConfig
        let config = NetworkFaultConfig {
            drop_rate: rng.gen_range(0.0..=0.3),
            duplicate_rate: rng.gen_range(0.0..=0.2),
            reorder_buffer_size: rng.gen_range(0..=5),
            latency: match rng.gen_range(0..3) {
                0 => LatencyConfig::None,
                1 => LatencyConfig::Fixed(Duration::from_millis(rng.gen_range(1..=20))),
                _ => LatencyConfig::Uniform {
                    min: Duration::from_millis(1),
                    max: Duration::from_millis(rng.gen_range(2..=50)),
                },
            },
            partition_pairs: BTreeSet::new(),
            clock_skew_nanos: rng.gen_range(-1_000_000_000..=1_000_000_000),
        };

        eprintln!(
            "Config: nodes={}, drop={:.2}, dup={:.2}, reorder={}, latency={:?}, skew={}",
            num_nodes, config.drop_rate, config.duplicate_rate,
            config.reorder_buffer_size, config.latency, config.clock_skew_nanos
        );

        // 3. Create ChannelRegistry and FaultyChannelTransports
        let registry = Arc::new(ChannelRegistry::<TestUpcalls>::default());
        let dir = Arc::new(InMemoryShardDirectory::new());
        let received: Arc<Mutex<Vec<(usize, usize, u64)>>> = Arc::new(Mutex::new(Vec::new()));
        let mut transports = Vec::new();

        for node_id in 0..num_nodes {
            let received_clone: Arc<Mutex<Vec<(usize, usize, u64)>>> = Arc::clone(&received);
            let ch = registry.channel(move |from, msg| {
                // Extract msg_id from the message
                use crate::ir::message::MessageImpl::*;
                if let FinalizeInconsistent(fi) = msg {
                    received_clone
                        .lock()
                        .unwrap()
                        .push((from, node_id, fi.op_id.number));
                }
                None
            }, Arc::clone(&dir));
            let transport = FaultyChannelTransport::new(ch, config.clone(), seed + node_id as u64);
            transports.push(transport);
        }

        // 4. Generate random operation sequence
        let num_ops = rng.gen_range(20..=50);
        let mut operations = Vec::new();

        #[derive(Debug, Clone)]
        enum Operation {
            Send { from: usize, to: usize, msg_id: u64 },
            PartitionNode(usize),
            HealNode(usize),
            SetDropRate(f64),
        }

        for _ in 0..num_ops {
            let op = match rng.gen_range(0..100) {
                0..=69 => Operation::Send {
                    from: rng.gen_range(0..num_nodes),
                    to: rng.gen_range(0..num_nodes),
                    msg_id: operations.len() as u64,
                },
                70..=84 => Operation::PartitionNode(rng.gen_range(0..num_nodes)),
                85..=94 => Operation::HealNode(rng.gen_range(0..num_nodes)),
                _ => Operation::SetDropRate(rng.gen_range(0.0..=0.5)),
            };
            operations.push(op);
        }

        // Track state for invariant checking.
        // do_send spawns async tasks — partition checks happen when those tasks
        // run during quiescence. Track partition windows to only check messages
        // sent AFTER partition AND partition never healed.
        let mut partition_start: BTreeMap<usize, usize> = BTreeMap::new(); // node -> operation_index
        let mut sent_messages: Vec<(usize, usize, u64, usize)> = Vec::new(); // (from, to, id, op_index)

        // 5. Execute operations
        eprintln!("Executing {} operations...", operations.len());
        for (op_index, op) in operations.iter().enumerate() {
            match op {
                Operation::Send { from, to, msg_id } => {
                    sent_messages.push((*from, *to, *msg_id, op_index));

                    let msg = FinalizeInconsistent {
                        op_id: OpId {
                            client_id: ClientId(0),
                            number: *msg_id,
                        },
                    };
                    transports[*from].do_send(*to, msg);
                }
                Operation::PartitionNode(node) => {
                    for transport in &transports {
                        transport.partition_node(*node);
                    }
                    partition_start.insert(*node, op_index);
                }
                Operation::HealNode(node) => {
                    for transport in &transports {
                        transport.heal_node(*node);
                    }
                    partition_start.remove(node);
                }
                Operation::SetDropRate(rate) => {
                    for transport in &transports {
                        transport.set_drop_rate(*rate);
                    }
                }
            }
        }

        // 6. Wait for quiescence — all spawned tasks complete
        <FaultyChannelTransport<TestUpcalls> as Transport<TestUpcalls>>::sleep(
            Duration::from_millis(500)
        ).await;

        // 7. Verify hard invariants
        let received_msgs = received.lock().unwrap().clone();
        let received_set: BTreeSet<(usize, usize, u64)> = received_msgs.iter().cloned().collect();

        let send_count = sent_messages.len();
        eprintln!("Sent {} messages, received {}", send_count, received_msgs.len());

        // Hard invariant: messages sent AFTER a node was partitioned AND that
        // node is still partitioned at quiescence (never healed) must not be
        // delivered. NOTE: partition_node() doesn't block self-sends (from==to).
        let mut blocked_by_partition = 0;
        for &(from, to, msg_id, send_index) in &sent_messages {
            // Skip self-sends — partition_node() doesn't block them
            if from == to {
                continue;
            }

            // Check if from was partitioned before this send and never healed
            let from_blocked = partition_start.get(&from)
                .map(|&part_idx| part_idx < send_index)
                .unwrap_or(false);

            // Check if to was partitioned before this send and never healed
            let to_blocked = partition_start.get(&to)
                .map(|&part_idx| part_idx < send_index)
                .unwrap_or(false);

            if from_blocked || to_blocked {
                blocked_by_partition += 1;
                assert!(
                    !received_set.contains(&(from, to, msg_id)),
                    "Message {} from {} to {} delivered despite partition (from_part={}, to_part={})",
                    msg_id, from, to, from_blocked, to_blocked
                );
            }
        }

        if blocked_by_partition > 0 {
            eprintln!("✓ Partition invariant: {}/{} messages must be blocked", blocked_by_partition, send_count);
        }
    }
}
