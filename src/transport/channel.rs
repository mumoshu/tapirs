use super::{TapirTransport, Transport};
use crate::{
    tapir::{Key, Value},
    IrMembership, IrMessage, IrReplicaUpcalls, ShardNumber, TapirReplica,
};
use serde::{de::DeserializeOwned, Serialize};
use std::{
    collections::HashMap,
    fmt::Debug,
    future::Future,
    sync::{Arc, Mutex, RwLock},
    time::Duration,
};
use tracing::{trace, warn};

const LOG: bool = true;

#[derive(Clone, Debug)]
pub struct RetryBackoff {
    pub initial: Duration,
    pub max: Duration,
    pub multiplier: u32,
}

impl Default for RetryBackoff {
    fn default() -> Self {
        Self {
            initial: Duration::from_millis(50),
            max: Duration::from_millis(1000),
            multiplier: 2,
        }
    }
}

pub struct Registry<U: IrReplicaUpcalls> {
    inner: Arc<RwLock<Inner<U>>>,
}

impl<M: IrReplicaUpcalls> Default for Registry<M> {
    fn default() -> Self {
        Self {
            inner: Default::default(),
        }
    }
}

struct Inner<U: IrReplicaUpcalls> {
    #[allow(clippy::type_complexity)]
    callbacks: Vec<
        Arc<
            dyn Fn(usize, IrMessage<U, Channel<U>>) -> Option<IrMessage<U, Channel<U>>>
                + Send
                + Sync,
        >,
    >,
    shards: HashMap<ShardNumber, IrMembership<usize>>,
    epoch: tokio::time::Instant,
}

impl<U: IrReplicaUpcalls> Default for Inner<U> {
    fn default() -> Self {
        Self {
            callbacks: Vec::new(),
            shards: Default::default(),
            epoch: tokio::time::Instant::now(),
        }
    }
}

impl<U: IrReplicaUpcalls> Registry<U> {
    pub fn channel(
        &self,
        callback: impl Fn(usize, IrMessage<U, Channel<U>>) -> Option<IrMessage<U, Channel<U>>>
            + Send
            + Sync
            + 'static,
    ) -> Channel<U> {
        let mut inner = self.inner.write().unwrap();
        let address = inner.callbacks.len();
        let epoch = inner.epoch;
        inner.callbacks.push(Arc::new(callback));
        Channel {
            address,
            persistent: Default::default(),
            inner: Arc::clone(&self.inner),
            epoch,
            retry_backoff: RetryBackoff::default(),
        }
    }

    pub fn put_shard_addresses(&self, shard: ShardNumber, membership: IrMembership<usize>) {
        let mut inner = self.inner.write().unwrap();
        inner.shards.insert(shard, membership);
    }

    pub fn len(&self) -> usize {
        self.inner.read().unwrap().callbacks.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

pub struct Channel<U: IrReplicaUpcalls> {
    address: usize,
    persistent: Arc<Mutex<HashMap<String, String>>>,
    inner: Arc<RwLock<Inner<U>>>,
    epoch: tokio::time::Instant,
    retry_backoff: RetryBackoff,
}

impl<U: IrReplicaUpcalls> Clone for Channel<U> {
    fn clone(&self) -> Self {
        Self {
            address: self.address,
            persistent: Arc::clone(&self.persistent),
            inner: Arc::clone(&self.inner),
            epoch: self.epoch,
            retry_backoff: self.retry_backoff.clone(),
        }
    }
}


impl<U: IrReplicaUpcalls> Transport<U> for Channel<U> {
    type Address = usize;
    type Sleep = tokio::time::Sleep;

    fn address(&self) -> Self::Address {
        self.address
    }

    fn time(&self) -> u64 {
        (tokio::time::Instant::now() - self.epoch).as_nanos() as u64
    }

    fn time_offset(&self, offset: i64) -> u64 {
        self.time()
            .saturating_add_signed(offset.saturating_mul(1_000_000))
    }

    fn sleep(duration: Duration) -> Self::Sleep {
        tokio::time::sleep(duration)
    }

    fn persist<T: Serialize>(&self, key: &str, value: Option<&T>) {
        let mut persistent = self.persistent.lock().unwrap();
        if let Some(value) = value {
            let string = serde_json::to_string(&value).unwrap();
            let to_display = if string.len() > 200 {
                let with_bc = bitcode::serialize(&value).unwrap();
                format!("<{} bytes ({} with bitcode)>", string.len(), with_bc.len())
            } else {
                string.clone()
            };
            trace!("{:?} persisting {:?} = {}", self.address, key, to_display);
            persistent.insert(key.to_owned(), string);
        } else {
            persistent.remove(key);
            unreachable!();
        }
    }

    fn persisted<T: DeserializeOwned>(&self, key: &str) -> Option<T> {
        self.persistent
            .lock()
            .unwrap()
            .get(key)
            .and_then(|value| serde_json::from_str(value).ok())
    }

    fn send<R: TryFrom<IrMessage<U, Self>> + Send + Debug>(
        &self,
        address: Self::Address,
        message: impl Into<IrMessage<U, Self>> + Debug,
    ) -> impl Future<Output = R> + 'static {
        let from: usize = self.address;
        if LOG {
            trace!("{from} sending {message:?} to {address}");
        }
        let message = message.into();
        let inner = Arc::clone(&self.inner);
        let backoff = self.retry_backoff.clone();
        async move {
            let mut delay = backoff.initial;
            loop {
                let callback = {
                    let inner = inner.read().unwrap();
                    inner.callbacks.get(address).map(Arc::clone)
                };
                if let Some(callback) = callback.as_ref() {
                    let result = callback(from, message.clone())
                        .map(|r| r.try_into().unwrap_or_else(|_| panic!()));
                    if let Some(result) = result {
                        if LOG {
                            trace!("{address} replying {result:?} to {from}");
                        }
                        break result;
                    } else {
                        // Replica didn't respond (e.g., view change in
                        // progress). Back off before retrying.
                        Self::sleep(delay).await;
                        delay = (delay * backoff.multiplier).min(backoff.max);
                    }
                } else {
                    warn!("unknown address {address:?}");
                }
            }
        }
    }

    fn spawn(future: impl Future<Output = ()> + Send + 'static) {
        tokio::spawn(future);
    }

    fn do_send(&self, address: Self::Address, message: impl Into<IrMessage<U, Self>> + Debug) {
        let from = self.address;
        if LOG {
            trace!("{from} do-sending {message:?} to {address}");
        }
        let message = message.into();
        let inner = self.inner.read().unwrap();
        let callback = inner.callbacks.get(address).map(Arc::clone);
        drop(inner);
        if let Some(callback) = callback {
            Self::spawn(async move {
                callback(from, message);
            });
        }
    }
}

impl<K: Key, V: Value> TapirTransport<K, V> for Channel<TapirReplica<K, V>> {
    fn shard_addresses(
        &self,
        shard: ShardNumber,
    ) -> impl Future<Output = IrMembership<Self::Address>> + Send + 'static {
        let inner = Arc::clone(&self.inner);
        async move {
            loop {
                {
                    let inner = inner.read().unwrap();
                    if let Some(membership) = inner.shards.get(&shard) {
                        break membership.clone();
                    }
                }

                <Self as Transport<TapirReplica<K, V>>>::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::OpId;
    use crate::TapirReplica;
    use crate::transport::faulty_channel::{FaultyChannelTransport, NetworkFaultConfig};
    use rand::rngs::StdRng;
    use rand::SeedableRng;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tokio::time::timeout;

    type TestUpcalls = TapirReplica<i64, i64>;
    type TestMessage = IrMessage<TestUpcalls, Channel<TestUpcalls>>;

    // Helper: Create a simple test message
    fn test_message(seq: u64) -> TestMessage {
        use crate::ir::message::FinalizeInconsistent;
        use crate::ir::ClientId;
        TestMessage::FinalizeInconsistent(FinalizeInconsistent {
            op_id: OpId {
                client_id: ClientId(0),
                number: seq,
            },
        })
    }

    // Helper: Create a reply message (for transport tests, we just reuse test_message)
    fn test_reply() -> TestMessage {
        test_message(999) // Use distinct seq number to identify replies
    }

    // ========================================
    // A. ORDERING TESTS (3 tests)
    // ========================================

    #[tokio::test(start_paused = true)]
    async fn test_ordering_send_maintains_fifo_without_faults() {
        let registry = Registry::<TestUpcalls>::default();
        let received = Arc::new(Mutex::new(Vec::new()));

        let recv_clone = Arc::clone(&received);
        let ch0 = registry.channel(|_, _| None);
        let ch1 = registry.channel(move |from, msg| {
            if let TestMessage::FinalizeInconsistent(finalize) = msg {
                recv_clone.lock().unwrap().push((from, finalize.op_id.number));
                Some(test_reply())
            } else {
                None
            }
        });

        // Send 5 numbered messages from node 0 to node 1
        for seq in 0..5 {
            ch0.send::<TestMessage>(1, test_message(seq)).await;
        }

        // Verify FIFO ordering
        let msgs = received.lock().unwrap();
        assert_eq!(msgs.len(), 5, "Should receive all 5 messages");
        for i in 0..5 {
            assert_eq!(msgs[i], (0, i as u64), "Message {} out of order", i);
        }
        // Verify monotonic increase
        assert!(
            msgs.windows(2).all(|w| w[1].1 == w[0].1 + 1),
            "Messages should be in strict FIFO order"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn test_ordering_do_send_preserves_delivered_order() {
        let registry = Registry::<TestUpcalls>::default();
        let received = Arc::new(Mutex::new(Vec::new()));

        let recv_clone = Arc::clone(&received);
        let ch0 = registry.channel(|_, _| None);
        let _ch1 = registry.channel(move |from, msg| {
            if let TestMessage::FinalizeInconsistent(finalize) = msg {
                recv_clone.lock().unwrap().push((from, finalize.op_id.number));
            }
            None
        });

        // Send 10 messages via do_send (fire-and-forget)
        for seq in 0..10 {
            ch0.do_send(1, test_message(seq));
        }

        // Wait for tasks to execute
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Verify delivered subset preserves order
        let msgs = received.lock().unwrap();
        assert_eq!(msgs.len(), 10, "Should receive all 10 messages");

        // Delivered messages should be in order (even if not all deliver)
        if msgs.len() > 1 {
            assert!(
                msgs.windows(2).all(|w| w[1].1 > w[0].1),
                "Delivered subset should preserve FIFO order"
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn test_ordering_with_drop_fault_maintains_delivered_order() {
        let mut rng = StdRng::seed_from_u64(42);

        let registry = Registry::<TestUpcalls>::default();
        let received = Arc::new(Mutex::new(Vec::new()));

        let recv_clone = Arc::clone(&received);
        let ch0_inner = registry.channel(|_, _| None);
        let ch1_inner = registry.channel(move |from, msg| {
            if let TestMessage::FinalizeInconsistent(finalize) = msg {
                recv_clone.lock().unwrap().push((from, finalize.op_id.number));
                Some(test_reply())
            } else {
                None
            }
        });

        // Wrap in FaultyChannelTransport with drop_rate=0.3
        let mut config = NetworkFaultConfig::default();
        config.drop_rate = 0.3;
        let ch0 = FaultyChannelTransport::new(ch0_inner, config.clone(), 42);
        let _ch1 = FaultyChannelTransport::new(ch1_inner, config, 43);

        // Send 20 messages (send() will retry on drops until success)
        for seq in 0..20 {
            let _result: IrMessage<TestUpcalls, FaultyChannelTransport<TestUpcalls>> =
                timeout(Duration::from_secs(5), ch0.send::<TestMessage>(1, test_message(seq)))
                    .await
                    .expect("Send should not timeout");
        }

        // Verify all delivered (send() retries ensure delivery)
        let msgs = received.lock().unwrap();
        assert_eq!(msgs.len(), 20, "All messages should eventually be delivered");

        // Verify strict FIFO order
        for i in 0..20 {
            assert_eq!(msgs[i].1, i as u64, "Message {} out of order", i);
        }
    }

    // ========================================
    // B. DELIVERY TESTS (4 tests)
    // ========================================

    #[tokio::test(start_paused = true)]
    async fn test_delivery_send_retries_with_sustained_loss() {
        let registry = Registry::<TestUpcalls>::default();
        let attempt_counts = Arc::new(Mutex::new(Vec::new()));

        let attempts_clone = Arc::clone(&attempt_counts);
        let ch0 = registry.channel(|_, _| None);
        let attempt_counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = Arc::clone(&attempt_counter);

        let _ch1 = registry.channel(move |from, msg| {
            let attempt = counter_clone.fetch_add(1, AtomicOrdering::SeqCst);

            if let TestMessage::FinalizeInconsistent(finalize) = msg {
                // Succeed on odd attempts (50% success rate)
                if attempt % 2 == 1 {
                    attempts_clone.lock().unwrap().push((finalize.op_id.number, attempt + 1));
                    Some(test_reply())
                } else {
                    None // Drop (triggers retry)
                }
            } else {
                None
            }
        });

        // Send 3 messages
        for seq in 0..3 {
            timeout(Duration::from_secs(5), ch0.send::<TestMessage>(1, test_message(seq)))
                .await
                .expect("Send should eventually succeed");
        }

        // Verify all succeeded and required retries
        let attempts = attempt_counts.lock().unwrap();
        assert_eq!(attempts.len(), 3, "All 3 messages should succeed");

        // Each message should require at least 2 attempts (first fails, second succeeds)
        for (seq, attempt_count) in attempts.iter() {
            assert!(
                *attempt_count >= 2,
                "Message {} should require at least 2 attempts, got {}",
                seq,
                attempt_count
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn test_delivery_do_send_fires_once_ignores_reply() {
        let registry = Registry::<TestUpcalls>::default();
        let receive_count = Arc::new(AtomicUsize::new(0));

        let count_clone = Arc::clone(&receive_count);
        let ch0 = registry.channel(|_, _| None);
        let _ch1 = registry.channel(move |_, _| {
            count_clone.fetch_add(1, AtomicOrdering::SeqCst);
            None // Always return None (not ready)
        });

        // Send 5 messages via do_send
        for seq in 0..5 {
            ch0.do_send(1, test_message(seq));
        }

        // Wait for tasks to execute
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Verify callback was invoked exactly 5 times (no retries)
        assert_eq!(
            receive_count.load(AtomicOrdering::SeqCst),
            5,
            "do_send should invoke callback exactly once per message"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn test_delivery_send_reply_drop_triggers_retry() {
        let registry = Registry::<TestUpcalls>::default();
        let callback_count = Arc::new(AtomicUsize::new(0));

        let count_clone = Arc::clone(&callback_count);
        let ch0 = registry.channel(|_, _| None);
        let _ch1 = registry.channel(move |_, msg| {
            let count = count_clone.fetch_add(1, AtomicOrdering::SeqCst);

            if let TestMessage::FinalizeInconsistent(_) = msg {
                // Return Some(reply) only on attempts > 2
                if count >= 2 {
                    Some(test_reply())
                } else {
                    None // Drop reply, trigger retry
                }
            } else {
                None
            }
        });

        // Send 1 message
        timeout(Duration::from_secs(5), ch0.send::<TestMessage>(1, test_message(0)))
            .await
            .expect("Send should eventually succeed");

        // Verify callback was called at least 3 times
        assert!(
            callback_count.load(AtomicOrdering::SeqCst) >= 3,
            "Callback should be called multiple times due to retries"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn test_delivery_with_partition_blocks_until_heal() {
        let mut rng = StdRng::seed_from_u64(123);

        let registry = Registry::<TestUpcalls>::default();

        let ch0_inner = registry.channel(|_, _| None);
        let ch1_inner = registry.channel(|_, msg| {
            if let TestMessage::FinalizeInconsistent(_) = msg {
                Some(test_reply())
            } else {
                None
            }
        });

        let config = NetworkFaultConfig::default();
        let ch0 = FaultyChannelTransport::new(ch0_inner, config.clone(), 42);
        let _ch1 = FaultyChannelTransport::new(ch1_inner, config, 43);

        // Partition node 1
        ch0.partition_node(1);

        // Spawn task to send (will block on partition)
        let ch0_clone = ch0.clone();
        let send_task = tokio::spawn(async move {
            let start = tokio::time::Instant::now();
            timeout(Duration::from_secs(5), ch0_clone.send::<TestMessage>(1, test_message(0)))
                .await
                .expect("Should complete after heal");
            start.elapsed()
        });

        // Wait 100ms then heal
        tokio::time::sleep(Duration::from_millis(100)).await;
        ch0.heal_node(1);

        // Wait for send to complete
        let elapsed = send_task.await.expect("Task should complete");

        // Verify task completed after partition healed
        assert!(
            elapsed >= Duration::from_millis(100),
            "Send should block during partition"
        );
        assert!(
            elapsed < Duration::from_millis(500),
            "Send should complete quickly after heal"
        );
    }

    // ========================================
    // C. BACKPRESSURE TESTS (3 tests)
    // ========================================

    // ========================================
    // D. INTEGRATION TESTS (1 test)
    // ========================================

    #[tokio::test(start_paused = true)]
    async fn test_integration_concurrent_senders_fifo_per_pair() {
        let registry = Registry::<TestUpcalls>::default();
        let received = Arc::new(Mutex::new(Vec::new()));

        let recv_clone = Arc::clone(&received);
        let ch0 = registry.channel(|_, _| None);
        let ch1 = registry.channel(|_, _| None);
        let _ch2 = registry.channel(move |from, msg| {
            if let TestMessage::FinalizeInconsistent(finalize) = msg {
                recv_clone.lock().unwrap().push((from, finalize.op_id.number));
                Some(test_reply())
            } else {
                None
            }
        });

        // Spawn concurrent tasks
        let ch0_clone = ch0.clone();
        let task_a = tokio::spawn(async move {
            for seq in 0..5 {
                ch0_clone.send::<TestMessage>(2, test_message(seq)).await;
            }
        });

        let ch1_clone = ch1.clone();
        let task_b = tokio::spawn(async move {
            for seq in 0..5 {
                ch1_clone.send::<TestMessage>(2, test_message(seq)).await;
            }
        });

        task_a.await.unwrap();
        task_b.await.unwrap();

        // Verify all 10 messages delivered
        let msgs = received.lock().unwrap();
        assert_eq!(msgs.len(), 10, "Should receive all 10 messages");

        // Separate by sender
        let from_node0: Vec<_> = msgs.iter().filter(|(from, _)| *from == 0).collect();
        let from_node1: Vec<_> = msgs.iter().filter(|(from, _)| *from == 1).collect();

        assert_eq!(from_node0.len(), 5, "5 messages from node 0");
        assert_eq!(from_node1.len(), 5, "5 messages from node 1");

        // Verify FIFO per sender
        for i in 0..5 {
            assert_eq!(from_node0[i].1, i as u64, "Node 0 message {} out of order", i);
            assert_eq!(from_node1[i].1, i as u64, "Node 1 message {} out of order", i);
        }
    }
}
