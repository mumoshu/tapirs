use super::ShardNumber;

/// Routes keys to shards. Static setups use a fixed mapping; dynamic setups
/// refresh the mapping when `OutOfRange` is received.
pub trait ShardRouter<K>: Send + Sync + 'static {
    fn route(&self, key: &K) -> ShardNumber;
    fn shards_for_range(&self, start: &K, end: &K) -> Vec<ShardNumber>;
    fn on_out_of_range(&self, shard: ShardNumber, key: &K);
}

/// Fixed single-shard router for static setups (backward compatible).
pub struct StaticRouter {
    shard: ShardNumber,
}

impl StaticRouter {
    pub fn new(shard: ShardNumber) -> Self {
        Self { shard }
    }
}

impl<K> ShardRouter<K> for StaticRouter {
    fn route(&self, _key: &K) -> ShardNumber {
        self.shard
    }

    fn shards_for_range(&self, _start: &K, _end: &K) -> Vec<ShardNumber> {
        vec![self.shard]
    }

    fn on_out_of_range(&self, _shard: ShardNumber, _key: &K) {
        // Static router cannot refresh — caller should retry or fail.
    }
}
