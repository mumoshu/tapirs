use super::ShardNumber;

/// Routes keys to shards. Implementations map keys to shard numbers and
/// determine which shards overlap a given key range.
pub trait ShardRouter<K>: Send + Sync + 'static {
    /// Route a key to its shard. Returns `None` when the key is not covered
    /// by any shard range — expected transiently during resharding while the
    /// directory catches up via eventual consistency.
    fn route(&self, key: &K) -> Option<ShardNumber>;
    fn shards_for_range(&self, start: &K, end: &K) -> Vec<ShardNumber>;
    fn on_out_of_range(&self, shard: ShardNumber, key: &K);
}
