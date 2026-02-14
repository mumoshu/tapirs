use super::ShardNumber;

/// Routes keys to shards. Implementations map keys to shard numbers and
/// determine which shards overlap a given key range.
pub trait ShardRouter<K>: Send + Sync + 'static {
    fn route(&self, key: &K) -> ShardNumber;
    fn shards_for_range(&self, start: &K, end: &K) -> Vec<ShardNumber>;
    fn on_out_of_range(&self, shard: ShardNumber, key: &K);
}
