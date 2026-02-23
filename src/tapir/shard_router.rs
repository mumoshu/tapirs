use super::ShardNumber;

/// Routes keys to shards. Implementations map keys to shard numbers and
/// determine which shards overlap a given key range.
pub trait ShardRouter<K>: Send + Sync + 'static {
    /// Route a key to its shard. Returns `None` when the key is not covered
    /// by any shard range — expected transiently during resharding while the
    /// directory catches up via eventual consistency.
    fn route(&self, key: &K) -> Option<ShardNumber>;
    fn shards_for_range(&self, start: &K, end: &K) -> Vec<ShardNumber>;
    /// Like `shards_for_range`, but returns per-shard clipped start/end keys.
    /// Each tuple's range is the intersection of [start, end) with the shard's
    /// key range, so a shard with range [g, n) receiving scan(a, z) returns
    /// clipped bounds (g, n). This prevents OutOfRange when a scan spans
    /// multiple shards after a split.
    fn scan_ranges(&self, start: &K, end: &K) -> Vec<(ShardNumber, K, K)>;
    fn on_out_of_range(&self, shard: ShardNumber, key: &K);
}
