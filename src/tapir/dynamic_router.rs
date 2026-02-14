use super::{KeyRange, ShardNumber};
use crate::tapir::shard_router::ShardRouter;
use std::fmt::Debug;
use std::sync::{Arc, RwLock};

/// A shard's key range assignment in the directory.
#[derive(Clone, Debug)]
pub struct ShardEntry<K> {
    pub shard: ShardNumber,
    pub range: KeyRange<K>,
}

/// Maps the full key space to shards via contiguous, non-overlapping ranges.
#[derive(Clone, Debug)]
pub struct ShardDirectory<K> {
    /// Internal counter incremented on each directory update (not in protocol).
    pub generation: u64,
    entries: Vec<ShardEntry<K>>,
}

/// Panics if any two entries have overlapping key ranges.
fn validate_no_overlaps<K: Ord + Clone + Debug>(entries: &[ShardEntry<K>]) {
    let mut sorted: Vec<_> = entries.iter().collect();
    sorted.sort_by(|a, b| a.range.start.cmp(&b.range.start));
    for pair in sorted.windows(2) {
        let a = &pair[0];
        let b = &pair[1];
        assert!(
            !a.range.overlaps(&b.range),
            "overlapping shard key ranges: shard {:?} range {:?} overlaps shard {:?} range {:?}",
            a.shard, a.range, b.shard, b.range,
        );
    }
}

impl<K: Ord + Clone + Debug> ShardDirectory<K> {
    pub fn new(entries: Vec<ShardEntry<K>>) -> Self {
        validate_no_overlaps(&entries);
        Self {
            generation: 0,
            entries,
        }
    }

    pub fn route(&self, key: &K) -> ShardNumber {
        for entry in &self.entries {
            if entry.range.contains(key) {
                return entry.shard;
            }
        }
        panic!(
            "key {:?} not covered by any shard range in directory ({} entries)",
            key,
            self.entries.len(),
        );
    }

    pub fn shards_for_range(&self, start: &K, end: &K) -> Vec<ShardNumber> {
        self.entries
            .iter()
            .filter(|entry| entry.range.overlaps_range(start, end))
            .map(|entry| entry.shard)
            .collect()
    }

    pub fn entries(&self) -> &[ShardEntry<K>] {
        &self.entries
    }

    pub fn update(&mut self, entries: Vec<ShardEntry<K>>) {
        validate_no_overlaps(&entries);
        self.entries = entries;
        self.generation += 1;
    }
}

/// Refreshable shard router backed by a shared directory.
pub struct DynamicRouter<K> {
    directory: Arc<RwLock<ShardDirectory<K>>>,
}

impl<K> DynamicRouter<K> {
    pub fn new(directory: Arc<RwLock<ShardDirectory<K>>>) -> Self {
        Self { directory }
    }

    pub fn directory(&self) -> Arc<RwLock<ShardDirectory<K>>> {
        Arc::clone(&self.directory)
    }
}

impl<K: Ord + Clone + Debug + Send + Sync + 'static> ShardRouter<K> for DynamicRouter<K> {
    fn route(&self, key: &K) -> ShardNumber {
        self.directory.read().unwrap().route(key)
    }

    fn shards_for_range(&self, start: &K, end: &K) -> Vec<ShardNumber> {
        self.directory.read().unwrap().shards_for_range(start, end)
    }

    fn on_out_of_range(&self, _shard: ShardNumber, _key: &K) {
        // In a real deployment, this would trigger an async directory refresh
        // from the transport layer. For now, the ShardManager updates the
        // directory directly and clients retry.
    }
}
