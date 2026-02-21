use super::{KeyRange, ShardNumber};
use crate::tapir::shard_router::ShardRouter;
use std::fmt::Debug;
use std::sync::{Arc, RwLock};

/// A shard's key range assignment in the directory.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
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

/// Returns true if any two entries have overlapping key ranges.
fn has_overlaps<K: Ord + Clone + Debug>(entries: &[ShardEntry<K>]) -> bool {
    let mut sorted: Vec<_> = entries.iter().collect();
    sorted.sort_by(|a, b| a.range.start.cmp(&b.range.start));
    for pair in sorted.windows(2) {
        if pair[0].range.overlaps(&pair[1].range) {
            return true;
        }
    }
    false
}

impl<K: Ord + Clone + Debug> ShardDirectory<K> {
    pub fn new(entries: Vec<ShardEntry<K>>) -> Self {
        assert!(!has_overlaps(&entries), "initial ShardDirectory has overlapping ranges");
        Self {
            generation: 0,
            entries,
        }
    }

    pub fn route(&self, key: &K) -> Option<ShardNumber> {
        for entry in &self.entries {
            if entry.range.contains(key) {
                return Some(entry.shard);
            }
        }
        None
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

    /// Update the directory with new shard entries.
    ///
    /// Skips the update if the entries contain overlapping key ranges
    /// (can happen transiently when the discovery source is eventually
    /// consistent). Returns `true` if the update was applied.
    pub fn update(&mut self, entries: Vec<ShardEntry<K>>) -> bool {
        if has_overlaps(&entries) {
            tracing::warn!(
                "skipping directory update: overlapping key ranges ({} entries)",
                entries.len()
            );
            return false;
        }
        self.entries = entries;
        self.generation += 1;
        true
    }

    /// Replace one shard number with another in-place.
    ///
    /// Used by compact where the key range is identical — no overlap
    /// validation needed since only the shard number changes.
    /// Panics if `old` is not found.
    pub fn replace(&mut self, old: ShardNumber, new: ShardNumber) {
        let entry = self.entries.iter_mut().find(|e| e.shard == old);
        match entry {
            Some(e) => {
                e.shard = new;
                self.generation += 1;
            }
            None => panic!(
                "replace: shard {:?} not found in directory ({} entries)",
                old,
                self.entries.len(),
            ),
        }
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
    fn route(&self, key: &K) -> Option<ShardNumber> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tapir::KeyRange;

    #[test]
    fn replace_swaps_shard_number() {
        let mut dir = ShardDirectory::new(vec![
            ShardEntry {
                shard: ShardNumber(0),
                range: KeyRange { start: None, end: Some(50) },
            },
            ShardEntry {
                shard: ShardNumber(1),
                range: KeyRange { start: Some(50), end: None },
            },
        ]);

        assert_eq!(dir.route(&25), Some(ShardNumber(0)));
        assert_eq!(dir.generation, 0);

        dir.replace(ShardNumber(0), ShardNumber(2));

        // Same key now routes to the new shard number.
        assert_eq!(dir.route(&25), Some(ShardNumber(2)));
        // Other shard untouched.
        assert_eq!(dir.route(&75), Some(ShardNumber(1)));
        // Generation bumped.
        assert_eq!(dir.generation, 1);
    }

    #[test]
    #[should_panic(expected = "replace: shard")]
    fn replace_panics_on_missing_shard() {
        let mut dir = ShardDirectory::new(vec![ShardEntry {
            shard: ShardNumber(0),
            range: KeyRange::<i32> { start: None, end: None },
        }]);
        dir.replace(ShardNumber(99), ShardNumber(1));
    }
}
