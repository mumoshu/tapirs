use super::types::CachedPrepare;
use std::collections::BTreeMap;
use std::sync::Arc;

/// LRU cache for deserialized CO::Prepare payloads from sealed VLog segments.
///
/// Only used by the `OnDisk` value resolution path (`resolve_on_disk`).
/// A single CO::Prepare entry may be referenced by multiple MVCC index
/// entries (one per `write_set` item), so caching the deserialized
/// `CachedPrepare` avoids repeated VLog reads when reading multiple keys
/// committed by the same transaction.
///
/// Keyed by `(segment_id, offset)` which uniquely identifies a VLog entry.
/// Uses `BTreeMap` (not `HashMap`) for deterministic eviction order.
pub struct PrepareCache<K, V> {
    /// Cache entries, keyed by (segment_id, offset).
    entries: BTreeMap<(u64, u64), CacheEntry<K, V>>,
    /// Access order: maps access_counter → (segment_id, offset).
    /// BTreeMap for deterministic eviction order.
    access_order: BTreeMap<u64, (u64, u64)>,
    /// Reverse map: (segment_id, offset) → access_counter for removal.
    key_to_counter: BTreeMap<(u64, u64), u64>,
    /// Monotonically increasing access counter.
    counter: u64,
    /// Maximum number of entries.
    capacity: usize,
}

struct CacheEntry<K, V> {
    value: Arc<CachedPrepare<K, V>>,
}

impl<K, V> PrepareCache<K, V> {
    pub fn new(capacity: usize) -> Self {
        Self {
            entries: BTreeMap::new(),
            access_order: BTreeMap::new(),
            key_to_counter: BTreeMap::new(),
            counter: 0,
            capacity,
        }
    }

    /// Get a cached prepare entry. Returns None on cache miss.
    pub fn get(&mut self, segment_id: u64, offset: u64) -> Option<Arc<CachedPrepare<K, V>>> {
        let key = (segment_id, offset);
        if let Some(entry) = self.entries.get(&key) {
            let value = entry.value.clone();
            // Update access order
            if let Some(old_counter) = self.key_to_counter.remove(&key) {
                self.access_order.remove(&old_counter);
            }
            self.counter += 1;
            self.access_order.insert(self.counter, key);
            self.key_to_counter.insert(key, self.counter);
            Some(value)
        } else {
            None
        }
    }

    /// Insert a prepare entry into the cache.
    pub fn insert(&mut self, segment_id: u64, offset: u64, prepare: Arc<CachedPrepare<K, V>>) {
        let key = (segment_id, offset);

        // Remove existing entry if present
        if self.entries.contains_key(&key)
            && let Some(old_counter) = self.key_to_counter.remove(&key)
        {
            self.access_order.remove(&old_counter);
        }

        // Evict if at capacity
        while self.entries.len() >= self.capacity {
            if let Some((&oldest_counter, &oldest_key)) = self.access_order.iter().next() {
                self.access_order.remove(&oldest_counter);
                self.key_to_counter.remove(&oldest_key);
                self.entries.remove(&oldest_key);
            } else {
                break;
            }
        }

        // Insert new entry
        self.counter += 1;
        self.entries.insert(key, CacheEntry { value: prepare });
        self.access_order.insert(self.counter, key);
        self.key_to_counter.insert(key, self.counter);
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.entries.len()
    }
}
