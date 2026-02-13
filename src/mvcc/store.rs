use super::backend::MvccBackend;
use crate::util::vectorize_btree;
use serde::{Deserialize, Serialize};
use std::{
    borrow::Borrow,
    collections::BTreeMap,
    convert::Infallible,
    ops::{Bound, Deref, DerefMut},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryStore<K, V, TS> {
    /// For each timestamped version of a key, track the
    /// value (or tombstone) and the optional read timestamp.
    ///
    /// For all keys, there is an implicit version (TS::default() => (None, None)),
    /// in other words the key was nonexistent at the beginning of time.
    #[serde(
        with = "vectorize_btree",
        bound(
            serialize = "K: Serialize + Ord, V: Serialize, TS: Serialize",
            deserialize = "K: Deserialize<'de> + Ord, V: Deserialize<'de>, TS: Deserialize<'de> + Ord"
        )
    )]
    inner: BTreeMap<K, Versions<V, TS>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Versions<V, TS> {
    #[serde(
        with = "vectorize_btree",
        bound(
            serialize = "V: Serialize, TS: Serialize",
            deserialize = "V: Deserialize<'de>, TS: Deserialize<'de> + Ord"
        )
    )]
    inner: BTreeMap<TS, (Option<V>, Option<TS>)>,
}

impl<V, TS> Default for Versions<V, TS> {
    fn default() -> Self {
        Self {
            inner: Default::default(),
        }
    }
}

impl<V, TS> Deref for Versions<V, TS> {
    type Target = BTreeMap<TS, (Option<V>, Option<TS>)>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<V, TS> DerefMut for Versions<V, TS> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl<K, V, TS> Default for MemoryStore<K, V, TS> {
    fn default() -> Self {
        Self {
            inner: Default::default(),
        }
    }
}

impl<K: Ord + Clone, V: Clone, TS: Ord + Eq + Copy + Default> MemoryStore<K, V, TS> {
    /// Return all committed versions in the timestamp range [start_ts, end_ts].
    pub fn scan_committed_since(&self, start_ts: TS, end_ts: TS) -> Vec<(K, Option<V>, TS)> {
        let mut changes = Vec::new();
        for (key, versions) in &self.inner {
            for (ts, (value, _)) in versions.range(start_ts..=end_ts) {
                changes.push((key.clone(), value.clone(), *ts));
            }
        }
        changes
    }
}

impl<K: Ord, V, TS: Ord + Eq + Copy + Default> MemoryStore<K, V, TS> {
    /// Get the latest version.
    pub fn get<Q: ?Sized + Ord>(&self, key: &Q) -> (Option<&V>, TS)
    where
        K: Borrow<Q>,
    {
        self.inner
            .get(key)
            .and_then(|versions| versions.last_key_value())
            .map(|(ts, (v, _))| (v.as_ref(), *ts))
            .unwrap_or_default()
    }

    /// Get the version valid at the timestamp.
    pub fn get_at<Q: ?Sized + Ord>(&self, key: &Q, timestamp: TS) -> (Option<&V>, TS)
    where
        K: Borrow<Q>,
    {
        self.inner
            .get(key)
            .and_then(|versions| versions.range(..=timestamp).next_back())
            .map(|(ts, (v, _))| (v.as_ref(), *ts))
            .unwrap_or_default()
    }

    /// Get range from a timestamp to the next timestamp (if any).
    pub fn get_range<Q: ?Sized + Ord>(&self, key: &Q, timestamp: TS) -> (TS, Option<TS>)
    where
        K: Borrow<Q>,
    {
        self.inner
            .get(key)
            .map(|versions| {
                if let Some((fk, _)) = versions.range(..=timestamp).next_back() {
                    let next = versions.range((Bound::Excluded(fk), Bound::Unbounded)).next();
                    if let Some((lk, _)) = next {
                        (*fk, Some(*lk))
                    } else {
                        (*fk, None)
                    }
                } else {
                    // Start at the implicit version and end at the first explict version, if any.
                    (TS::default(), versions.first_key_value().map(|(k, _)| *k))
                }
            })
            .unwrap_or_default()
    }

    /// Install a timestamped version for a key.
    pub fn put(&mut self, key: K, value: Option<V>, timestamp: TS) {
        debug_assert!(timestamp > TS::default());
        self.inner
            .entry(key)
            .or_default()
            .insert(timestamp, (value, None));
    }

    /// Update the timestamp of the latest read transaction for the
    /// version of the key that the transaction read.
    pub fn commit_get(&mut self, key: K, read: TS, commit: TS) {
        let versions = self.inner.entry(key).or_default();
        if let Some((_, (_, version))) = versions.range_mut(..=read).next_back() {
            *version = Some(if let Some(version) = *version {
                version.max(commit)
            } else {
                commit
            });
        } else {
            // Make the implicit version explicit.
            versions.insert(TS::default(), (None, Some(commit)));
        }
    }

    /// Get the last read timestamp of the last version.
    pub fn get_last_read<Q: ?Sized + Ord>(&self, key: &Q) -> Option<TS>
    where
        K: Borrow<Q>,
    {
        self.inner
            .get(key)
            .and_then(|entries| entries.last_key_value())
            .and_then(|(_, (_, ts))| *ts)
    }

    /// Get the last read timestamp of a specific version.
    pub fn get_last_read_at<Q: ?Sized + Ord>(&self, key: &Q, timestamp: TS) -> Option<TS>
    where
        K: Borrow<Q>,
    {
        self.inner
            .get(key)
            .and_then(|entries| entries.range(..=timestamp).next_back().map(|(_, v)| v))
            .and_then(|(_, ts)| *ts)
    }
}

impl<K: Ord + Clone, V: Clone, TS: Ord + Eq + Copy + Default> MemoryStore<K, V, TS> {
    /// Return all key-value pairs in `[start..=end]` at the given timestamp.
    pub fn scan(&self, start: &K, end: &K, timestamp: TS) -> Vec<(K, Option<V>, TS)> {
        let mut results = Vec::new();
        for (key, versions) in self.inner.range(start..=end) {
            if let Some((ts, (value, _))) = versions.range(..=timestamp).next_back() {
                results.push((key.clone(), value.clone(), *ts));
            }
        }
        results
    }

    /// Check if any writes exist for keys in `[start..=end]` with timestamps in `(after_ts, before_ts)`.
    pub fn has_writes_in_range(&self, start: &K, end: &K, after_ts: TS, before_ts: TS) -> bool {
        for (_key, versions) in self.inner.range(start..=end) {
            if versions
                .range((Bound::Excluded(after_ts), Bound::Excluded(before_ts)))
                .next()
                .is_some()
            {
                return true;
            }
        }
        false
    }
}

impl<K: Ord + Clone, V: Clone, TS: Ord + Eq + Copy + Default + Send + std::fmt::Debug>
    MvccBackend<K, V, TS> for MemoryStore<K, V, TS>
where
    K: Send,
    V: Send,
    TS: Send,
{
    type Error = Infallible;

    fn get(&self, key: &K) -> Result<(Option<V>, TS), Infallible> {
        let (v, ts) = MemoryStore::get(self, key);
        Ok((v.cloned(), ts))
    }

    fn get_at(&self, key: &K, timestamp: TS) -> Result<(Option<V>, TS), Infallible> {
        let (v, ts) = MemoryStore::get_at(self, key, timestamp);
        Ok((v.cloned(), ts))
    }

    fn get_range(&self, key: &K, timestamp: TS) -> Result<(TS, Option<TS>), Infallible> {
        Ok(MemoryStore::get_range(self, key, timestamp))
    }

    fn put(&mut self, key: K, value: Option<V>, timestamp: TS) -> Result<(), Infallible> {
        MemoryStore::put(self, key, value, timestamp);
        Ok(())
    }

    fn commit_get(&mut self, key: K, read: TS, commit: TS) -> Result<(), Infallible> {
        MemoryStore::commit_get(self, key, read, commit);
        Ok(())
    }

    fn get_last_read(&self, key: &K) -> Result<Option<TS>, Infallible> {
        Ok(MemoryStore::get_last_read(self, key))
    }

    fn get_last_read_at(&self, key: &K, timestamp: TS) -> Result<Option<TS>, Infallible> {
        Ok(MemoryStore::get_last_read_at(self, key, timestamp))
    }

    fn scan(&self, start: &K, end: &K, timestamp: TS) -> Result<Vec<(K, Option<V>, TS)>, Infallible> {
        Ok(MemoryStore::scan(self, start, end, timestamp))
    }

    fn has_writes_in_range(&self, start: &K, end: &K, after_ts: TS, before_ts: TS) -> Result<bool, Infallible> {
        Ok(MemoryStore::has_writes_in_range(self, start, end, after_ts, before_ts))
    }
}

/// Type alias for backward compatibility.
pub type Store<K, V, TS> = MemoryStore<K, V, TS>;

#[cfg(test)]
mod tests {
    use super::MemoryStore;

    #[test]
    fn simple() {
        let mut store = MemoryStore::default();
        assert_eq!(store.get("test1"), (None, 0));

        store.put("test1".to_owned(), Some("abc".to_owned()), 10);
        assert_eq!(store.get("test1"), (Some(&String::from("abc")), 10));

        store.put("test1".to_owned(), Some("xyz".to_owned()), 11);
        assert_eq!(store.get("test1"), (Some(&String::from("xyz")), 11));

        assert_eq!(store.get_at("test1", 10), (Some(&String::from("abc")), 10));
    }
}
