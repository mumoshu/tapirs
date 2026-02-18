use super::backend::MvccBackend;
use crate::util::vectorize_btree;
use serde::{Deserialize, Serialize};
use std::{
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

impl<K: Ord + Clone, V: Clone, TS: Ord + Eq + Copy + Default + Send + std::fmt::Debug>
    MvccBackend<K, V, TS> for MemoryStore<K, V, TS>
where
    K: Send,
    V: Send,
    TS: Send,
{
    type Error = Infallible;

    fn get(&self, key: &K) -> Result<(Option<V>, TS), Infallible> {
        Ok(self
            .inner
            .get(key)
            .and_then(|versions| versions.last_key_value())
            .map(|(ts, (v, _))| (v.clone(), *ts))
            .unwrap_or_default())
    }

    fn get_at(&self, key: &K, timestamp: TS) -> Result<(Option<V>, TS), Infallible> {
        Ok(self
            .inner
            .get(key)
            .and_then(|versions| versions.range(..=timestamp).next_back())
            .map(|(ts, (v, _))| (v.clone(), *ts))
            .unwrap_or_default())
    }

    fn get_range(&self, key: &K, timestamp: TS) -> Result<(TS, Option<TS>), Infallible> {
        Ok(self
            .inner
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
                    // Start at the implicit version and end at the first explicit version, if any.
                    (TS::default(), versions.first_key_value().map(|(k, _)| *k))
                }
            })
            .unwrap_or_default())
    }

    fn put(&mut self, key: K, value: Option<V>, timestamp: TS) -> Result<(), Infallible> {
        debug_assert!(timestamp > TS::default());
        self.inner
            .entry(key)
            .or_default()
            .insert(timestamp, (value, None));
        Ok(())
    }

    fn commit_get(&mut self, key: K, read: TS, commit: TS) -> Result<(), Infallible> {
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
        Ok(())
    }

    fn get_last_read(&self, key: &K) -> Result<Option<TS>, Infallible> {
        Ok(self
            .inner
            .get(key)
            .and_then(|entries| entries.last_key_value())
            .and_then(|(_, (_, ts))| *ts))
    }

    fn get_last_read_at(&self, key: &K, timestamp: TS) -> Result<Option<TS>, Infallible> {
        Ok(self
            .inner
            .get(key)
            .and_then(|entries| entries.range(..=timestamp).next_back().map(|(_, v)| v))
            .and_then(|(_, ts)| *ts))
    }

    fn scan(
        &self,
        start: &K,
        end: &K,
        timestamp: TS,
    ) -> Result<Vec<(K, Option<V>, TS)>, Infallible> {
        let mut results = Vec::new();
        for (key, versions) in self.inner.range(start..=end) {
            if let Some((ts, (value, _))) = versions.range(..=timestamp).next_back() {
                results.push((key.clone(), value.clone(), *ts));
            }
        }
        Ok(results)
    }

    fn has_writes_in_range(
        &self,
        start: &K,
        end: &K,
        after_ts: TS,
        before_ts: TS,
    ) -> Result<bool, Infallible> {
        for (_key, versions) in self.inner.range(start..=end) {
            if versions
                .range((Bound::Excluded(after_ts), Bound::Excluded(before_ts)))
                .next()
                .is_some()
            {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

/// Type alias for backward compatibility.
pub type Store<K, V, TS> = MemoryStore<K, V, TS>;

#[cfg(test)]
mod tests {
    use super::MemoryStore;
    use super::super::backend::MvccBackend;

    #[test]
    fn simple() {
        let mut store: MemoryStore<String, String, u64> = MemoryStore::default();
        assert_eq!(store.get(&"test1".to_owned()).unwrap(), (None, 0));

        store.put("test1".to_owned(), Some("abc".to_owned()), 10).unwrap();
        assert_eq!(
            store.get(&"test1".to_owned()).unwrap(),
            (Some(String::from("abc")), 10)
        );

        store.put("test1".to_owned(), Some("xyz".to_owned()), 11).unwrap();
        assert_eq!(
            store.get(&"test1".to_owned()).unwrap(),
            (Some(String::from("xyz")), 11)
        );

        assert_eq!(
            store.get_at(&"test1".to_owned(), 10).unwrap(),
            (Some(String::from("abc")), 10)
        );
    }
}
