use crate::mvcc::backend::MvccBackend;
use crate::mvcc::disk::memtable::{CompositeKey, LsmEntry, MaxValue, Memtable};
use crate::occ::Timestamp as OccTimestamp;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Debug};
use std::path::PathBuf;
use surrealkv::{LSMIterator, Mode, ReadOptions, Tree, TreeBuilder};

#[derive(Debug)]
pub enum SurrealKvError {
    Surreal(surrealkv::Error),
    Codec(String),
}

impl fmt::Display for SurrealKvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SurrealKvError::Surreal(e) => write!(f, "surrealkv: {e:?}"),
            SurrealKvError::Codec(msg) => write!(f, "codec: {msg}"),
        }
    }
}

impl From<surrealkv::Error> for SurrealKvError {
    fn from(e: surrealkv::Error) -> Self {
        SurrealKvError::Surreal(e)
    }
}

fn encode_surreal_key<K: Serialize, TS: Serialize>(key: &K, ts: &TS) -> Vec<u8> {
    bitcode::serialize(&(key, ts)).expect("bitcode key serialization")
}

fn decode_surreal_key<K: DeserializeOwned, TS: DeserializeOwned>(
    bytes: &[u8],
) -> Result<(K, TS), SurrealKvError> {
    bitcode::deserialize(bytes).map_err(|e| SurrealKvError::Codec(e.to_string()))
}

fn encode_surreal_value<V: Serialize>(value: &Option<V>) -> Vec<u8> {
    bitcode::serialize(value).expect("bitcode value serialization")
}

fn decode_surreal_value<V: DeserializeOwned>(bytes: &[u8]) -> Result<Option<V>, SurrealKvError> {
    bitcode::deserialize(bytes).map_err(|e| SurrealKvError::Codec(e.to_string()))
}

pub struct SurrealKvStore<K, V, TS> {
    runtime: tokio::runtime::Runtime,
    tree: Tree,
    index: Memtable<K, TS>,
    path: PathBuf,
    _v: std::marker::PhantomData<V>,
}

impl<K, V, TS> SurrealKvStore<K, V, TS>
where
    K: Serialize + DeserializeOwned + Ord + Clone + Send + Debug + std::hash::Hash,
    V: Serialize + DeserializeOwned + Clone + Send + Debug,
    TS: Serialize
        + DeserializeOwned
        + Ord
        + Copy
        + Default
        + MaxValue
        + Send
        + Debug
        + OccTimestamp<Time = u64>,
{
    pub fn open(path: PathBuf) -> Result<Self, SurrealKvError> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .map_err(|e| SurrealKvError::Codec(e.to_string()))?;
        let tree = TreeBuilder::new().with_path(path.clone()).build()?;
        let mut store = Self {
            runtime,
            tree,
            index: Memtable::new(),
            path,
            _v: std::marker::PhantomData,
        };
        store.rebuild_index()?;
        Ok(store)
    }

    fn rebuild_index(&mut self) -> Result<(), SurrealKvError> {
        self.index.clear();
        let txn = self.tree.begin_with_mode(Mode::ReadOnly)?;
        let mut iter = txn.range_with_options(&ReadOptions::default())?;
        if !iter.seek_first()? {
            return Ok(());
        }
        while iter.valid() {
            let (key, ts): (K, TS) = decode_surreal_key(iter.key().user_key())?;
            self.index.insert(
                CompositeKey::new(key, ts),
                LsmEntry {
                    value_ptr: None,
                    last_read_ts: None,
                },
            );
            if !iter.next()? {
                break;
            }
        }
        Ok(())
    }

    fn surreal_get(&self, key: &K, ts: &TS) -> Result<Option<V>, SurrealKvError> {
        let encoded = encode_surreal_key(key, ts);
        let txn = self.tree.begin_with_mode(Mode::ReadOnly)?;
        match txn.get(&encoded)? {
            Some(bytes) => decode_surreal_value(&bytes),
            None => Ok(None),
        }
    }

    fn surreal_put(&self, key: &K, ts: &TS, value: &Option<V>) -> Result<(), SurrealKvError> {
        let encoded_key = encode_surreal_key(key, ts);
        let encoded_value = encode_surreal_value(value);
        let mut txn = self.tree.begin()?;
        txn.set(&encoded_key, &encoded_value)?;
        self.runtime.block_on(txn.commit())?;
        Ok(())
    }

    fn find_next_version(&self, key: &K, after_ts: TS) -> Option<TS> {
        let mut next: Option<TS> = None;
        for (ck, _) in self.index.iter() {
            if ck.key == *key && ck.timestamp.0 > after_ts {
                next = Some(match next {
                    Some(existing) => existing.min(ck.timestamp.0),
                    None => ck.timestamp.0,
                });
            }
        }
        next
    }

    fn decode_last_read_ts(&self, raw: Option<u64>) -> Result<Option<TS>, SurrealKvError> {
        Ok(raw.map(TS::from_time))
    }
}

impl<K, V, TS> MvccBackend<K, V, TS> for SurrealKvStore<K, V, TS>
where
    K: Serialize + DeserializeOwned + Ord + Clone + Send + Debug + std::hash::Hash,
    V: Serialize + DeserializeOwned + Clone + Send + Debug,
    TS: Serialize
        + DeserializeOwned
        + Ord
        + Copy
        + Default
        + MaxValue
        + Send
        + Debug
        + OccTimestamp<Time = u64>,
{
    type Error = SurrealKvError;

    fn get(&self, key: &K) -> Result<(Option<V>, TS), SurrealKvError> {
        if let Some((ck, _)) = self.index.get_latest(key) {
            let ts = ck.timestamp.0;
            let value = self.surreal_get(key, &ts)?;
            return Ok((value, ts));
        }
        Ok((None, TS::default()))
    }

    fn get_at(&self, key: &K, timestamp: TS) -> Result<(Option<V>, TS), SurrealKvError> {
        if let Some((ck, _)) = self.index.get_at(key, &timestamp) {
            let ts = ck.timestamp.0;
            let value = self.surreal_get(key, &ts)?;
            return Ok((value, ts));
        }
        Ok((None, TS::default()))
    }

    fn get_range(&self, key: &K, timestamp: TS) -> Result<(TS, Option<TS>), SurrealKvError> {
        let (_, at_ts) = self.get_at(key, timestamp)?;
        let next_ts = self.find_next_version(key, at_ts);
        Ok((at_ts, next_ts))
    }

    fn put(&mut self, key: K, value: Option<V>, timestamp: TS) -> Result<(), SurrealKvError> {
        self.surreal_put(&key, &timestamp, &value)?;
        self.index.insert(
            CompositeKey::new(key, timestamp),
            LsmEntry {
                value_ptr: None,
                last_read_ts: None,
            },
        );
        Ok(())
    }

    fn commit_get(&mut self, key: K, read: TS, commit: TS) -> Result<(), SurrealKvError> {
        self.index.update_last_read(&key, &read, commit.time());
        Ok(())
    }

    fn get_last_read(&self, key: &K) -> Result<Option<TS>, SurrealKvError> {
        if let Some((_, entry)) = self.index.get_latest(key) {
            return self.decode_last_read_ts(entry.last_read_ts);
        }
        Ok(None)
    }

    fn get_last_read_at(&self, key: &K, timestamp: TS) -> Result<Option<TS>, SurrealKvError> {
        if let Some((_, entry)) = self.index.get_at(key, &timestamp) {
            return self.decode_last_read_ts(entry.last_read_ts);
        }
        Ok(None)
    }

    fn scan(
        &self,
        start: &K,
        end: &K,
        timestamp: TS,
    ) -> Result<Vec<(K, Option<V>, TS)>, SurrealKvError> {
        let mem_results = self.index.scan(start, end, &timestamp);
        let mut results = Vec::new();
        for (ck, _) in mem_results {
            let value = self.surreal_get(&ck.key, &ck.timestamp.0)?;
            results.push((ck.key.clone(), value, ck.timestamp.0));
        }
        Ok(results)
    }

    fn has_writes_in_range(
        &self,
        start: &K,
        end: &K,
        after_ts: TS,
        before_ts: TS,
    ) -> Result<bool, SurrealKvError> {
        for (ck, _) in self.index.iter() {
            if ck.key < *start {
                continue;
            }
            if ck.key > *end {
                break;
            }
            if ck.timestamp.0 > after_ts && ck.timestamp.0 < before_ts {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn commit_batch(
        &mut self,
        writes: Vec<(K, Option<V>)>,
        reads: Vec<(K, TS)>,
        commit: TS,
    ) -> Result<(), SurrealKvError> {
        if !writes.is_empty() {
            let mut txn = self.tree.begin()?;
            for (key, value) in &writes {
                let encoded_key = encode_surreal_key(key, &commit);
                let encoded_value = encode_surreal_value(value);
                txn.set(&encoded_key, &encoded_value)?;
            }
            self.runtime.block_on(txn.commit())?;

            for (key, _) in &writes {
                self.index.insert(
                    CompositeKey::new(key.clone(), commit),
                    LsmEntry {
                        value_ptr: None,
                        last_read_ts: None,
                    },
                );
            }
        }

        for (key, read) in reads {
            self.commit_get(key, read, commit)?;
        }

        Ok(())
    }
}

impl<K, V, TS> Serialize for SurrealKvStore<K, V, TS> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let _ = self.tree.flush_wal(true);
        self.path.serialize(serializer)
    }
}

impl<'de, K, V, TS> Deserialize<'de> for SurrealKvStore<K, V, TS>
where
    K: Serialize + DeserializeOwned + Ord + Clone + Send + Debug + std::hash::Hash,
    V: Serialize + DeserializeOwned + Clone + Send + Debug,
    TS: Serialize
        + DeserializeOwned
        + Ord
        + Copy
        + Default
        + MaxValue
        + Send
        + Debug
        + OccTimestamp<Time = u64>,
{
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let path = PathBuf::deserialize(deserializer)?;
        Self::open(path).map_err(serde::de::Error::custom)
    }
}
