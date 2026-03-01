use crate::tapir::LeaderRecordDelta;
use crate::util::vectorize_btree;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// CDC delta storage: tracks `LeaderRecordDelta` entries keyed by base view.
///
/// Each entry records the committed KV changes observed during a single
/// view transition. Used by resharding catch-up to replay changes.
#[derive(Serialize, Deserialize)]
pub struct RecordDeltaDuringView<K, V> {
    #[serde(
        with = "vectorize_btree",
        bound(
            serialize = "K: Serialize, V: Serialize",
            deserialize = "K: Deserialize<'de>, V: Deserialize<'de>"
        )
    )]
    inner: BTreeMap<u64, LeaderRecordDelta<K, V>>,
}

impl<K, V> Default for RecordDeltaDuringView<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> RecordDeltaDuringView<K, V> {
    pub fn new() -> Self {
        Self {
            inner: BTreeMap::new(),
        }
    }

    pub fn record_cdc_delta(&mut self, base_view: u64, delta: LeaderRecordDelta<K, V>) {
        self.inner.insert(base_view, delta);
    }

    pub fn cdc_deltas_from(&self, from_view: u64) -> Vec<LeaderRecordDelta<K, V>>
    where
        K: Clone,
        V: Clone,
    {
        self.inner
            .range(from_view..)
            .map(|(_, delta)| delta.clone())
            .collect()
    }

    pub fn cdc_max_view(&self) -> Option<u64> {
        self.inner.keys().next_back().copied()
    }
}
