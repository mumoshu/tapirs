use serde::{Deserialize, Serialize};
use std::cmp::Reverse;

/// Trait for types that have a maximum value.
/// Used to construct seek keys for BTreeMap lookups.
pub trait MaxValue {
    fn max_value() -> Self;
}

impl MaxValue for u64 {
    fn max_value() -> Self {
        u64::MAX
    }
}

/// Composite key: (user key ASC, timestamp DESC).
///
/// Descending timestamp within each key means the latest version
/// appears first when iterating forward through the BTreeMap.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct CompositeKey<K, TS> {
    pub key: K,
    pub timestamp: Reverse<TS>,
}

impl<K, TS> CompositeKey<K, TS> {
    pub fn new(key: K, timestamp: TS) -> Self {
        Self {
            key,
            timestamp: Reverse(timestamp),
        }
    }
}
