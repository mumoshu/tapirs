use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KeyRange<K> {
    /// Inclusive lower bound. `None` = unbounded below.
    pub start: Option<K>,
    /// Exclusive upper bound. `None` = unbounded above.
    pub end: Option<K>,
}

impl<K: Ord> KeyRange<K> {
    pub fn contains(&self, key: &K) -> bool {
        if let Some(start) = &self.start {
            if key < start {
                return false;
            }
        }
        if let Some(end) = &self.end {
            if key >= end {
                return false;
            }
        }
        true
    }

    pub fn is_empty(&self) -> bool {
        matches!((&self.start, &self.end), (Some(s), Some(e)) if s >= e)
    }
}
