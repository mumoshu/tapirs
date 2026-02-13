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

    /// Returns true if this range overlaps the inclusive range [start, end).
    pub fn overlaps_range(&self, start: &K, end: &K) -> bool {
        // No overlap if this range ends before the query starts.
        if let Some(self_end) = &self.end {
            if self_end <= start {
                return false;
            }
        }
        // No overlap if this range starts at or after the query ends.
        if let Some(self_start) = &self.start {
            if self_start >= end {
                return false;
            }
        }
        true
    }
}
