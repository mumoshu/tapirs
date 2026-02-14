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
        if let Some(start) = &self.start
            && key < start {
                return false;
            }
        if let Some(end) = &self.end
            && key >= end {
                return false;
            }
        true
    }

    pub fn is_empty(&self) -> bool {
        matches!((&self.start, &self.end), (Some(s), Some(e)) if s >= e)
    }

    /// Returns true if this range overlaps another `KeyRange`.
    /// Two ranges [a_start, a_end) and [b_start, b_end) overlap when
    /// a_start < b_end AND b_start < a_end (None = unbounded).
    pub fn overlaps(&self, other: &KeyRange<K>) -> bool {
        // No overlap if self ends before other starts.
        if let (Some(self_end), Some(other_start)) = (&self.end, &other.start) {
            if self_end <= other_start {
                return false;
            }
        }
        // No overlap if other ends before self starts.
        if let (Some(other_end), Some(self_start)) = (&other.end, &self.start) {
            if other_end <= self_start {
                return false;
            }
        }
        true
    }

    /// Returns true if this range overlaps the inclusive range [start, end).
    pub fn overlaps_range(&self, start: &K, end: &K) -> bool {
        // No overlap if this range ends before the query starts.
        if let Some(self_end) = &self.end
            && self_end <= start {
                return false;
            }
        // No overlap if this range starts at or after the query ends.
        if let Some(self_start) = &self.start
            && self_start >= end {
                return false;
            }
        true
    }
}
