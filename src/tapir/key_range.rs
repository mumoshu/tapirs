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

impl<K: Ord + Eq + Clone> KeyRange<K> {
    /// Returns true if the two ranges are adjacent (one's end equals the other's start).
    /// Order-independent: `[A, M).adjacent(&[M, Z))` and `[M, Z).adjacent(&[A, M))` are both true.
    pub fn adjacent(&self, other: &KeyRange<K>) -> bool {
        match (&self.end, &other.start) {
            (Some(e), Some(s)) if e == s => return true,
            _ => {}
        }
        match (&other.end, &self.start) {
            (Some(e), Some(s)) if e == s => return true,
            _ => {}
        }
        false
    }

    /// Combines two adjacent ranges into one. Panics if not adjacent.
    pub fn union(&self, other: &KeyRange<K>) -> KeyRange<K> {
        assert!(self.adjacent(other), "ranges must be adjacent to union");
        if matches!((&self.end, &other.start), (Some(e), Some(s)) if e == s) {
            KeyRange {
                start: self.start.clone(),
                end: other.end.clone(),
            }
        } else {
            KeyRange {
                start: other.start.clone(),
                end: self.end.clone(),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn adjacent_left_right() {
        let a = KeyRange { start: Some(10), end: Some(50) };
        let b = KeyRange { start: Some(50), end: Some(90) };
        assert!(a.adjacent(&b));
    }

    #[test]
    fn adjacent_right_left() {
        let a = KeyRange { start: Some(50), end: Some(90) };
        let b = KeyRange { start: Some(10), end: Some(50) };
        assert!(a.adjacent(&b));
    }

    #[test]
    fn adjacent_unbounded_left() {
        let a: KeyRange<u64> = KeyRange { start: None, end: Some(50) };
        let b = KeyRange { start: Some(50), end: Some(90) };
        assert!(a.adjacent(&b));
    }

    #[test]
    fn adjacent_unbounded_right() {
        let a = KeyRange { start: Some(10), end: Some(50) };
        let b: KeyRange<u64> = KeyRange { start: Some(50), end: None };
        assert!(a.adjacent(&b));
    }

    #[test]
    fn adjacent_both_unbounded() {
        let a: KeyRange<u64> = KeyRange { start: None, end: Some(50) };
        let b: KeyRange<u64> = KeyRange { start: Some(50), end: None };
        assert!(a.adjacent(&b));
    }

    #[test]
    fn not_adjacent_gap() {
        let a = KeyRange { start: Some(10), end: Some(40) };
        let b = KeyRange { start: Some(50), end: Some(90) };
        assert!(!a.adjacent(&b));
    }

    #[test]
    fn not_adjacent_overlap() {
        let a = KeyRange { start: Some(10), end: Some(60) };
        let b = KeyRange { start: Some(50), end: Some(90) };
        assert!(!a.adjacent(&b));
    }

    #[test]
    fn not_adjacent_same() {
        let a = KeyRange { start: Some(10), end: Some(50) };
        let b = KeyRange { start: Some(10), end: Some(50) };
        assert!(!a.adjacent(&b));
    }

    #[test]
    fn union_basic() {
        let a = KeyRange { start: Some(10), end: Some(50) };
        let b = KeyRange { start: Some(50), end: Some(90) };
        let u = a.union(&b);
        assert_eq!(u.start, Some(10));
        assert_eq!(u.end, Some(90));
    }

    #[test]
    fn union_reversed() {
        let a = KeyRange { start: Some(50), end: Some(90) };
        let b = KeyRange { start: Some(10), end: Some(50) };
        let u = a.union(&b);
        assert_eq!(u.start, Some(10));
        assert_eq!(u.end, Some(90));
    }

    #[test]
    fn union_unbounded() {
        let a: KeyRange<u64> = KeyRange { start: None, end: Some(50) };
        let b: KeyRange<u64> = KeyRange { start: Some(50), end: None };
        let u = a.union(&b);
        assert_eq!(u.start, None);
        assert_eq!(u.end, None);
    }
}
