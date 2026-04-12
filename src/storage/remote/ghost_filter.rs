use serde::{Deserialize, Serialize};

/// Hides MVCC entries in the range `(cutoff_ts, ceiling_ts]`.
///
/// # When
///
/// After restoring a cluster from a cross-shard consistent snapshot where
/// shards were backed up at different points in time.
///
/// # Why
///
/// A cross-shard transaction may have committed on shard A (max_ts=200)
/// but not yet on shard B (max_ts=150). Without the filter, shard A would
/// serve the transaction's writes while shard B would not, violating
/// cross-shard consistency. The filter makes shard A's view match shard B's
/// by hiding entries in the range that only some shards have.
///
/// # What happens without it
///
/// Reads on different shards return inconsistent results for cross-shard
/// transactions — shard A returns the new value, shard B returns the old
/// value (or not-found). This breaks application invariants that depend
/// on atomic cross-shard updates.
///
/// # Timestamp ranges
///
/// - `ts <= cutoff_ts` → visible (consistent across all shards)
/// - `cutoff_ts < ts <= ceiling_ts` → **hidden** (may be inconsistent)
/// - `ts > ceiling_ts` → visible (new writes after restore)
///
/// `cutoff_ts` is global: `min(max_committed_ts)` across all shards.
/// `ceiling_ts` is per-shard: this shard's `max_committed_ts` at backup time.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GhostFilter {
    pub cutoff_ts: u64,
    pub ceiling_ts: u64,
}

impl GhostFilter {
    /// Returns true if the given timestamp falls in the hidden range.
    pub fn is_hidden(&self, ts_time: u64) -> bool {
        ts_time > self.cutoff_ts && ts_time <= self.ceiling_ts
    }

    /// Returns true if the hidden range is empty (cutoff >= ceiling).
    pub fn is_empty(&self) -> bool {
        self.cutoff_ts >= self.ceiling_ts
    }

    /// Clamp a snapshot timestamp to skip the ghost range.
    ///
    /// When a read requests a snapshot at ts=180 (in the ghost range
    /// (150, 200]), clamping to cutoff_ts=150 makes the MVCC search skip
    /// all entries in (150, 200] and find the latest entry at or below
    /// ts=150 instead. This is correct because all shards are guaranteed
    /// to have all entries at ts<=150 (the minimum max_ts across shards).
    ///
    /// Entries above ceiling (ts>200) are new writes after restore and
    /// are NOT clamped — they are visible because all shards produce them
    /// going forward.
    ///
    /// Returns `cutoff_ts` if ts is in the ghost range, `ts` unchanged
    /// otherwise.
    pub fn clamp_ts(&self, ts: u64) -> u64 {
        if self.is_hidden(ts) {
            self.cutoff_ts
        } else {
            ts
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn range_boundaries() {
        let gf = GhostFilter {
            cutoff_ts: 150,
            ceiling_ts: 200,
        };

        assert!(!gf.is_hidden(150));
        assert!(!gf.is_hidden(100));
        assert!(!gf.is_hidden(0));
        assert!(gf.is_hidden(151));
        assert!(gf.is_hidden(175));
        assert!(gf.is_hidden(200));
        assert!(!gf.is_hidden(201));
        assert!(!gf.is_hidden(1000));
    }

    #[test]
    fn empty_filter() {
        let gf = GhostFilter { cutoff_ts: 150, ceiling_ts: 150 };
        assert!(gf.is_empty());
        assert!(!gf.is_hidden(150));
        assert!(!gf.is_hidden(151));

        let gf2 = GhostFilter { cutoff_ts: 200, ceiling_ts: 150 };
        assert!(gf2.is_empty());
    }

    #[test]
    fn clamp_ts_below_cutoff_unchanged() {
        let gf = GhostFilter { cutoff_ts: 150, ceiling_ts: 200 };
        assert_eq!(gf.clamp_ts(100), 100);
        assert_eq!(gf.clamp_ts(150), 150);
    }

    #[test]
    fn clamp_ts_in_ghost_range_returns_cutoff() {
        let gf = GhostFilter { cutoff_ts: 150, ceiling_ts: 200 };
        assert_eq!(gf.clamp_ts(151), 150);
        assert_eq!(gf.clamp_ts(175), 150);
        assert_eq!(gf.clamp_ts(200), 150);
    }

    #[test]
    fn clamp_ts_above_ceiling_unchanged() {
        let gf = GhostFilter { cutoff_ts: 150, ceiling_ts: 200 };
        assert_eq!(gf.clamp_ts(201), 201);
        assert_eq!(gf.clamp_ts(300), 300);
    }
}
