use serde::{Deserialize, Serialize};

/// Reusable min-prepare-time state machine.
///
/// Tracks two thresholds used by TAPIR to reject stale prepare requests:
/// - **tentative** (`min_prepare_time`): the working threshold used by
///   `check_prepare_status`. Can be raised speculatively and rolled back.
/// - **finalized** (`finalized_min_prepare_time`): quorum-confirmed floor.
///   Monotonically increasing; survives view changes.
///
/// See `TapirStore::raise_min_prepare_time` for the full state model docs.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MinPrepareTimes {
    min_prepare_time: u64,
    pub finalized_min_prepare_time: u64,
}

impl Default for MinPrepareTimes {
    fn default() -> Self {
        Self::new()
    }
}

impl MinPrepareTimes {
    pub fn new() -> Self {
        Self {
            min_prepare_time: 0,
            finalized_min_prepare_time: 0,
        }
    }

    pub fn min_prepare_time(&self) -> u64 {
        self.min_prepare_time
    }

    /// Speculatively raise the tentative min_prepare_time.
    ///
    /// `min_prepared_ts` is the smallest commit timestamp among all currently
    /// prepared transactions, or `None` if no transactions are prepared
    /// (treated as `u64::MAX`).
    pub fn raise(&mut self, time: u64, min_prepared_ts: Option<u64>) -> u64 {
        let cap = min_prepared_ts.unwrap_or(u64::MAX);
        let new_mpt = self.min_prepare_time.max(time.min(cap));
        self.min_prepare_time = new_mpt;
        self.min_prepare_time
    }

    /// Finalize the min_prepare_time after IR quorum agreement.
    pub fn finalize(&mut self, time: u64) {
        self.finalized_min_prepare_time = self.finalized_min_prepare_time.max(time);
        self.min_prepare_time = self.min_prepare_time.max(self.finalized_min_prepare_time);
    }

    /// Sync the min_prepare_time from the leader's authoritative value.
    /// Can roll back tentative below finalized.
    pub fn sync(&mut self, time: u64) {
        self.finalized_min_prepare_time = self.finalized_min_prepare_time.max(time);
        self.min_prepare_time = self.min_prepare_time.min(self.finalized_min_prepare_time);
    }

    /// Reset tentative min_prepare_time to the finalized value,
    /// discarding speculative raises.
    pub fn reset_to_finalized(&mut self) {
        self.min_prepare_time = self.finalized_min_prepare_time;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raise_no_prepared() {
        let mut mpt = MinPrepareTimes::new();
        assert_eq!(mpt.raise(100, None), 100);
        assert_eq!(mpt.raise(50, None), 100); // monotonic
        assert_eq!(mpt.raise(200, None), 200);
    }

    #[test]
    fn raise_capped_by_min_prepared() {
        let mut mpt = MinPrepareTimes::new();
        mpt.raise(100, None);
        // min_prepared=50 caps raise(200) to 50, which < current 100
        assert_eq!(mpt.raise(200, Some(50)), 100);
    }

    #[test]
    fn finalize_raises_both() {
        let mut mpt = MinPrepareTimes::new();
        mpt.raise(50, None);
        mpt.finalize(100);
        assert_eq!(mpt.raise(0, None), 100);
        assert_eq!(mpt.finalized_min_prepare_time, 100);
    }

    #[test]
    fn sync_rolls_back_tentative() {
        let mut mpt = MinPrepareTimes::new();
        mpt.raise(100, None);
        mpt.finalize(30);
        mpt.sync(50);
        assert_eq!(mpt.raise(0, None), 50);
        assert_eq!(mpt.finalized_min_prepare_time, 50);
    }

    #[test]
    fn reset_to_finalized() {
        let mut mpt = MinPrepareTimes::new();
        mpt.finalize(42);
        mpt.raise(100, None);
        mpt.reset_to_finalized();
        assert_eq!(mpt.raise(0, None), 42);
    }
}
