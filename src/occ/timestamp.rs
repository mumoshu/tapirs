use serde::{de::DeserializeOwned, Serialize};
use std::fmt::Debug;

pub trait Timestamp: Ord + Copy + Default + Debug + Serialize + DeserializeOwned {
    type Time: Ord + Copy + Serialize + DeserializeOwned;

    fn time(&self) -> Self::Time;

    /// Construct a timestamp from just the time component.
    /// Other fields (e.g., client_id) are set to default values.
    /// Used by DiskStore to reconstruct timestamps from stored time values.
    fn from_time(time: Self::Time) -> Self;
}

impl Timestamp for u64 {
    type Time = u64;

    fn time(&self) -> Self::Time {
        *self
    }

    fn from_time(time: Self::Time) -> Self {
        time
    }
}
