use crate::{IrClientId, OccTimestamp};
use crate::storage::io::memtable::MaxValue;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

#[derive(Clone, Copy, Eq, PartialEq, Hash, Ord, PartialOrd, Serialize, Deserialize)]
pub struct Timestamp {
    pub time: u64,
    pub client_id: IrClientId,
}

impl Debug for Timestamp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Ts({}, {})", self.time, self.client_id.0)
    }
}

impl Default for Timestamp {
    fn default() -> Self {
        Self {
            time: 0,
            client_id: IrClientId(0),
        }
    }
}

impl OccTimestamp for Timestamp {
    type Time = u64;

    fn time(&self) -> Self::Time {
        self.time
    }

    fn from_time(time: Self::Time) -> Self {
        Self {
            time,
            client_id: IrClientId(u64::MAX),
        }
    }
}

impl MaxValue for Timestamp {
    fn max_value() -> Self {
        Self {
            time: u64::MAX,
            client_id: IrClientId(u64::MAX),
        }
    }
}
