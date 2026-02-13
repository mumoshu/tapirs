mod client;
mod membership;
pub(crate) mod message;
mod op;
mod record;
mod replica;
mod shared_view;
mod view;

#[cfg(test)]
mod tests;

pub use client::{Client, Id as ClientId};
pub use membership::{Membership, Size as MembershipSize};
pub use message::{
    AddMember, Confirm, DoViewChange, FinalizeConsensus, FinalizeInconsistent, Message, ProposeConsensus, ProposeInconsistent, RemoveMember, ReplyConsensus,
    ReplyInconsistent, ReplyUnlogged, RequestUnlogged, StartView,
};
pub use op::Id as OpId;
pub use record::{
    ConsensusEntry as RecordConsensusEntry,
    InconsistentEntry as RecordInconsistentEntry, Record, State as RecordEntryState,
};
pub use replica::{Replica, Upcalls as ReplicaUpcalls};
pub use view::{Number as ViewNumber, View};
