mod client;
mod inmem_versioned_ir_record_store;
mod ir_record_store;
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
pub use ir_record_store::IrRecordStore;
pub use membership::{Membership, Size as MembershipSize};
pub use message::{
    AddMember, Confirm, DoViewChange, FinalizeConsensus, FinalizeInconsistent,
    Message, ProposeConsensus, ProposeInconsistent,
    RemoveMember, ReplyConsensus, ReplyInconsistent, ReplyUnlogged, RequestUnlogged,
    StartView,
};
pub use op::Id as OpId;
pub use inmem_versioned_ir_record_store::VersionedRecord;
pub use record::{
    ConsensusEntry as RecordConsensusEntry,
    InconsistentEntry as RecordInconsistentEntry, Record, RecordView,
    State as RecordEntryState, VersionedEntry,
};
pub use replica::{Replica, ReplicaMetrics, Upcalls as ReplicaUpcalls};
pub use shared_view::SharedView;
pub use view::{Number as ViewNumber, View};
