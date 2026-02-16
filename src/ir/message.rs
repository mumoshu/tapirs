use super::{
    record::RecordImpl, shared_view::SharedView, OpId, RecordEntryState, ReplicaUpcalls, ViewNumber,
};
use crate::Transport;
use serde::{Deserialize, Serialize};
use std::{fmt::Debug, sync::Arc};

pub type Message<U, T> = MessageImpl<
    <U as ReplicaUpcalls>::UO,
    <U as ReplicaUpcalls>::UR,
    <U as ReplicaUpcalls>::IO,
    <U as ReplicaUpcalls>::IR,
    <U as ReplicaUpcalls>::CO,
    <U as ReplicaUpcalls>::CR,
    <T as Transport<U>>::Address,
>;

#[derive(Clone, derive_more::From, derive_more::TryInto, Serialize, Deserialize)]
pub enum MessageImpl<UO, UR, IO, IR, CO, CR, A> {
    RequestUnlogged(RequestUnlogged<UO>),
    ReplyUnlogged(ReplyUnlogged<UR, A>),
    ProposeInconsistent(ProposeInconsistent<IO>),
    ProposeConsensus(ProposeConsensus<CO>),
    ReplyInconsistent(ReplyInconsistent<A>),
    ReplyConsensus(ReplyConsensus<CR, A>),
    FinalizeInconsistent(FinalizeInconsistent),
    FinalizeInconsistentReply(FinalizeInconsistentReply<IR, A>),
    FinalizeConsensus(FinalizeConsensus<CR>),
    Confirm(Confirm<A>),
    DoViewChange(DoViewChange<IO, CO, CR, A>),
    StartView(StartView<IO, CO, CR, A>),
    AddMember(AddMember<A>),
    RemoveMember(RemoveMember<A>),
    Reconfigure(Reconfigure),
    FetchLeaderRecord(FetchLeaderRecord),
    LeaderRecordReply(LeaderRecordReply<IO, CO, CR, A>),
    BootstrapRecord(BootstrapRecord<IO, CO, CR, A>),
}

impl<UO: Debug, UR: Debug, IO: Debug, IR: Debug, CO: Debug, CR: Debug, A: Debug> Debug
    for MessageImpl<UO, UR, IO, IR, CO, CR, A>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RequestUnlogged(r) => Debug::fmt(r, f),
            Self::ReplyUnlogged(r) => Debug::fmt(r, f),
            Self::ProposeInconsistent(r) => Debug::fmt(r, f),
            Self::ProposeConsensus(r) => Debug::fmt(r, f),
            Self::ReplyInconsistent(r) => Debug::fmt(r, f),
            Self::ReplyConsensus(r) => Debug::fmt(r, f),
            Self::FinalizeInconsistent(r) => Debug::fmt(r, f),
            Self::FinalizeInconsistentReply(r) => Debug::fmt(r, f),
            Self::FinalizeConsensus(r) => Debug::fmt(r, f),
            Self::Confirm(r) => Debug::fmt(r, f),
            Self::DoViewChange(r) => Debug::fmt(r, f),
            Self::StartView(r) => Debug::fmt(r, f),
            Self::AddMember(r) => Debug::fmt(r, f),
            Self::RemoveMember(r) => Debug::fmt(r, f),
            Self::Reconfigure(r) => Debug::fmt(r, f),
            Self::FetchLeaderRecord(r) => Debug::fmt(r, f),
            Self::LeaderRecordReply(r) => Debug::fmt(r, f),
            Self::BootstrapRecord(r) => Debug::fmt(r, f),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestUnlogged<UO> {
    pub op: UO,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplyUnlogged<UR, A> {
    pub result: UR,
    pub view: SharedView<A>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProposeInconsistent<IO> {
    pub op_id: OpId,
    pub op: IO,
    /// Highest view number known to the client,
    /// used for identifying old messages and
    /// starting view changes.
    pub recent: ViewNumber,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProposeConsensus<CO> {
    pub op_id: OpId,
    pub op: CO,
    /// Highest view number known to the client,
    /// used for identifying old messages and
    /// starting view changes.
    pub recent: ViewNumber,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplyInconsistent<A> {
    pub op_id: OpId,
    pub view: SharedView<A>,
    /// If `None`, the request couldn't be processed because
    /// `recent` wasn't recent.
    pub state: Option<RecordEntryState>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplyConsensus<CR, A> {
    pub op_id: OpId,
    pub view: SharedView<A>,
    /// If `None`, the request couldn't be processed because
    /// `recent` wasn't recent.
    pub result_state: Option<(CR, RecordEntryState)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FinalizeInconsistent {
    pub op_id: OpId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FinalizeInconsistentReply<IR, A> {
    pub op_id: OpId,
    pub result: IR,
    pub view: SharedView<A>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FinalizeConsensus<CR> {
    pub op_id: OpId,
    pub result: CR,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Confirm<A> {
    pub op_id: OpId,
    pub view: SharedView<A>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum RecordPayload<IO, CO, CR> {
    Full(RecordImpl<IO, CO, CR>),
    Delta {
        base_view: ViewNumber,
        entries: RecordImpl<IO, CO, CR>,
    },
}

impl<IO: Clone, CO: Clone, CR: Clone> RecordPayload<IO, CO, CR> {
    pub fn resolve(self, base: Option<&RecordImpl<IO, CO, CR>>) -> RecordImpl<IO, CO, CR> {
        match self {
            Self::Full(record) => record,
            Self::Delta { entries, .. } => {
                let base = base.expect("delta requires matching base");
                let mut full = base.clone();
                // insert (overwrite), not or_insert: delta entries may be updates
                // to existing entries (FinalizeConsensus changes result + state).
                for (op_id, entry) in entries.inconsistent {
                    full.inconsistent.insert(op_id, entry);
                }
                for (op_id, entry) in entries.consensus {
                    full.consensus.insert(op_id, entry);
                }
                full
            }
        }
    }
}

pub(crate) struct LeaderRecord<IO, CO, CR, A> {
    pub record: Arc<RecordImpl<IO, CO, CR>>,
    pub view: SharedView<A>,
}

/// Informs a replica about a new view.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DoViewChange<IO, CO, CR, A> {
    /// View to change to.
    pub view: SharedView<A>,
    /// From client (don't send cached leader record).
    pub from_client: bool,
    /// Is `Some` when sent from replica to new leader.
    pub addendum: Option<ViewChangeAddendum<IO, CO, CR, A>>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct ViewChangeAddendum<IO, CO, CR, A> {
    /// Sender replica's record (full or delta).
    pub payload: RecordPayload<IO, CO, CR>,
    /// Latest view in which sender replica had a normal state.
    pub latest_normal_view: SharedView<A>,
}

impl<IO, CO, CR, A: Debug> Debug for ViewChangeAddendum<IO, CO, CR, A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Addendum")
            .field("latest_normal_view", &self.latest_normal_view)
            .finish_non_exhaustive()
    }
}

/// From leader to inform a replica that a new view has begun.
#[derive(Clone, Serialize, Deserialize)]
pub struct StartView<IO, CO, CR, A> {
    /// Leader's merged record (full or delta).
    pub payload: RecordPayload<IO, CO, CR>,
    /// New view.
    pub view: SharedView<A>,
}

impl<IO, CO, CR, A: Debug> Debug for StartView<IO, CO, CR, A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StartView")
            .field("view", &self.view)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AddMember<A> {
    pub address: A,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RemoveMember<A> {
    pub address: A,
}

/// Sets `view.app_config` and triggers a view change to propagate the
/// new configuration to all replicas atomically.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Reconfigure {
    pub config: Vec<u8>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FetchLeaderRecord;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LeaderRecordReply<IO, CO, CR, A> {
    pub record: Option<Arc<RecordImpl<IO, CO, CR>>>,
    pub view: Option<SharedView<A>>,
}

/// Administrative message: client sends to a fresh replica to pre-load it
/// with a record. The replica converts this to a self-directed StartView.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BootstrapRecord<IO, CO, CR, A> {
    pub record: RecordImpl<IO, CO, CR>,
    pub view: SharedView<A>,
}
