use super::address::UringAddress;
use crate::ir::ReplicaUpcalls;
use crate::ir::message::MessageImpl;
use serde::{Deserialize, Serialize};

/// Concrete IR message type for the UringTransport.
pub(crate) type UringIrMessage<U> = MessageImpl<
    <U as ReplicaUpcalls>::UO,
    <U as ReplicaUpcalls>::UR,
    <U as ReplicaUpcalls>::IO,
    <U as ReplicaUpcalls>::IR,
    <U as ReplicaUpcalls>::CO,
    <U as ReplicaUpcalls>::CR,
    UringAddress,
>;

/// Wire-level message envelope for the io_uring transport.
#[derive(Serialize, Deserialize)]
pub(crate) enum WireMessage<U: ReplicaUpcalls> {
    Request {
        from: UringAddress,
        request_id: u64,
        #[serde(bound(
            serialize = "<U as ReplicaUpcalls>::UO: Serialize, <U as ReplicaUpcalls>::UR: Serialize, <U as ReplicaUpcalls>::IO: Serialize, <U as ReplicaUpcalls>::IR: Serialize, <U as ReplicaUpcalls>::CO: Serialize, <U as ReplicaUpcalls>::CR: Serialize",
            deserialize = "<U as ReplicaUpcalls>::UO: Deserialize<'de>, <U as ReplicaUpcalls>::UR: Deserialize<'de>, <U as ReplicaUpcalls>::IO: Deserialize<'de>, <U as ReplicaUpcalls>::IR: Deserialize<'de>, <U as ReplicaUpcalls>::CO: Deserialize<'de>, <U as ReplicaUpcalls>::CR: Deserialize<'de>"
        ))]
        payload: UringIrMessage<U>,
    },
    Reply {
        request_id: u64,
        #[serde(bound(
            serialize = "<U as ReplicaUpcalls>::UO: Serialize, <U as ReplicaUpcalls>::UR: Serialize, <U as ReplicaUpcalls>::IO: Serialize, <U as ReplicaUpcalls>::IR: Serialize, <U as ReplicaUpcalls>::CO: Serialize, <U as ReplicaUpcalls>::CR: Serialize",
            deserialize = "<U as ReplicaUpcalls>::UO: Deserialize<'de>, <U as ReplicaUpcalls>::UR: Deserialize<'de>, <U as ReplicaUpcalls>::IO: Deserialize<'de>, <U as ReplicaUpcalls>::IR: Deserialize<'de>, <U as ReplicaUpcalls>::CO: Deserialize<'de>, <U as ReplicaUpcalls>::CR: Deserialize<'de>"
        ))]
        payload: UringIrMessage<U>,
    },
    FireAndForget {
        from: UringAddress,
        #[serde(bound(
            serialize = "<U as ReplicaUpcalls>::UO: Serialize, <U as ReplicaUpcalls>::UR: Serialize, <U as ReplicaUpcalls>::IO: Serialize, <U as ReplicaUpcalls>::IR: Serialize, <U as ReplicaUpcalls>::CO: Serialize, <U as ReplicaUpcalls>::CR: Serialize",
            deserialize = "<U as ReplicaUpcalls>::UO: Deserialize<'de>, <U as ReplicaUpcalls>::UR: Deserialize<'de>, <U as ReplicaUpcalls>::IO: Deserialize<'de>, <U as ReplicaUpcalls>::IR: Deserialize<'de>, <U as ReplicaUpcalls>::CO: Deserialize<'de>, <U as ReplicaUpcalls>::CR: Deserialize<'de>"
        ))]
        payload: UringIrMessage<U>,
    },
}
