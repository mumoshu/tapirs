use super::address::TcpAddress;
use crate::ir::message::MessageImpl;
use crate::ir::ReplicaUpcalls;
use serde::{Deserialize, Serialize};

/// Concrete IR message type for the Tokio bitcode TCP transport.
pub(crate) type TcpIrMessage<U> = MessageImpl<
    <U as ReplicaUpcalls>::UO,
    <U as ReplicaUpcalls>::UR,
    <U as ReplicaUpcalls>::IO,
    <U as ReplicaUpcalls>::IR,
    <U as ReplicaUpcalls>::CO,
    <U as ReplicaUpcalls>::CR,
    TcpAddress,
    <U as ReplicaUpcalls>::Payload,
>;

/// Wire-level message envelope for the Tokio bitcode TCP transport.
///
/// Same structure as the uring transport's WireMessage — both use
/// length-prefixed bitcode encoding over TCP. Since TcpAddress and
/// UringAddress are both newtypes around SocketAddr, the on-the-wire
/// bytes are identical (bitcode serializes through newtypes
/// transparently), making the two transports wire-compatible.
#[derive(Serialize, Deserialize)]
pub(crate) enum WireMessage<U: ReplicaUpcalls> {
    Request {
        from: TcpAddress,
        request_id: u64,
        #[serde(bound(
            serialize = "U::UO: Serialize, U::UR: Serialize, U::IO: Serialize, U::IR: Serialize, U::CO: Serialize, U::CR: Serialize, U::Payload: Serialize",
            deserialize = "U::UO: Deserialize<'de>, U::UR: Deserialize<'de>, U::IO: Deserialize<'de>, U::IR: Deserialize<'de>, U::CO: Deserialize<'de>, U::CR: Deserialize<'de>, U::Payload: Deserialize<'de>"
        ))]
        payload: TcpIrMessage<U>,
    },
    Reply {
        request_id: u64,
        #[serde(bound(
            serialize = "U::UO: Serialize, U::UR: Serialize, U::IO: Serialize, U::IR: Serialize, U::CO: Serialize, U::CR: Serialize, U::Payload: Serialize",
            deserialize = "U::UO: Deserialize<'de>, U::UR: Deserialize<'de>, U::IO: Deserialize<'de>, U::IR: Deserialize<'de>, U::CO: Deserialize<'de>, U::CR: Deserialize<'de>, U::Payload: Deserialize<'de>"
        ))]
        payload: TcpIrMessage<U>,
    },
    FireAndForget {
        from: TcpAddress,
        #[serde(bound(
            serialize = "U::UO: Serialize, U::UR: Serialize, U::IO: Serialize, U::IR: Serialize, U::CO: Serialize, U::CR: Serialize, U::Payload: Serialize",
            deserialize = "U::UO: Deserialize<'de>, U::UR: Deserialize<'de>, U::IO: Deserialize<'de>, U::IR: Deserialize<'de>, U::CO: Deserialize<'de>, U::CR: Deserialize<'de>, U::Payload: Deserialize<'de>"
        ))]
        payload: TcpIrMessage<U>,
    },
}
