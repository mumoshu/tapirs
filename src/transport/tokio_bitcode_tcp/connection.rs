use super::codec::{FrameCodec, FrameReader};
use super::state::TransportInner;
use super::wire::WireMessage;
use crate::ir::ReplicaUpcalls;
use serde::{de::DeserializeOwned, Serialize};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio::sync::mpsc;

/// Get or lazily create an outbound TCP connection to a peer.
///
/// Returns the mpsc sender for enqueuing frames. If the connection
/// does not exist or was closed, establishes a new one and spawns
/// the read/write background tasks.
pub(super) async fn ensure_connection<U: ReplicaUpcalls>(
    inner: &Arc<TransportInner<U>>,
    addr: SocketAddr,
) -> mpsc::Sender<Vec<u8>>
where
    U::UO: Serialize + DeserializeOwned,
    U::UR: Serialize + DeserializeOwned,
    U::IO: Serialize + DeserializeOwned,
    U::IR: Serialize + DeserializeOwned,
    U::CO: Serialize + DeserializeOwned,
    U::CR: Serialize + DeserializeOwned,
{
    // Check for existing open connection (short lock).
    {
        let conns = inner.connections.lock().unwrap();
        if let Some(tx) = conns.get(&addr) {
            if !tx.is_closed() {
                return tx.clone();
            }
        }
    }

    // Establish new connection.
    let stream = TcpStream::connect(addr)
        .await
        .unwrap_or_else(|e| panic!("connect to {addr} failed: {e}"));
    let (read_half, write_half) = stream.into_split();

    let (tx, rx) = mpsc::channel::<Vec<u8>>(1024);

    tokio::spawn(write_loop(write_half, rx));
    tokio::spawn(read_loop_outbound(read_half, Arc::clone(inner)));

    inner.connections.lock().unwrap().insert(addr, tx.clone());
    tx
}

/// Write loop: drains the mpsc channel and writes frames to the socket.
///
/// Coalesces multiple pending frames into a single write when the
/// channel has buffered messages, reducing syscall count under load.
async fn write_loop(mut writer: OwnedWriteHalf, mut rx: mpsc::Receiver<Vec<u8>>) {
    while let Some(first_frame) = rx.recv().await {
        // Coalesce: drain any additional buffered frames.
        let mut buf = first_frame;
        while let Ok(extra) = rx.try_recv() {
            buf.extend_from_slice(&extra);
        }
        if writer.write_all(&buf).await.is_err() {
            break;
        }
    }
}

/// Outbound read loop: dispatches replies to pending_replies.
///
/// Runs on connections initiated by this transport (for receiving
/// reply messages). When the connection is closed, pending replies
/// will timeout in send() and trigger retry with reconnection.
async fn read_loop_outbound<U: ReplicaUpcalls>(
    mut reader: OwnedReadHalf,
    inner: Arc<TransportInner<U>>,
) where
    U::UO: Serialize + DeserializeOwned,
    U::UR: Serialize + DeserializeOwned,
    U::IO: Serialize + DeserializeOwned,
    U::IR: Serialize + DeserializeOwned,
    U::CO: Serialize + DeserializeOwned,
    U::CR: Serialize + DeserializeOwned,
{
    let mut frame_reader = FrameReader::new();
    loop {
        let buf = frame_reader.recv_buf();
        let n = match reader.read(buf).await {
            Ok(0) | Err(_) => break,
            Ok(n) => n,
        };
        frame_reader.advance(n);

        while let Ok(Some(payload)) = frame_reader.try_read_frame() {
            let wire: WireMessage<U> = match FrameCodec::decode(&payload) {
                Ok(m) => m,
                Err(_) => continue,
            };
            if let WireMessage::Reply {
                request_id,
                payload,
            } = wire
            {
                let sender = inner.pending_replies.lock().unwrap().remove(&request_id);
                if let Some(sender) = sender {
                    let _ = sender.send(payload);
                }
            }
        }
    }
}
