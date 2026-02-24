use super::codec::{FrameCodec, FrameReader};
use super::state::TransportInner;
use super::wire::WireMessage;
use crate::ir::ReplicaUpcalls;
use serde::{de::DeserializeOwned, Serialize};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
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
) -> Option<mpsc::Sender<Vec<u8>>>
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
        if let Some(tx) = conns.get(&addr)
            && !tx.is_closed()
        {
            return Some(tx.clone());
        }
    }

    // Establish new connection.
    let stream = match TcpStream::connect(addr).await {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!("connect to {addr} failed: {e}");
            return None;
        }
    };

    let (tx, rx) = mpsc::channel::<Vec<u8>>(1024);

    #[cfg(feature = "tls")]
    if let Some(ref connector) = inner.tls_connector {
        let tls_connector = connector.connector();
        let server_name = if let Some(ref name) = inner.tls_server_name {
            name.clone()
        } else {
            rustls::pki_types::ServerName::IpAddress(
                rustls::pki_types::IpAddr::from(addr.ip()),
            )
        };
        match tls_connector.connect(server_name, stream).await {
            Ok(tls_stream) => {
                let (read_half, write_half) = tokio::io::split(tls_stream);
                tokio::spawn(write_loop(write_half, rx));
                tokio::spawn(read_loop_outbound(read_half, Arc::clone(inner)));
                inner.connections.lock().unwrap().insert(addr, tx.clone());
                return Some(tx);
            }
            Err(e) => {
                tracing::warn!("TLS connect to {addr} failed: {e}");
                return None;
            }
        }
    }

    // Plain TCP path.
    let (read_half, write_half) = stream.into_split();
    tokio::spawn(write_loop(write_half, rx));
    tokio::spawn(read_loop_outbound(read_half, Arc::clone(inner)));

    inner.connections.lock().unwrap().insert(addr, tx.clone());
    Some(tx)
}

/// Write loop: drains the mpsc channel and writes frames to the socket.
///
/// Coalesces multiple pending frames into a single write when the
/// channel has buffered messages, reducing syscall count under load.
///
/// Generic over `AsyncWrite` so both plain TCP and TLS streams work.
pub(super) async fn write_loop<W: AsyncWrite + Unpin>(mut writer: W, mut rx: mpsc::Receiver<Vec<u8>>) {
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
///
/// Generic over `AsyncRead` so both plain TCP and TLS streams work.
pub(super) async fn read_loop_outbound<R, U>(
    mut reader: R,
    inner: Arc<TransportInner<U>>,
) where
    R: AsyncRead + Unpin,
    U: ReplicaUpcalls,
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
