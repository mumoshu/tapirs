use super::codec::{FrameCodec, FrameReader};
use super::state::TransportInner;
use super::wire::WireMessage;
use crate::ir::ReplicaUpcalls;
use serde::{de::DeserializeOwned, Serialize};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
use tracing::warn;

/// Bind a TCP listener and spawn the accept loop.
pub(super) async fn listen<U: ReplicaUpcalls>(
    addr: SocketAddr,
    inner: Arc<TransportInner<U>>,
) -> tokio::io::Result<()>
where
    U::UO: Serialize + DeserializeOwned,
    U::UR: Serialize + DeserializeOwned,
    U::IO: Serialize + DeserializeOwned,
    U::IR: Serialize + DeserializeOwned,
    U::CO: Serialize + DeserializeOwned,
    U::CR: Serialize + DeserializeOwned,
    U::Payload: Serialize + DeserializeOwned,
{
    let listener = TcpListener::bind(addr).await?;
    tokio::spawn(accept_loop(listener, inner));
    Ok(())
}

/// Accept connections on a pre-bound `std::net::TcpListener`.
///
/// Converts the std listener to a tokio `TcpListener` and spawns the
/// accept loop. Eliminates the TOCTOU gap between port allocation and
/// listener startup.
pub(super) fn listen_from_std<U: ReplicaUpcalls>(
    listener: std::net::TcpListener,
    inner: Arc<TransportInner<U>>,
) -> tokio::io::Result<()>
where
    U::UO: Serialize + DeserializeOwned,
    U::UR: Serialize + DeserializeOwned,
    U::IO: Serialize + DeserializeOwned,
    U::IR: Serialize + DeserializeOwned,
    U::CO: Serialize + DeserializeOwned,
    U::CR: Serialize + DeserializeOwned,
    U::Payload: Serialize + DeserializeOwned,
{
    listener.set_nonblocking(true)?;
    let listener = TcpListener::from_std(listener)?;
    tokio::spawn(accept_loop(listener, inner));
    Ok(())
}

/// Accept loop: spawns a handler task per inbound connection.
async fn accept_loop<U: ReplicaUpcalls>(
    listener: TcpListener,
    inner: Arc<TransportInner<U>>,
) where
    U::UO: Serialize + DeserializeOwned,
    U::UR: Serialize + DeserializeOwned,
    U::IO: Serialize + DeserializeOwned,
    U::IR: Serialize + DeserializeOwned,
    U::CO: Serialize + DeserializeOwned,
    U::CR: Serialize + DeserializeOwned,
    U::Payload: Serialize + DeserializeOwned,
{
    loop {
        match listener.accept().await {
            Ok((stream, _peer)) => {
                let inner = Arc::clone(&inner);
                tokio::spawn(async move {
                    accept_connection(stream, inner).await;
                });
            }
            Err(e) => {
                warn!("accept error: {e}");
            }
        }
    }
}

/// Handle an accepted TCP connection, optionally wrapping with TLS.
async fn accept_connection<U: ReplicaUpcalls>(
    stream: tokio::net::TcpStream,
    inner: Arc<TransportInner<U>>,
) where
    U::UO: Serialize + DeserializeOwned,
    U::UR: Serialize + DeserializeOwned,
    U::IO: Serialize + DeserializeOwned,
    U::IR: Serialize + DeserializeOwned,
    U::CO: Serialize + DeserializeOwned,
    U::CR: Serialize + DeserializeOwned,
    U::Payload: Serialize + DeserializeOwned,
{
    #[cfg(feature = "tls")]
    if let Some(ref acceptor) = inner.tls_acceptor {
        let tls_acceptor = acceptor.acceptor();
        match tls_acceptor.accept(stream).await {
            Ok(tls_stream) => {
                let (read, write) = tokio::io::split(tls_stream);
                read_loop_inbound(read, write, inner).await;
            }
            Err(e) => {
                warn!("TLS accept error: {e}");
            }
        }
        return;
    }

    // Plain TCP path.
    let (read, write) = stream.into_split();
    read_loop_inbound(read, write, inner).await;
}

/// Inbound read loop: dispatches requests to the receive callback,
/// writes replies back inline.
///
/// The receive callback (IrReplica::receive()) is synchronous —
/// it processes the message and returns immediately. This avoids
/// async overhead for the hot path.
///
/// Generic over `AsyncRead`/`AsyncWrite` so both plain TCP and TLS
/// streams can use the same loop.
pub(super) async fn read_loop_inbound<R, W, U>(
    mut reader: R,
    mut writer: W,
    inner: Arc<TransportInner<U>>,
) where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
    U: ReplicaUpcalls,
    U::UO: Serialize + DeserializeOwned,
    U::UR: Serialize + DeserializeOwned,
    U::IO: Serialize + DeserializeOwned,
    U::IR: Serialize + DeserializeOwned,
    U::CO: Serialize + DeserializeOwned,
    U::CR: Serialize + DeserializeOwned,
    U::Payload: Serialize + DeserializeOwned,
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
            match wire {
                WireMessage::Request {
                    from,
                    request_id,
                    payload,
                } => {
                    let cb = inner.receive_callback.lock().unwrap().clone();
                    if let Some(cb) = cb
                        && let Some(reply) = cb(from, payload)
                    {
                        let reply_wire =
                            WireMessage::<U>::Reply {
                                request_id,
                                payload: reply,
                            };
                        if let Ok(frame) = FrameCodec::encode(&reply_wire) {
                            let _ = writer.write_all(&frame).await;
                        }
                    }
                }
                WireMessage::Reply {
                    request_id,
                    payload,
                } => {
                    // Bidirectional: also handle replies on inbound connections.
                    let sender =
                        inner.pending_replies.lock().unwrap().remove(&request_id);
                    if let Some(sender) = sender {
                        let _ = sender.send(payload);
                    }
                }
                WireMessage::FireAndForget { from, payload } => {
                    let cb = inner.receive_callback.lock().unwrap().clone();
                    if let Some(cb) = cb {
                        cb(from, payload);
                    }
                }
            }
        }
    }
}
