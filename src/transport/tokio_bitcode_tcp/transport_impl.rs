use super::address::TcpAddress;
use super::codec::FrameCodec;
use super::connection::ensure_connection;
use super::state::TcpTransport;
use super::wire::{TcpIrMessage, WireMessage};
use crate::ir::ReplicaUpcalls;
use crate::tapir::{Key, Value};
use crate::transport::{TapirTransport, Transport};
use crate::{IrMembership, IrMessage, ShardNumber, TapirReplica};
use serde::{de::DeserializeOwned, Serialize};
use std::fmt::Debug;
use std::future::Future;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

impl<U: ReplicaUpcalls> Transport<U> for TcpTransport<U>
where
    U::UO: Serialize + DeserializeOwned,
    U::UR: Serialize + DeserializeOwned,
    U::IO: Serialize + DeserializeOwned,
    U::CO: Serialize + DeserializeOwned,
    U::CR: Serialize + DeserializeOwned,
{
    type Address = TcpAddress;
    type Sleep = tokio::time::Sleep;

    fn address(&self) -> TcpAddress {
        self.address
    }

    fn sleep(duration: Duration) -> Self::Sleep {
        tokio::time::sleep(duration)
    }

    fn persist<T: Serialize>(&self, key: &str, value: Option<&T>) {
        let dir = &self.inner.persist_dir;
        let path = format!("{dir}/{key}.bin");
        if let Some(val) = value {
            let data = bitcode::serialize(val).expect("serialize");
            std::fs::create_dir_all(dir).ok();
            std::fs::write(&path, data).expect("persist write");
        } else {
            std::fs::remove_file(&path).ok();
        }
    }

    fn persisted<T: DeserializeOwned>(&self, key: &str) -> Option<T> {
        let path = format!("{}/{key}.bin", self.inner.persist_dir);
        std::fs::read(&path)
            .ok()
            .and_then(|data| bitcode::deserialize(&data).ok())
    }

    fn send<R: TryFrom<IrMessage<U, Self>> + Send + Debug>(
        &self,
        address: TcpAddress,
        message: impl Into<IrMessage<U, Self>> + Debug,
    ) -> impl Future<Output = R> + Send + 'static {
        let message: TcpIrMessage<U> = message.into();
        let inner = Arc::clone(&self.inner);
        let from = self.address;

        async move {
            let mut delay = Duration::from_millis(100);
            let max_delay = Duration::from_secs(1);

            loop {
                let request_id = inner.next_request_id.fetch_add(1, Ordering::Relaxed);
                let (sender, receiver) = tokio::sync::oneshot::channel();
                inner
                    .pending_replies
                    .lock()
                    .unwrap()
                    .insert(request_id, sender);

                let wire = WireMessage::<U>::Request {
                    from,
                    request_id,
                    payload: message.clone(),
                };
                let frame = FrameCodec::encode(&wire).expect("encode");

                let tx = ensure_connection::<U>(&inner, address.socket_addr()).await;
                if tx.send(frame).await.is_err() {
                    inner
                        .pending_replies
                        .lock()
                        .unwrap()
                        .remove(&request_id);
                    tokio::time::sleep(delay).await;
                    delay = (delay * 2).min(max_delay);
                    continue;
                }

                tokio::select! {
                    result = receiver => {
                        match result {
                            Ok(reply) => {
                                return reply
                                    .try_into()
                                    .unwrap_or_else(|_| panic!("unexpected reply type"));
                            }
                            Err(_) => {
                                // Sender dropped (connection died), retry.
                                tokio::time::sleep(delay).await;
                                delay = (delay * 2).min(max_delay);
                                continue;
                            }
                        }
                    }
                    _ = tokio::time::sleep(Duration::from_secs(1)) => {
                        // Timeout, cleanup and retry.
                        inner.pending_replies.lock().unwrap().remove(&request_id);
                        delay = (delay * 2).min(max_delay);
                        continue;
                    }
                }
            }
        }
    }

    fn do_send(&self, address: TcpAddress, message: impl Into<IrMessage<U, Self>> + Debug) {
        let message: TcpIrMessage<U> = message.into();
        let inner = Arc::clone(&self.inner);
        let from = self.address;

        tokio::spawn(async move {
            let wire = WireMessage::<U>::FireAndForget {
                from,
                payload: message,
            };
            if let Ok(frame) = FrameCodec::encode(&wire) {
                let tx = ensure_connection::<U>(&inner, address.socket_addr()).await;
                let _ = tx.send(frame).await;
            }
        });
    }

    fn spawn(future: impl Future<Output = ()> + Send + 'static) {
        tokio::spawn(future);
    }

    fn on_membership_changed(&self, membership: &IrMembership<Self::Address>) {
        if let Some(shard) = *self.inner.shard.read().unwrap() {
            self.inner
                .shard_directory
                .write()
                .unwrap()
                .insert(shard, membership.clone());
        }
    }
}

impl<K: Key, V: Value> TapirTransport<K, V> for TcpTransport<TapirReplica<K, V>>
where
    <TapirReplica<K, V> as ReplicaUpcalls>::UO: Serialize + DeserializeOwned,
    <TapirReplica<K, V> as ReplicaUpcalls>::UR: Serialize + DeserializeOwned,
    <TapirReplica<K, V> as ReplicaUpcalls>::IO: Serialize + DeserializeOwned,
    <TapirReplica<K, V> as ReplicaUpcalls>::CO: Serialize + DeserializeOwned,
    <TapirReplica<K, V> as ReplicaUpcalls>::CR: Serialize + DeserializeOwned,
{
    fn shard_addresses(
        &self,
        shard: ShardNumber,
    ) -> impl Future<Output = IrMembership<TcpAddress>> + Send + 'static {
        let inner = Arc::clone(&self.inner);
        async move {
            loop {
                {
                    let dir = inner.shard_directory.read().unwrap();
                    if let Some(m) = dir.get(&shard) {
                        return m.clone();
                    }
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

impl<U: ReplicaUpcalls> TcpTransport<U>
where
    U::UO: Serialize + DeserializeOwned,
    U::UR: Serialize + DeserializeOwned,
    U::IO: Serialize + DeserializeOwned,
    U::CO: Serialize + DeserializeOwned,
    U::CR: Serialize + DeserializeOwned,
{
    /// Bind a TCP listener and start accepting inbound connections.
    pub async fn listen(&self, addr: std::net::SocketAddr) -> tokio::io::Result<()> {
        super::listener::listen(addr, Arc::clone(&self.inner)).await
    }
}
