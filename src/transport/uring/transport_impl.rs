use super::address::UringAddress;
use super::codec::FrameCodec;
use super::conn_pool::ConnectionPool;
use super::reactor::with_reactor;
use super::timer::UringSleep;
use super::transport::{PendingReply, TransportState, UringTransport};
use super::wire::{UringIrMessage, WireMessage};
use crate::ir::ReplicaUpcalls;
use crate::{IrMembership, IrMessage, ShardNumber};
use crate::transport::{TapirTransport, Transport};
use crate::tapir::{Key, Value};
use crate::TapirReplica;
use serde::{Serialize, de::DeserializeOwned};
use std::cell::RefCell;
use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};
use std::time::Duration;

impl<U: ReplicaUpcalls> Transport<U> for UringTransport<U>
where
    U::UO: Serialize + DeserializeOwned,
    U::UR: Serialize + DeserializeOwned,
    U::IO: Serialize + DeserializeOwned,
    U::IR: Serialize + DeserializeOwned,
    U::CO: Serialize + DeserializeOwned,
    U::CR: Serialize + DeserializeOwned,
    U::Payload: Serialize + DeserializeOwned,
{
    type Address = UringAddress;
    type Sleep = UringSleep;

    fn address(&self) -> UringAddress {
        self.assert_thread();
        self.address
    }

    fn sleep(duration: Duration) -> UringSleep {
        UringSleep::new(duration)
    }

    fn send<R: TryFrom<IrMessage<U, Self>> + Send + Debug>(
        &self,
        address: UringAddress,
        message: impl Into<IrMessage<U, Self>> + Debug,
    ) -> impl Future<Output = R> + Send + 'static {
        self.assert_thread();
        let message: UringIrMessage<U> = message.into();
        let state = Rc::clone(&self.state);
        let from = self.address;

        let request_id = {
            let mut s = state.borrow_mut();
            let id = s.next_request_id;
            s.next_request_id += 1;
            s.pending_replies.insert(id, PendingReply {
                result: None,
                waker: None,
            });
            id
        };

        let wire = WireMessage::<U>::Request {
            from,
            request_id,
            payload: message.clone(),
        };
        let frame = match FrameCodec::encode(&wire) {
            Ok(f) => f,
            Err(e) => {
                // Serialization failed - remove pending reply and panic
                eprintln!("encode error: {e}");
                let mut s = state.borrow_mut();
                s.pending_replies.remove(&request_id);
                panic!("serialization failed: {e}");
            }
        };
        let should_spawn = {
            let mut s = state.borrow_mut();
            send_frame(&mut s, address, frame).expect("write queue full (backpressure)")
        };
        if should_spawn {
            let t = self.clone();
            with_reactor(|r| {
                r.executor.spawn(super::transport::connect_and_write(address.socket_addr(), t));
            });
        }

        // SAFETY: Future contains Rc but is only used on the reactor
        // thread. See UringTransport Send impl.
        UnsafeSendFuture(async move {
            std::future::poll_fn(move |cx| {
                let mut s = state.borrow_mut();
                if let Some(pending) = s.pending_replies.get_mut(&request_id) {
                    if let Some(msg) = pending.result.take() {
                        s.pending_replies.remove(&request_id);
                        let result: R = msg.try_into().unwrap_or_else(|_| {
                            panic!("unexpected reply type")
                        });
                        Poll::Ready(result)
                    } else {
                        pending.waker = Some(cx.waker().clone());
                        Poll::Pending
                    }
                } else {
                    Poll::Pending
                }
            })
            .await
        })
    }

    fn do_send(
        &self,
        address: UringAddress,
        message: impl Into<IrMessage<U, Self>> + Debug,
    ) {
        self.assert_thread();
        let message: UringIrMessage<U> = message.into();
        let wire = WireMessage::<U>::FireAndForget {
            from: self.address,
            payload: message,
        };
        let frame = match FrameCodec::encode(&wire) {
            Ok(f) => f,
            Err(e) => {
                eprintln!("encode error in do_send: {e}");
                return; // Drop message silently on encode error
            }
        };
        let should_spawn = {
            let mut state = self.state.borrow_mut();
            match send_frame(&mut state, address, frame) {
                Ok(spawn) => spawn,
                Err(()) => {
                    eprintln!("write queue full for {address:?}, dropping message");
                    return; // Drop message on backpressure
                }
            }
        };
        if should_spawn {
            let t = self.clone();
            with_reactor(|r| {
                r.executor.spawn(super::transport::connect_and_write(address.socket_addr(), t));
            });
        }
    }

    fn spawn(future: impl Future<Output = ()> + Send + 'static) {
        with_reactor(|r| r.executor.spawn(future));
    }
}

fn send_frame<U: ReplicaUpcalls>(
    state: &mut TransportState<U>,
    address: UringAddress,
    frame: Vec<u8>,
) -> Result<bool, ()> {
    let addr = address.socket_addr();
    if state.conn_pool.is_connected(&addr) {
        if let Some(conn) = state.conn_pool.get_mut(&addr) {
            conn.try_enqueue_frame(frame)?; // Propagate backpressure error
        }
        Ok(false)
    } else if state.conn_pool.is_connecting(&addr) {
        // Check connecting queue size too
        if let Some(queue) = state.conn_pool.connecting.get(&addr) {
            if queue.len() >= 1000 {
                return Err(()); // Connecting queue full
            }
        }
        state.conn_pool.queue_while_connecting(addr, frame);
        Ok(false)  // Already connecting, don't spawn duplicate
    } else {
        state.conn_pool.start_connecting(addr);
        state.conn_pool.queue_while_connecting(addr, frame);
        Ok(true)  // New connection, spawn connect_and_write
    }
}

/// Wrapper that implements Send for futures containing Rc.
/// SAFETY: Only used on the reactor thread (thread-per-core guarantee).
struct UnsafeSendFuture<F>(F);
unsafe impl<F> Send for UnsafeSendFuture<F> {}

impl<F: Future> Future for UnsafeSendFuture<F> {
    type Output = F::Output;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
        // SAFETY: We only project to the inner future, preserving pin.
        let inner = unsafe { self.map_unchecked_mut(|s| &mut s.0) };
        inner.poll(cx)
    }
}

impl<K: Key, V: Value> TapirTransport<K, V> for UringTransport<TapirReplica<K, V>>
where
    <TapirReplica<K, V> as ReplicaUpcalls>::UO: Serialize + DeserializeOwned,
    <TapirReplica<K, V> as ReplicaUpcalls>::UR: Serialize + DeserializeOwned,
    <TapirReplica<K, V> as ReplicaUpcalls>::IO: Serialize + DeserializeOwned,
    <TapirReplica<K, V> as ReplicaUpcalls>::IR: Serialize + DeserializeOwned,
    <TapirReplica<K, V> as ReplicaUpcalls>::CO: Serialize + DeserializeOwned,
    <TapirReplica<K, V> as ReplicaUpcalls>::CR: Serialize + DeserializeOwned,
{
    fn shard_addresses(
        &self,
        shard: ShardNumber,
    ) -> impl Future<Output = IrMembership<UringAddress>> + Send + 'static {
        let state = Rc::clone(&self.state);
        UnsafeSendFuture(async move {
            loop {
                {
                    let s = state.borrow();
                    if let Some(m) = s.shard_directory.get(&shard) {
                        return m.clone();
                    }
                }
                UringSleep::new(Duration::from_millis(100)).await;
            }
        })
    }
}
