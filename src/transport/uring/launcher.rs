use super::address::UringAddress;
use super::codec::{FrameCodec, FrameReader};
use super::reactor;
use super::tcp::{TcpListener, TcpStream};
use super::transport::UringTransport;
use super::wire::WireMessage;
use crate::ir::ReplicaUpcalls;
use crate::{IrMembership, ShardNumber};
use serde::{Serialize, de::DeserializeOwned};
use std::net::SocketAddr;

/// Assignment of a shard to a core thread.
pub struct ShardAssignment {
    pub shard: ShardNumber,
    pub listen_addr: SocketAddr,
    pub membership: IrMembership<UringAddress>,
}

/// Configuration for a single core thread.
pub struct CoreConfig {
    pub cpu_id: usize,
    pub shards: Vec<ShardAssignment>,
    pub ring_size: u32,
    pub persist_dir: String,
}

/// Launches reactor threads, one per core.
pub struct CoreLauncher {
    pub configs: Vec<CoreConfig>,
}

impl CoreLauncher {
    pub fn new(configs: Vec<CoreConfig>) -> Self {
        Self { configs }
    }

    /// Launch all core threads. Each thread runs its reactor forever.
    /// Returns join handles for the spawned threads.
    pub fn launch<U>(self) -> Vec<std::thread::JoinHandle<()>>
    where
        U: ReplicaUpcalls,
        U::UO: Serialize + DeserializeOwned,
        U::UR: Serialize + DeserializeOwned,
        U::IO: Serialize + DeserializeOwned,
        U::CO: Serialize + DeserializeOwned,
        U::CR: Serialize + DeserializeOwned,
    {
        self.configs
            .into_iter()
            .map(|config| {
                std::thread::spawn(move || {
                    pin_to_cpu(config.cpu_id);
                    reactor::init_reactor(config.ring_size);

                    for shard_config in &config.shards {
                        let addr = UringAddress::from(shard_config.listen_addr);
                        let transport = UringTransport::<U>::new(
                            addr,
                            config.persist_dir.clone(),
                        );

                        // Store shard directory.
                        {
                            let mut state = transport.state.borrow_mut();
                            state.shard_directory.insert(
                                shard_config.shard,
                                shard_config.membership.clone(),
                            );
                        }

                        // Bind TCP listener.
                        let listener = TcpListener::bind(shard_config.listen_addr)
                            .expect("bind failed");

                        // Spawn accept loop.
                        let t = transport.clone();
                        reactor::with_reactor(|r| {
                            r.executor.spawn(accept_loop(listener, t));
                        });
                    }

                    // Run reactor forever.
                    reactor::with_reactor(|r| r.run());
                })
            })
            .collect()
    }
}

/// Pin the current thread to a specific CPU core.
fn pin_to_cpu(cpu_id: usize) {
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_SET(cpu_id, &mut set);
        let ret = libc::sched_setaffinity(0, std::mem::size_of_val(&set), &set);
        if ret != 0 {
            eprintln!("warning: sched_setaffinity failed for cpu {cpu_id}");
        }
    }
}

/// Accept loop: accepts connections and spawns per-connection read loops.
async fn accept_loop<U: ReplicaUpcalls>(
    listener: TcpListener,
    transport: UringTransport<U>,
)
where
    U::UO: Serialize + DeserializeOwned,
    U::UR: Serialize + DeserializeOwned,
    U::IO: Serialize + DeserializeOwned,
    U::CO: Serialize + DeserializeOwned,
    U::CR: Serialize + DeserializeOwned,
{
    loop {
        match listener.accept().await {
            Ok((stream, _peer)) => {
                let t = transport.clone();
                reactor::with_reactor(|r| {
                    r.executor.spawn(read_loop(stream, t));
                });
            }
            Err(e) => {
                eprintln!("accept error: {e}");
            }
        }
    }
}

/// Per-connection read loop: reads frames and dispatches messages.
async fn read_loop<U: ReplicaUpcalls>(
    stream: TcpStream,
    transport: UringTransport<U>,
)
where
    U::UO: Serialize + DeserializeOwned,
    U::UR: Serialize + DeserializeOwned,
    U::IO: Serialize + DeserializeOwned,
    U::CO: Serialize + DeserializeOwned,
    U::CR: Serialize + DeserializeOwned,
{
    let mut reader = FrameReader::new();
    loop {
        let buf = reader.recv_buf();
        let n = match stream.recv(buf).await {
            Ok(0) => break, // Connection closed.
            Ok(n) => n,
            Err(_) => break,
        };
        reader.advance(n);

        while let Ok(Some(payload)) = reader.try_read_frame() {
            let wire: WireMessage<U> = match FrameCodec::decode(&payload) {
                Ok(m) => m,
                Err(_) => continue,
            };
            if let Some(reply_frame) = dispatch_wire_message(&transport, wire) {
                if stream.send(&reply_frame).await.is_err() {
                    return; // Connection broken.
                }
            }
        }
    }
}

/// Dispatch an incoming wire message. Returns a reply frame to send, if any.
fn dispatch_wire_message<U: ReplicaUpcalls>(
    transport: &UringTransport<U>,
    wire: WireMessage<U>,
) -> Option<Vec<u8>>
where
    U::UO: Serialize + DeserializeOwned,
    U::UR: Serialize + DeserializeOwned,
    U::IO: Serialize + DeserializeOwned,
    U::CO: Serialize + DeserializeOwned,
    U::CR: Serialize + DeserializeOwned,
{
    let state = transport.state.borrow();
    match wire {
        WireMessage::Request {
            from,
            request_id,
            payload,
        } => {
            if let Some(ref cb) = state.receive_callback {
                if let Some(reply) = cb(from, payload) {
                    let reply_wire = WireMessage::<U>::Reply {
                        request_id,
                        payload: reply,
                    };
                    return Some(FrameCodec::encode(&reply_wire));
                }
            }
            None
        }
        WireMessage::Reply {
            request_id,
            payload,
        } => {
            drop(state);
            let mut state = transport.state.borrow_mut();
            if let Some(pending) = state.pending_replies.get_mut(&request_id) {
                pending.result = Some(payload);
                if let Some(waker) = pending.waker.take() {
                    waker.wake();
                }
            }
            None
        }
        WireMessage::FireAndForget { from, payload } => {
            if let Some(ref cb) = state.receive_callback {
                cb(from, payload);
            }
            None
        }
    }
}
