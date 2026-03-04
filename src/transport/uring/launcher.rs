use super::address::UringAddress;
use super::reactor;
use super::transport::UringTransport;
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
    /// Connection timeout (milliseconds). Default: 5000ms.
    pub connect_timeout_ms: u64,
    /// Request timeout (milliseconds). Default: 30000ms (not yet implemented).
    pub request_timeout_ms: u64,
}

impl Default for CoreConfig {
    fn default() -> Self {
        Self {
            cpu_id: 0,
            shards: Vec::new(),
            ring_size: 256,
            connect_timeout_ms: 5000,
            request_timeout_ms: 30000,
        }
    }
}

/// Launches reactor threads, one per core.
pub struct CoreLauncher {
    configs: Vec<CoreConfig>,
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
        U::IR: Serialize + DeserializeOwned,
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
                        let transport = UringTransport::<U>::new(addr);

                        {
                            let mut state = transport.state.borrow_mut();
                            state.shard_directory.insert(
                                shard_config.shard,
                                shard_config.membership.clone(),
                            );
                        }

                        transport.listen(shard_config.listen_addr);
                    }

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
