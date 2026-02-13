//! io_uring-based thread-per-core transport for TAPIR.
//!
//! Gated behind `#[cfg(all(target_os = "linux", feature = "io-uring"))]`.

mod address;
mod codec;
mod conn_pool;
mod disk_io;
mod error;
mod launcher;
pub(crate) mod reactor;
mod task;
mod tcp;
mod timer;
pub(crate) mod transport;
mod transport_impl;
mod wire;

pub use address::UringAddress;
pub use disk_io::UringDirectIo;
pub use error::UringError;
pub use launcher::{CoreConfig, CoreLauncher, ShardAssignment};
pub use timer::UringSleep;
pub use transport::UringTransport;
