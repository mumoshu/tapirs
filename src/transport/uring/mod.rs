//! io_uring-based thread-per-core transport for TAPIR.
//!
//! Gated behind `#[cfg(all(target_os = "linux", feature = "io-uring"))]`.

mod address;
mod codec;
mod conn_pool;
mod disk_io;
mod error;
pub(crate) mod reactor;
mod task;
mod tcp;
mod timer;

pub use address::UringAddress;
pub use error::UringError;
pub use timer::UringSleep;
