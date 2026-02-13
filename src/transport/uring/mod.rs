//! io_uring-based thread-per-core transport for TAPIR.
//!
//! Gated behind `#[cfg(all(target_os = "linux", feature = "io-uring"))]`.

mod address;
mod codec;
mod error;

pub use address::UringAddress;
pub use error::UringError;
