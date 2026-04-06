//! # Tokio Bitcode TCP Transport
//!
//! A TCP transport for IR/TAPIR using the Tokio async runtime and bitcode
//! binary encoding with length-prefixed framing.
//!
//! ## Design
//!
//! - **Runtime**: Tokio (epoll-based async reactor on Linux)
//! - **Codec**: Bitcode binary encoding (~10x faster than JSON)
//! - **Framing**: 4-byte LE u32 length prefix + bitcode payload (max 16 MB)
//! - **Request-reply**: oneshot channels for pending replies (not Waker-based
//!   like the uring transport)
//! - **Connection model**: `TcpStream::into_split()` for full-duplex — read and
//!   write halves run as independent async tasks on separate Tokio tasks
//! - **Inbound dispatch**: The receive callback (`IrReplica::receive()`) is
//!   synchronous, so inbound messages are dispatched inline in the read loop
//!   without async overhead
//!
//! ## Efficiency
//!
//! - **Non-blocking I/O**: All network I/O via Tokio's epoll reactor — can
//!   saturate bandwidth without blocking threads
//! - **Full-duplex TCP**: Read/write halves run concurrently as separate tasks
//! - **Request multiplexing**: Multiple in-flight requests share a single TCP
//!   connection via `request_id` matching — no head-of-line blocking
//! - **Write coalescing**: The write loop drains the entire mpsc channel and
//!   concatenates frames into a single `write_all`, reducing syscall count
//!   under load
//! - **Connection pooling**: One persistent connection per peer, reused across
//!   all requests
//! - **Bitcode encoding**: Fast binary codec with minimal serialization overhead
//!
//! The performance bottleneck in TAPIR is consensus round-trips (2 RTTs for
//! prepare + commit across f+1 replicas), not transport encoding or syscall
//! overhead.
//!
//! ## vs uring transport
//!
//! This transport trades the uring transport's thread-per-core completion-based
//! I/O (io_uring, Rc/RefCell, CPU-pinned threads) for Tokio compatibility —
//! needed for the admin API, client REPL, and interactive use. For maximum
//! throughput, the `uring` transport remains available.
//!
//! ## Wire compatibility
//!
//! Both transports use length-prefixed bitcode encoding over TCP. Since
//! `TcpAddress` and `UringAddress` are both newtypes around `SocketAddr`,
//! and bitcode serializes through newtypes transparently, the on-the-wire
//! bytes are identical — making the two transports wire-compatible.

mod address;
mod codec;
mod connection;
mod listener;
mod state;
mod transport_impl;
pub mod wire;

pub use address::TcpAddress;
pub use state::TcpTransport;
