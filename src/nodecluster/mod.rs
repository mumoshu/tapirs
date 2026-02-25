//! Solo cluster operations — work via direct node access without depending
//! on a discovery store or ShardManager.
//!
//! These operations communicate directly with node admin APIs to discover
//! shard membership and create replicas. No centralized metadata service
//! is required. This makes them suitable for standalone single-shard
//! clusters (e.g. the discovery store itself) where no ShardManager exists.

mod backup_cluster;
mod clone_shard;
mod restore_cluster;
mod types;

pub use types::{CloneError, SoloClusterManager};
