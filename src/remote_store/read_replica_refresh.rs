//! Refresh logic for read replicas backed by S3.
//!
//! Polls S3 for newer manifest versions and reopens the store when
//! a new version is found. Sealed segments are lazy-downloaded via
//! S3CachingIo, so refresh only downloads the manifest itself.

use std::time::Duration;

use crate::backup::s3backup::S3BackupStorage;
use crate::mvcc::disk::disk_io::OpenFlags;
use crate::mvcc::disk::s3_caching_io::S3CachingIo;
use crate::unified::combined::CombinedStoreInner;

use super::manifest_store::RemoteManifestStore;
use super::open_remote::prepare_local_lazy;
use super::read_replica::ReadReplica;

/// Attempt a single refresh cycle: check S3 for a newer manifest,
/// and if found, reopen the store from the new manifest.
///
/// Returns `Ok(true)` if the store was refreshed, `Ok(false)` if
/// already up to date, or `Err` on failure.
pub async fn refresh_once(
    replica: &ReadReplica,
    manifest_store: &RemoteManifestStore<S3BackupStorage>,
) -> Result<bool, String> {
    let current = replica.current_view();
    let versions = manifest_store
        .list_manifest_versions(replica.shard_name())
        .await?;
    let latest = versions.last().copied().unwrap_or(0);

    if latest <= current {
        return Ok(false);
    }

    // Download newer manifest and register S3 cache for lazy segments.
    prepare_local_lazy(
        manifest_store,
        replica.s3_config(),
        replica.shard_name(),
        replica.base_dir(),
    )
    .await?;

    // Reopen store from the updated manifest.
    let inner = CombinedStoreInner::<String, String, S3CachingIo>::open(
        replica.base_dir(),
        OpenFlags { create: true, direct: false },
        replica.shard(),
        true,
    )
    .map_err(|e| format!("reopen after refresh: {e}"))?;

    let record = inner.into_record_handle();
    let tapir = record.tapir_handle();
    replica.swap_handle(tapir);

    Ok(true)
}

/// Spawn a background task that periodically refreshes a read replica.
pub fn spawn_refresh_task(
    replica: &std::sync::Arc<ReadReplica>,
    manifest_store: std::sync::Arc<RemoteManifestStore<S3BackupStorage>>,
    interval: Duration,
) -> tokio::task::JoinHandle<()> {
    let replica = std::sync::Arc::clone(replica);
    tokio::task::spawn(async move {
        loop {
            tokio::time::sleep(interval).await;
            match refresh_once(&replica, &manifest_store).await {
                Ok(true) => {
                    tracing::debug!(shard = replica.shard_name(), "read replica refreshed");
                }
                Ok(false) => {}
                Err(e) => {
                    tracing::warn!(
                        shard = replica.shard_name(),
                        error = %e,
                        "read replica refresh failed"
                    );
                }
            }
        }
    })
}
