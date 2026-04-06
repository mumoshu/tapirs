//! Read replica backed by S3CachingIo with support for refresh.
//!
//! A read replica opens from a remote manifest (S3), lazily downloads
//! segments on first read, and can be refreshed to pick up newer data
//! uploaded by the source shard.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use arc_swap::ArcSwap;

use crate::backup::storage::BackupStorage;
use crate::mvcc::disk::disk_io::OpenFlags;
use crate::mvcc::disk::s3_caching_io::S3CachingIo;
use crate::tapir::store::TapirStore;
use crate::tapir::{ShardNumber, Timestamp};
use crate::unified::combined::tapir_handle::CombinedTapirHandle;
use crate::unified::combined::CombinedStoreInner;

use super::config::S3StorageConfig;
use super::manifest_store::RemoteManifestStore;
use super::open_remote::prepare_local_lazy;

type TapirHandle = CombinedTapirHandle<String, String, S3CachingIo>;

/// A refreshable read replica opened from S3.
pub struct ReadReplica {
    handle: ArcSwap<TapirHandle>,
    base_dir: PathBuf,
    s3_config: S3StorageConfig,
    shard: ShardNumber,
    shard_name: String,
}

impl ReadReplica {
    /// Open a read replica from the latest manifest on S3.
    pub async fn open<S: BackupStorage>(
        manifest_store: &RemoteManifestStore<S>,
        s3_config: S3StorageConfig,
        shard: ShardNumber,
        base_dir: &Path,
    ) -> Result<Self, String> {
        let shard_name = format!("shard_{}", shard.0);
        prepare_local_lazy(manifest_store, &s3_config, &shard_name, base_dir).await?;

        let inner = CombinedStoreInner::<String, String, S3CachingIo>::open(
            base_dir,
            OpenFlags { create: true, direct: false },
            shard,
            true,
        )
        .map_err(|e| format!("open read replica: {e}"))?;

        let record = inner.into_record_handle();
        let tapir = record.tapir_handle();

        Ok(Self {
            handle: ArcSwap::new(Arc::new(tapir)),
            base_dir: base_dir.to_path_buf(),
            s3_config,
            shard,
            shard_name,
        })
    }

    /// Current manifest view number.
    pub fn current_view(&self) -> u64 {
        let h = self.handle.load();
        h.inner.lock().unwrap().tapir_manifest.current_view
    }

    /// Load the current tapir handle for direct read access.
    pub fn load_handle(&self) -> arc_swap::Guard<Arc<TapirHandle>> {
        self.handle.load()
    }

    /// Read a key at a given timestamp.
    pub fn get_at(&self, key: &String, ts: Timestamp) -> Option<(Option<String>, Timestamp)> {
        let h = self.handle.load();
        h.do_uncommitted_get_at(key, ts).ok()
    }

    /// Swap the internal store with a new one opened from an updated manifest.
    pub(crate) fn swap_handle(&self, new_handle: TapirHandle) {
        self.handle.store(Arc::new(new_handle));
    }

    pub fn base_dir(&self) -> &Path {
        &self.base_dir
    }

    pub fn s3_config(&self) -> &S3StorageConfig {
        &self.s3_config
    }

    pub fn shard(&self) -> ShardNumber {
        self.shard
    }

    pub fn shard_name(&self) -> &str {
        &self.shard_name
    }
}
