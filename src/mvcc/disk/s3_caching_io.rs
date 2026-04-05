//! Lazy-download DiskIo that fetches segments from S3 on first access.
//!
//! When `open()` is called for a file that doesn't exist locally, the file
//! is downloaded from S3 to the local cache directory, then opened via
//! `BufferedIo`. Subsequent `pread()` calls hit the local file. Since sealed
//! VlogLsm segments are immutable, the cache never invalidates.
//!
//! The S3 download config (endpoint, bucket, shard prefix) is stored in a
//! process-global registry keyed by base directory. `register_s3_cache()`
//! must be called before opening files from that directory.

use std::collections::BTreeMap;
use std::future::{Future, Ready, ready};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

use super::aligned_buf::AlignedBuf;
use super::disk_io::{BufferedIo, DiskIo, OpenFlags};
use super::error::StorageError;

/// S3 download configuration for a base directory.
#[derive(Clone)]
pub struct S3CacheConfig {
    pub endpoint: String,
    pub bucket: String,
    /// S3 key prefix for this shard's segments (e.g. "shard_0/segments/").
    pub shard_segments_prefix: String,
}

static REGISTRY: OnceLock<Mutex<BTreeMap<PathBuf, S3CacheConfig>>> = OnceLock::new();

fn registry() -> &'static Mutex<BTreeMap<PathBuf, S3CacheConfig>> {
    REGISTRY.get_or_init(|| Mutex::new(BTreeMap::new()))
}

/// Register S3 config for a base directory. Called by CombinedStore open paths
/// that use S3-backed storage (open_from_remote, clone_from_remote).
pub fn register_s3_cache(base_dir: &Path, config: S3CacheConfig) {
    registry()
        .lock()
        .unwrap()
        .insert(base_dir.to_path_buf(), config);
}

/// Remove S3 config for a base directory (cleanup).
pub fn unregister_s3_cache(base_dir: &Path) {
    registry().lock().unwrap().remove(base_dir);
}

fn lookup_config(path: &Path) -> Option<S3CacheConfig> {
    let dir = path.parent()?;
    registry().lock().unwrap().get(dir).cloned()
}

/// Download a single file from S3 to a local path.
///
/// Uses a temporary tokio runtime since DiskIo::open() is synchronous.
/// This is acceptable because segment downloads happen once per segment.
fn download_from_s3_blocking(
    config: &S3CacheConfig,
    segment_name: &str,
    local_path: &Path,
) -> Result<(), StorageError> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| StorageError::Io(std::io::Error::other(format!("tokio runtime: {e}"))))?;

    rt.block_on(async {
        let sdk_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(aws_config::Region::new("us-east-1"))
            .endpoint_url(&config.endpoint)
            .load()
            .await;
        let s3_config = aws_sdk_s3::config::Builder::from(&sdk_config)
            .force_path_style(true)
            .build();
        let client = aws_sdk_s3::Client::from_conf(s3_config);

        let key = format!("{}{segment_name}", config.shard_segments_prefix);
        let resp = client
            .get_object()
            .bucket(&config.bucket)
            .key(&key)
            .send()
            .await
            .map_err(|e| {
                StorageError::Io(std::io::Error::other(format!(
                    "S3 GetObject {key}: {e}"
                )))
            })?;

        let bytes = resp.body.collect().await.map_err(|e| {
            StorageError::Io(std::io::Error::other(format!(
                "S3 read body {key}: {e}"
            )))
        })?;

        std::fs::write(local_path, bytes.into_bytes()).map_err(StorageError::Io)
    })
}

/// DiskIo implementation that lazily downloads segments from S3 on first open.
///
/// If a file exists locally, opens it directly via BufferedIo (zero S3 cost).
/// If a file doesn't exist locally but S3 config is registered for its
/// directory, downloads the file from S3 first, then opens via BufferedIo.
/// If no S3 config is registered, falls back to normal BufferedIo open
/// (which will fail with "not found" if the file doesn't exist).
#[derive(Clone)]
pub struct S3CachingIo {
    inner: BufferedIo,
}

impl DiskIo for S3CachingIo {
    type ReadFuture = Ready<Result<(), StorageError>>;
    type WriteFuture = Ready<Result<(), StorageError>>;

    fn open(path: &Path, flags: OpenFlags) -> Result<Self, StorageError> {
        if !path.exists() {
            if let Some(config) = lookup_config(path) {
                if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                    download_from_s3_blocking(&config, name, path)?;
                }
            }
        }
        Ok(Self {
            inner: BufferedIo::open(path, flags)?,
        })
    }

    fn pread(&self, buf: &mut AlignedBuf, offset: u64) -> Self::ReadFuture {
        self.inner.pread(buf, offset)
    }

    fn pwrite(&self, buf: &AlignedBuf, offset: u64) -> Self::WriteFuture {
        self.inner.pwrite(buf, offset)
    }

    fn fsync(&self) -> impl Future<Output = Result<(), StorageError>> + Send {
        self.inner.fsync()
    }

    fn close(self) {
        self.inner.close();
    }

    fn file_len(&self) -> Result<u64, StorageError> {
        self.inner.file_len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_file_opens_without_s3() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.dat");
        std::fs::write(&path, b"hello").unwrap();

        let io = S3CachingIo::open(&path, OpenFlags { create: false, direct: false }).unwrap();
        let mut buf = AlignedBuf::new(4096);
        futures::executor::block_on(io.pread(&mut buf, 0)).unwrap();
        assert_eq!(&buf.as_slice()[..5], b"hello");
    }

    #[test]
    fn missing_file_without_config_fails() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nonexistent.dat");

        let result = S3CachingIo::open(&path, OpenFlags { create: false, direct: false });
        assert!(result.is_err());
    }
}
