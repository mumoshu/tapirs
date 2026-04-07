/// Pluggable storage backend for backup/restore operations.
///
/// Each instance is scoped to a base directory or prefix. File names
/// passed to methods are relative to that scope.
///
/// Uses `async fn` in trait — safe because all callers use generics
/// (not `dyn BackupStorage`), so the compiler monomorphizes per backend.
#[allow(async_fn_in_trait)]
pub trait BackupStorage: Sized + Send + Sync {
    /// Ensure storage location exists (create dirs for local, no-op for S3).
    async fn init(&self) -> Result<(), String>;
    /// Check if a file exists.
    async fn exists(&self, name: &str) -> Result<bool, String>;
    /// Get file size in bytes. Returns None if the file does not exist.
    async fn size(&self, name: &str) -> Result<Option<u64>, String>;
    /// Read file as bytes.
    async fn read(&self, name: &str) -> Result<Vec<u8>, String>;
    /// Write bytes to a file (unconditional overwrite).
    async fn write(&self, name: &str, data: &[u8]) -> Result<(), String>;
    /// Atomically create a file only if it does not already exist.
    ///
    /// Returns `Ok(true)` if created, `Ok(false)` if the file already exists.
    /// S3: PutObject with `If-None-Match: *`. Local: O_EXCL.
    ///
    /// Used for newly sealed segments — the first replica to upload wins.
    async fn create_if_absent(&self, name: &str, data: &[u8]) -> Result<bool, String>;
    /// Atomically write data only if it is larger than the current remote copy.
    ///
    /// Returns `Ok(true)` if written, `Ok(false)` if skipped (remote is same
    /// size or larger). Uses compare-and-swap internally to prevent races
    /// between concurrent writers (e.g., multiple replicas uploading the same
    /// active segment).
    ///
    /// Active segments grow by append, so "larger" means "more up-to-date".
    /// A replica with less data must not overwrite a more complete copy.
    /// S3: HEAD for ETag+size, PutObject with `If-Match: <etag>`, CAS retry.
    /// Local: compare file size, write if larger.
    async fn write_if_larger(&self, name: &str, data: &[u8]) -> Result<bool, String>;
    /// List immediate subdirectories (for list_backups scanning).
    async fn list_subdirs(&self) -> Result<Vec<String>, String>;
    /// List files matching a name prefix, sorted ascending.
    ///
    /// Returns relative file names (not full paths/keys).
    /// S3: ListObjectsV2 with prefix, pages through all results.
    /// Local: read_dir + filter by prefix.
    async fn list_files(&self, prefix: &str) -> Result<Vec<String>, String>;
    /// List the last `limit` files matching a name prefix (descending order).
    ///
    /// Equivalent to `list_files(prefix)` sorted descending, truncated to `limit`.
    /// Used for O(1)-ish "latest version" lookups.
    ///
    /// Current S3 impl lists all matching files and takes the last N — O(n)
    /// where n is the total number of matching files. A future implementation
    /// could use a dedicated metadata store for O(1) reverse lookups.
    async fn list_files_reverse(&self, prefix: &str, limit: usize) -> Result<Vec<String>, String> {
        let mut files = self.list_files(prefix).await?;
        files.reverse();
        files.truncate(limit);
        Ok(files)
    }
    /// Create a sub-storage scoped to a child directory/prefix.
    fn sub(&self, name: &str) -> Self;
    /// Human-readable display path (e.g. "/tmp/backup" or "s3://bucket/prefix/").
    fn display_path(&self) -> String;

    /// Read file as UTF-8 string. Default impl delegates to read().
    async fn read_string(&self, name: &str) -> Result<String, String> {
        let bytes = self.read(name).await?;
        String::from_utf8(bytes).map_err(|e| format!("invalid UTF-8 in {name}: {e}"))
    }
}
