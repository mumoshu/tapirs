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
    /// Read file as bytes.
    async fn read(&self, name: &str) -> Result<Vec<u8>, String>;
    /// Write bytes to a file.
    async fn write(&self, name: &str, data: &[u8]) -> Result<(), String>;
    /// List immediate subdirectories (for list_backups scanning).
    async fn list_subdirs(&self) -> Result<Vec<String>, String>;
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
