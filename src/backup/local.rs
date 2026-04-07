use super::storage::BackupStorage;

/// Local filesystem backup storage backend.
///
/// Wraps `std::fs` calls in async fn signatures (completes synchronously).
/// Each instance is scoped to a base directory path.
pub struct LocalBackupStorage {
    base_path: String,
}

impl LocalBackupStorage {
    pub fn new(base_path: &str) -> Self {
        Self {
            base_path: base_path.to_string(),
        }
    }

    fn path(&self, name: &str) -> String {
        format!("{}/{name}", self.base_path)
    }
}

impl BackupStorage for LocalBackupStorage {
    async fn init(&self) -> Result<(), String> {
        std::fs::create_dir_all(&self.base_path).map_err(|e| format!("create dir: {e}"))
    }

    async fn exists(&self, name: &str) -> Result<bool, String> {
        Ok(std::path::Path::new(&self.path(name)).exists())
    }

    async fn size(&self, name: &str) -> Result<Option<u64>, String> {
        match std::fs::metadata(self.path(name)) {
            Ok(m) => Ok(Some(m.len())),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(format!("stat {name}: {e}")),
        }
    }

    async fn read(&self, name: &str) -> Result<Vec<u8>, String> {
        std::fs::read(self.path(name)).map_err(|e| format!("read {name}: {e}"))
    }

    async fn write(&self, name: &str, data: &[u8]) -> Result<(), String> {
        std::fs::write(self.path(name), data).map_err(|e| format!("write {name}: {e}"))
    }

    async fn create_if_absent(&self, name: &str, data: &[u8]) -> Result<bool, String> {
        use std::io::Write;
        let path = self.path(name);
        match std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
        {
            Ok(mut f) => {
                f.write_all(data).map_err(|e| format!("write {name}: {e}"))?;
                Ok(true)
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Ok(false),
            Err(e) => Err(format!("create {name}: {e}")),
        }
    }

    async fn write_if_larger(&self, name: &str, data: &[u8]) -> Result<bool, String> {
        let path = self.path(name);
        match std::fs::metadata(&path) {
            Ok(m) if data.len() as u64 <= m.len() => Ok(false),
            Ok(_) | Err(_) => {
                std::fs::write(&path, data).map_err(|e| format!("write {name}: {e}"))?;
                Ok(true)
            }
        }
    }

    async fn list_files(&self, prefix: &str) -> Result<Vec<String>, String> {
        let entries = match std::fs::read_dir(&self.base_path) {
            Ok(e) => e,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(format!("read dir: {e}")),
        };
        let mut files = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|e| format!("read entry: {e}"))?;
            if entry.file_type().is_ok_and(|ft| ft.is_file())
                && let Some(name) = entry.file_name().to_str()
                && name.starts_with(prefix)
            {
                files.push(name.to_string());
            }
        }
        files.sort();
        Ok(files)
    }

    async fn list_subdirs(&self) -> Result<Vec<String>, String> {
        let entries =
            std::fs::read_dir(&self.base_path).map_err(|e| format!("read dir: {e}"))?;
        let mut dirs = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|e| format!("read entry: {e}"))?;
            if entry.file_type().is_ok_and(|ft| ft.is_dir())
                && let Some(name) = entry.file_name().to_str()
            {
                dirs.push(name.to_string());
            }
        }
        Ok(dirs)
    }

    fn sub(&self, name: &str) -> Self {
        if name.is_empty() {
            Self::new(&self.base_path)
        } else {
            Self::new(&self.path(name))
        }
    }

    fn display_path(&self) -> String {
        self.base_path.clone()
    }
}
