use super::aligned_buf::AlignedBuf;
use super::disk_io::{DiskIo, OpenFlags};
use super::error::StorageError;
use std::cell::RefCell;
use std::collections::{BTreeMap, HashSet};
use std::future::{Ready, ready};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// In-memory virtual filesystem shared by all `MemoryIo` handles on the same thread.
struct MemoryFsInner {
    files: BTreeMap<PathBuf, Vec<u8>>,
    dirs: HashSet<PathBuf>,
}

impl MemoryFsInner {
    fn new() -> Self {
        Self {
            files: BTreeMap::new(),
            dirs: HashSet::new(),
        }
    }
}

thread_local! {
    static MEMORY_FS: RefCell<Arc<Mutex<MemoryFsInner>>> = RefCell::new(
        Arc::new(Mutex::new(MemoryFsInner::new()))
    );
}

fn get_fs() -> Arc<Mutex<MemoryFsInner>> {
    MEMORY_FS.with(|fs| fs.borrow().clone())
}

/// In-memory `DiskIo` implementation for testing.
///
/// All file data lives in a thread-local virtual filesystem. Each test thread
/// gets its own isolated filesystem, and unique paths (via `temp_path()`)
/// prevent collision between tests on the same thread.
#[derive(Clone)]
pub struct MemoryIo {
    fs: Arc<Mutex<MemoryFsInner>>,
    path: PathBuf,
}

impl MemoryIo {
    /// Generate a unique in-memory path for a store directory.
    pub fn temp_path() -> PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        PathBuf::from(format!(
            "/memfs/store_{}",
            COUNTER.fetch_add(1, Ordering::Relaxed)
        ))
    }

    /// List all files under the given directory prefix, returning (path, size) pairs
    /// sorted by path. Only returns files whose path starts with `dir/`.
    pub fn list_files(dir: &Path) -> Vec<(PathBuf, usize)> {
        let fs = get_fs();
        let inner = fs.lock().unwrap();
        let prefix = format!("{}/", dir.display());
        let mut result: Vec<(PathBuf, usize)> = inner
            .files
            .iter()
            .filter(|(p, _)| p.to_string_lossy().starts_with(&prefix))
            .map(|(p, data)| (p.clone(), data.len()))
            .collect();
        result.sort_by(|a, b| a.0.cmp(&b.0));
        result
    }
}

impl DiskIo for MemoryIo {
    type ReadFuture = Ready<Result<(), StorageError>>;
    type WriteFuture = Ready<Result<(), StorageError>>;

    fn open(path: &Path, flags: OpenFlags) -> Result<Self, StorageError> {
        let fs = get_fs();
        {
            let mut inner = fs.lock().unwrap();
            if !inner.files.contains_key(path) {
                if flags.create {
                    inner.files.insert(path.to_path_buf(), Vec::new());
                } else {
                    return Err(StorageError::Io(std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        format!("file not found: {}", path.display()),
                    )));
                }
            }
        }
        Ok(Self {
            fs,
            path: path.to_path_buf(),
        })
    }

    fn pread(&self, buf: &mut AlignedBuf, offset: u64) -> Self::ReadFuture {
        let inner = self.fs.lock().unwrap();
        match inner.files.get(&self.path) {
            Some(data) => {
                let offset = offset as usize;
                let available = data.len().saturating_sub(offset);
                let to_read = available.min(buf.capacity());
                if to_read > 0 {
                    buf.as_full_slice_mut()[..to_read]
                        .copy_from_slice(&data[offset..offset + to_read]);
                }
                buf.set_len(to_read);
                ready(Ok(()))
            }
            None => ready(Err(StorageError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("file not found: {}", self.path.display()),
            )))),
        }
    }

    fn pwrite(&self, buf: &AlignedBuf, offset: u64) -> Self::WriteFuture {
        let mut inner = self.fs.lock().unwrap();
        match inner.files.get_mut(&self.path) {
            Some(data) => {
                let offset = offset as usize;
                let end = offset + buf.capacity();
                if data.len() < end {
                    data.resize(end, 0);
                }
                data[offset..end].copy_from_slice(buf.as_full_slice());
                ready(Ok(()))
            }
            None => ready(Err(StorageError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("file not found: {}", self.path.display()),
            )))),
        }
    }

    fn fsync(&self) -> impl std::future::Future<Output = Result<(), StorageError>> + Send {
        ready(Ok(()))
    }

    fn close(self) {
        // Data persists in the MemoryFs.
    }

    fn file_len(&self) -> Result<u64, StorageError> {
        let inner = self.fs.lock().unwrap();
        match inner.files.get(&self.path) {
            Some(data) => Ok(data.len() as u64),
            None => Err(StorageError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("file not found: {}", self.path.display()),
            ))),
        }
    }

    fn create_dir_all(path: &Path) -> Result<(), StorageError> {
        let fs = get_fs();
        let mut inner = fs.lock().unwrap();
        // Insert the path and all ancestors.
        let mut current = path.to_path_buf();
        loop {
            inner.dirs.insert(current.clone());
            match current.parent() {
                Some(parent) if parent != current => current = parent.to_path_buf(),
                _ => break,
            }
        }
        Ok(())
    }

    fn remove_file(path: &Path) -> Result<(), StorageError> {
        let fs = get_fs();
        let mut inner = fs.lock().unwrap();
        if inner.files.remove(path).is_some() {
            Ok(())
        } else {
            Err(StorageError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("file not found: {}", path.display()),
            )))
        }
    }

    fn exists(path: &Path) -> bool {
        let fs = get_fs();
        let inner = fs.lock().unwrap();
        inner.files.contains_key(path) || inner.dirs.contains(path)
    }

    fn read_file(path: &Path) -> Result<Vec<u8>, StorageError> {
        let fs = get_fs();
        let inner = fs.lock().unwrap();
        match inner.files.get(path) {
            Some(data) => Ok(data.clone()),
            None => Err(StorageError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("file not found: {}", path.display()),
            ))),
        }
    }

    fn write_file(path: &Path, data: &[u8]) -> Result<(), StorageError> {
        let fs = get_fs();
        let mut inner = fs.lock().unwrap();
        inner.files.insert(path.to_path_buf(), data.to_vec());
        Ok(())
    }

    fn rename(from: &Path, to: &Path) -> Result<(), StorageError> {
        let fs = get_fs();
        let mut inner = fs.lock().unwrap();
        match inner.files.remove(from) {
            Some(data) => {
                inner.files.insert(to.to_path_buf(), data);
                Ok(())
            }
            None => Err(StorageError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("file not found: {}", from.display()),
            ))),
        }
    }

    fn sync_path(_path: &Path) -> Result<(), StorageError> {
        Ok(())
    }
}
