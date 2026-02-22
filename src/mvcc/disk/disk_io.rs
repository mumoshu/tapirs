use super::aligned_buf::AlignedBuf;
use super::error::StorageError;
use std::fs::OpenOptions;
use std::future::{Future, Ready, ready};
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::io::{AsRawFd, OwnedFd};
use std::path::Path;

/// Flags for opening a file.
#[derive(Debug, Clone, Copy)]
pub struct OpenFlags {
    pub create: bool,
    pub direct: bool,
}

impl Default for OpenFlags {
    fn default() -> Self {
        Self {
            create: true,
            direct: true,
        }
    }
}

/// Abstract disk I/O trait.
///
/// All pread/pwrite operations require 4 KiB-aligned buffers, offsets, and sizes.
///
/// In addition to per-file-handle operations (open, pread, pwrite, fsync, close,
/// file_len), this trait provides filesystem-level associated functions
/// (create_dir_all, remove_file, etc.) with default implementations that
/// delegate to `std::fs`. Implementations can override these to abstract
/// away filesystem access (e.g., for in-memory or remote storage).
pub trait DiskIo: Clone + Send + 'static {
    type ReadFuture: Future<Output = Result<(), StorageError>> + Send;
    type WriteFuture: Future<Output = Result<(), StorageError>> + Send;

    fn open(path: &Path, flags: OpenFlags) -> Result<Self, StorageError>;
    fn pread(&self, buf: &mut AlignedBuf, offset: u64) -> Self::ReadFuture;
    fn pwrite(&self, buf: &AlignedBuf, offset: u64) -> Self::WriteFuture;
    fn fsync(&self) -> impl Future<Output = Result<(), StorageError>> + Send;
    fn close(self);

    /// Return the size of the open file in bytes (via fstat on the fd).
    fn file_len(&self) -> Result<u64, StorageError>;

    // -- Filesystem-level operations (default to std::fs) --

    /// Create a directory and all parent directories.
    fn create_dir_all(path: &Path) -> Result<(), StorageError> {
        std::fs::create_dir_all(path)?;
        Ok(())
    }

    /// Remove a file.
    fn remove_file(path: &Path) -> Result<(), StorageError> {
        std::fs::remove_file(path)?;
        Ok(())
    }

    /// Check whether a path exists.
    fn exists(path: &Path) -> bool {
        path.exists()
    }

    /// Read an entire file into a byte vector.
    fn read_file(path: &Path) -> Result<Vec<u8>, StorageError> {
        Ok(std::fs::read(path)?)
    }

    /// Write `data` to a file, creating or truncating it.
    fn write_file(path: &Path, data: &[u8]) -> Result<(), StorageError> {
        std::fs::write(path, data)?;
        Ok(())
    }

    /// Atomically rename `from` to `to`.
    fn rename(from: &Path, to: &Path) -> Result<(), StorageError> {
        std::fs::rename(from, to)?;
        Ok(())
    }

    /// Open a path and fsync it (useful for files or directories).
    fn sync_path(path: &Path) -> Result<(), StorageError> {
        let file = std::fs::File::open(path)?;
        file.sync_all()?;
        Ok(())
    }

    /// Block on a future to completion using this IO backend's executor.
    ///
    /// Default: `futures::executor::block_on`. Works for synchronous backends
    /// (BufferedIo, SyncDirectIo, MemoryIo) whose futures resolve immediately.
    ///
    /// UringDirectIo overrides this to drive the io_uring ring (submit SQEs,
    /// wait for CQEs) so io_uring futures can complete.
    fn block_on<F: Future>(fut: F) -> F::Output {
        futures::executor::block_on(fut)
    }
}

/// Synchronous O_DIRECT I/O (Phase 1 implementation).
///
/// Blocks the calling thread on each operation — acceptable for
/// development and testing. Production would use `UringDirectIo`.
#[derive(Clone)]
pub struct SyncDirectIo {
    fd: std::sync::Arc<OwnedFd>,
}

impl DiskIo for SyncDirectIo {
    type ReadFuture = Ready<Result<(), StorageError>>;
    type WriteFuture = Ready<Result<(), StorageError>>;

    fn open(path: &Path, flags: OpenFlags) -> Result<Self, StorageError> {
        let mut opts = OpenOptions::new();
        opts.read(true).write(true).create(flags.create);
        if flags.direct {
            opts.custom_flags(libc::O_DIRECT);
        }
        let file = opts.open(path)?;
        Ok(Self {
            fd: std::sync::Arc::new(file.into()),
        })
    }

    fn pread(&self, buf: &mut AlignedBuf, offset: u64) -> Self::ReadFuture {
        let n = unsafe {
            libc::pread(
                self.fd.as_raw_fd(),
                buf.as_mut_ptr() as *mut libc::c_void,
                buf.capacity(),
                offset as libc::off_t,
            )
        };
        if n < 0 {
            return ready(Err(std::io::Error::last_os_error().into()));
        }
        buf.set_len(n as usize);
        ready(Ok(()))
    }

    fn pwrite(&self, buf: &AlignedBuf, offset: u64) -> Self::WriteFuture {
        let n = unsafe {
            libc::pwrite(
                self.fd.as_raw_fd(),
                buf.as_ptr() as *const libc::c_void,
                buf.capacity(),
                offset as libc::off_t,
            )
        };
        if n < 0 {
            return ready(Err(std::io::Error::last_os_error().into()));
        }
        ready(Ok(()))
    }

    fn fsync(&self) -> impl Future<Output = Result<(), StorageError>> + Send {
        let ret = unsafe { libc::fsync(self.fd.as_raw_fd()) };
        if ret < 0 {
            ready(Err(std::io::Error::last_os_error().into()))
        } else {
            ready(Ok(()))
        }
    }

    fn close(self) {
        // OwnedFd drops and closes automatically.
        drop(self);
    }

    fn file_len(&self) -> Result<u64, StorageError> {
        let mut stat: libc::stat = unsafe { std::mem::zeroed() };
        let ret = unsafe { libc::fstat(self.fd.as_raw_fd(), &mut stat) };
        if ret < 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        Ok(stat.st_size as u64)
    }
}

/// Non-direct I/O for testing (no O_DIRECT alignment requirements).
#[derive(Clone)]
pub struct BufferedIo {
    fd: std::sync::Arc<OwnedFd>,
}

impl DiskIo for BufferedIo {
    type ReadFuture = Ready<Result<(), StorageError>>;
    type WriteFuture = Ready<Result<(), StorageError>>;

    fn open(path: &Path, flags: OpenFlags) -> Result<Self, StorageError> {
        let mut opts = OpenOptions::new();
        opts.read(true).write(true).create(flags.create);
        // No O_DIRECT — works on any filesystem (tmpfs, etc.)
        let file = opts.open(path)?;
        Ok(Self {
            fd: std::sync::Arc::new(file.into()),
        })
    }

    fn pread(&self, buf: &mut AlignedBuf, offset: u64) -> Self::ReadFuture {
        let n = unsafe {
            libc::pread(
                self.fd.as_raw_fd(),
                buf.as_mut_ptr() as *mut libc::c_void,
                buf.capacity(),
                offset as libc::off_t,
            )
        };
        if n < 0 {
            return ready(Err(std::io::Error::last_os_error().into()));
        }
        buf.set_len(n as usize);
        ready(Ok(()))
    }

    fn pwrite(&self, buf: &AlignedBuf, offset: u64) -> Self::WriteFuture {
        let n = unsafe {
            libc::pwrite(
                self.fd.as_raw_fd(),
                buf.as_ptr() as *const libc::c_void,
                buf.capacity(),
                offset as libc::off_t,
            )
        };
        if n < 0 {
            return ready(Err(std::io::Error::last_os_error().into()));
        }
        ready(Ok(()))
    }

    fn fsync(&self) -> impl Future<Output = Result<(), StorageError>> + Send {
        let ret = unsafe { libc::fsync(self.fd.as_raw_fd()) };
        if ret < 0 {
            ready(Err(std::io::Error::last_os_error().into()))
        } else {
            ready(Ok(()))
        }
    }

    fn close(self) {
        drop(self);
    }

    fn file_len(&self) -> Result<u64, StorageError> {
        let mut stat: libc::stat = unsafe { std::mem::zeroed() };
        let ret = unsafe { libc::fstat(self.fd.as_raw_fd(), &mut stat) };
        if ret < 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        Ok(stat.st_size as u64)
    }
}
