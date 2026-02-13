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
/// All operations require 4 KiB-aligned buffers, offsets, and sizes.
pub trait DiskIo: Clone + Send + 'static {
    type ReadFuture: Future<Output = Result<(), StorageError>> + Send;
    type WriteFuture: Future<Output = Result<(), StorageError>> + Send;

    fn open(path: &Path, flags: OpenFlags) -> Result<Self, StorageError>;
    fn pread(&self, buf: &mut AlignedBuf, offset: u64) -> Self::ReadFuture;
    fn pwrite(&self, buf: &AlignedBuf, offset: u64) -> Self::WriteFuture;
    fn fsync(&self) -> impl Future<Output = Result<(), StorageError>> + Send;
    fn close(self);
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
}
