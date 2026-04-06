use super::reactor::{with_reactor, Completion, OpKey};
use crate::mvcc::disk::aligned_buf::AlignedBuf;
use crate::mvcc::disk::disk_io::{DiskIo, OpenFlags};
use crate::mvcc::disk::StorageError;
use io_uring::opcode;
use io_uring::types::Fd;
use std::future::Future;
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::io::{AsRawFd, OwnedFd};
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

/// io_uring-based direct I/O implementation.
#[derive(Clone)]
pub struct UringDirectIo {
    fd: Arc<OwnedFd>,
}

impl DiskIo for UringDirectIo {
    type ReadFuture = UringReadFuture;
    type WriteFuture = UringWriteFuture;

    fn open(path: &Path, flags: OpenFlags, _expected_size: Option<u64>) -> Result<Self, StorageError> {
        let mut opts = std::fs::OpenOptions::new();
        opts.read(true).write(true).create(flags.create);
        if flags.direct {
            opts.custom_flags(libc::O_DIRECT);
        }
        let file = opts.open(path)?;
        Ok(Self {
            fd: Arc::new(file.into()),
        })
    }

    fn pread(&self, buf: &mut AlignedBuf, offset: u64) -> Self::ReadFuture {
        UringReadFuture {
            fd: self.fd.as_raw_fd(),
            buf: buf as *mut AlignedBuf,
            offset,
            key: None,
        }
    }

    fn pwrite(&self, buf: &AlignedBuf, offset: u64) -> Self::WriteFuture {
        UringWriteFuture {
            fd: self.fd.as_raw_fd(),
            buf: buf as *const AlignedBuf,
            offset,
            key: None,
        }
    }

    fn fsync(&self) -> impl Future<Output = Result<(), StorageError>> + Send {
        UringFsyncFuture {
            fd: self.fd.as_raw_fd(),
            key: None,
        }
    }

    fn close(self) {
        // OwnedFd drops and closes automatically via Arc.
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

    fn block_on<F: std::future::Future>(fut: F) -> F::Output {
        super::reactor::uring_block_on(fut)
    }
}

// SAFETY: UringDirectIo is only used within a single thread-per-core
// reactor. The Send bound is required by DiskIo trait but cross-thread
// usage is prevented by the CoreLauncher architecture.
unsafe impl Send for UringReadFuture {}
unsafe impl Send for UringWriteFuture {}

pub struct UringReadFuture {
    fd: i32,
    buf: *mut AlignedBuf,
    offset: u64,
    key: Option<OpKey>,
}

impl Future for UringReadFuture {
    type Output = Result<(), StorageError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if this.key.is_none() {
            // SAFETY: buf pointer is valid for the lifetime of the pread call.
            let buf = unsafe { &mut *this.buf };
            let entry = opcode::Read::new(
                Fd(this.fd),
                buf.as_mut_ptr(),
                buf.capacity() as u32,
            )
            .offset(this.offset)
            .build();
            with_reactor(|r| {
                let key = r.next_key();
                unsafe { r.submit_sqe(entry.user_data(key.0)) };
                this.key = Some(key);
            });
        }
        let key = this.key.unwrap();
        with_reactor(|r| match r.register_waker(key, cx.waker().clone()) {
            Some(Completion { result, .. }) if result >= 0 => {
                let buf = unsafe { &mut *this.buf };
                buf.set_len(result as usize);
                Poll::Ready(Ok(()))
            }
            Some(Completion { result, .. }) => {
                Poll::Ready(Err(std::io::Error::from_raw_os_error(-result).into()))
            }
            None => Poll::Pending,
        })
    }
}

pub struct UringWriteFuture {
    fd: i32,
    buf: *const AlignedBuf,
    offset: u64,
    key: Option<OpKey>,
}

impl Future for UringWriteFuture {
    type Output = Result<(), StorageError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if this.key.is_none() {
            // SAFETY: buf pointer is valid for the lifetime of the pwrite call.
            let buf = unsafe { &*this.buf };
            let entry = opcode::Write::new(
                Fd(this.fd),
                buf.as_ptr(),
                buf.capacity() as u32,
            )
            .offset(this.offset)
            .build();
            with_reactor(|r| {
                let key = r.next_key();
                unsafe { r.submit_sqe(entry.user_data(key.0)) };
                this.key = Some(key);
            });
        }
        let key = this.key.unwrap();
        with_reactor(|r| match r.register_waker(key, cx.waker().clone()) {
            Some(Completion { result, .. }) if result >= 0 => Poll::Ready(Ok(())),
            Some(Completion { result, .. }) => {
                Poll::Ready(Err(std::io::Error::from_raw_os_error(-result).into()))
            }
            None => Poll::Pending,
        })
    }
}

struct UringFsyncFuture {
    fd: i32,
    key: Option<OpKey>,
}

impl Future for UringFsyncFuture {
    type Output = Result<(), StorageError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if this.key.is_none() {
            let entry = opcode::Fsync::new(Fd(this.fd)).build();
            with_reactor(|r| {
                let key = r.next_key();
                unsafe { r.submit_sqe(entry.user_data(key.0)) };
                this.key = Some(key);
            });
        }
        let key = this.key.unwrap();
        with_reactor(|r| match r.register_waker(key, cx.waker().clone()) {
            Some(Completion { result, .. }) if result >= 0 => Poll::Ready(Ok(())),
            Some(Completion { result, .. }) => {
                Poll::Ready(Err(std::io::Error::from_raw_os_error(-result).into()))
            }
            None => Poll::Pending,
        })
    }
}
