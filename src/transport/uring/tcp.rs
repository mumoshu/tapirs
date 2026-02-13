use super::reactor::{with_reactor, Completion, OpKey};
use io_uring::opcode;
use io_uring::types::Fd;
use socket2::{Domain, Protocol, SockAddr, Socket, Type};
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::os::unix::io::{AsRawFd, RawFd};
use std::pin::Pin;
use std::task::{Context, Poll};

/// A TCP listener that accepts connections via io_uring.
pub(crate) struct TcpListener {
    fd: RawFd,
}

impl TcpListener {
    /// Bind a TCP listener with SO_REUSEPORT for multi-core accept.
    pub fn bind(addr: SocketAddr) -> io::Result<Self> {
        let domain = if addr.is_ipv4() {
            Domain::IPV4
        } else {
            Domain::IPV6
        };
        let socket = Socket::new(domain, Type::STREAM, Some(Protocol::TCP))?;
        socket.set_reuse_port(true)?;
        socket.set_reuse_address(true)?;
        socket.set_nonblocking(true)?;
        socket.bind(&SockAddr::from(addr))?;
        socket.listen(128)?;
        let fd = socket.as_raw_fd();
        std::mem::forget(socket); // Ownership transferred to TcpListener.
        Ok(Self { fd })
    }

    /// Accept a new connection. Returns (TcpStream, peer_addr).
    pub fn accept(&self) -> AcceptFuture {
        AcceptFuture {
            listener_fd: self.fd,
            key: None,
            addr: Box::new(unsafe { std::mem::zeroed::<libc::sockaddr_storage>() }),
            addr_len: Box::new(std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t),
        }
    }
}

impl Drop for TcpListener {
    fn drop(&mut self) {
        unsafe { libc::close(self.fd) };
    }
}

pub(crate) struct AcceptFuture {
    listener_fd: RawFd,
    key: Option<OpKey>,
    addr: Box<libc::sockaddr_storage>,
    addr_len: Box<libc::socklen_t>,
}

impl Future for AcceptFuture {
    type Output = io::Result<(TcpStream, SocketAddr)>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if this.key.is_none() {
            let entry = opcode::Accept::new(
                Fd(this.listener_fd),
                this.addr.as_mut() as *mut _ as *mut _,
                this.addr_len.as_mut(),
            )
            .build();
            with_reactor(|r| {
                let key = r.next_key();
                // SAFETY: addr and addr_len are heap-allocated in this
                // future and live until the future is dropped/completed.
                unsafe { r.submit_sqe(entry.user_data(key.0)) };
                this.key = Some(key);
            });
        }
        let key = this.key.unwrap();
        with_reactor(|r| match r.register_waker(key, cx.waker().clone()) {
            Some(Completion { result, .. }) if result >= 0 => {
                let fd = result;
                let peer = sockaddr_to_socketaddr(&*this.addr, *this.addr_len);
                Poll::Ready(Ok((TcpStream { fd }, peer)))
            }
            Some(Completion { result, .. }) => {
                Poll::Ready(Err(io::Error::from_raw_os_error(-result)))
            }
            None => Poll::Pending,
        })
    }
}

/// A TCP stream with io_uring-based recv/send.
pub(crate) struct TcpStream {
    pub fd: RawFd,
}

impl TcpStream {
    /// Create a connected TCP stream.
    pub fn connect(addr: SocketAddr) -> ConnectFuture {
        let domain = if addr.is_ipv4() {
            Domain::IPV4
        } else {
            Domain::IPV6
        };
        let socket = Socket::new(domain, Type::STREAM, Some(Protocol::TCP))
            .expect("socket creation failed");
        socket.set_nonblocking(true).expect("set_nonblocking");
        let fd = socket.as_raw_fd();
        std::mem::forget(socket);
        ConnectFuture {
            fd,
            addr: Box::new(SockAddr::from(addr)),
            key: None,
            transferred: false,
        }
    }

    /// Receive data into buffer.
    pub fn recv<'a>(&self, buf: &'a mut [u8]) -> RecvFuture<'a> {
        RecvFuture {
            fd: self.fd,
            buf,
            key: None,
        }
    }

    /// Send data from buffer.
    pub fn send<'a>(&self, buf: &'a [u8]) -> SendFuture<'a> {
        SendFuture {
            fd: self.fd,
            buf,
            key: None,
        }
    }
}

impl Drop for TcpStream {
    fn drop(&mut self) {
        // Submit an async close via io_uring.
        let entry = opcode::Close::new(Fd(self.fd)).build();
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            with_reactor(|r| {
                let key = r.next_key();
                // SAFETY: Close doesn't reference any buffers.
                unsafe { r.submit_sqe(entry.user_data(key.0)) };
            });
        }));
    }
}

pub(crate) struct ConnectFuture {
    fd: RawFd,
    addr: Box<SockAddr>,
    key: Option<OpKey>,
    /// Set to true once the fd has been transferred to a TcpStream.
    transferred: bool,
}

impl Future for ConnectFuture {
    type Output = io::Result<TcpStream>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if this.key.is_none() {
            let entry = opcode::Connect::new(
                Fd(this.fd),
                this.addr.as_ptr(),
                this.addr.len(),
            )
            .build();
            with_reactor(|r| {
                let key = r.next_key();
                // SAFETY: addr is heap-allocated and lives until
                // the future completes.
                unsafe { r.submit_sqe(entry.user_data(key.0)) };
                this.key = Some(key);
            });
        }
        let key = this.key.unwrap();
        with_reactor(|r| match r.register_waker(key, cx.waker().clone()) {
            Some(Completion { result, .. }) if result >= 0 => {
                this.transferred = true;
                Poll::Ready(Ok(TcpStream { fd: this.fd }))
            }
            Some(Completion { result, .. }) => {
                Poll::Ready(Err(io::Error::from_raw_os_error(-result)))
            }
            None => Poll::Pending,
        })
    }
}

impl Drop for ConnectFuture {
    fn drop(&mut self) {
        if !self.transferred {
            unsafe { libc::close(self.fd) };
        }
    }
}

pub(crate) struct RecvFuture<'a> {
    fd: RawFd,
    buf: &'a mut [u8],
    key: Option<OpKey>,
}

impl<'a> Future for RecvFuture<'a> {
    type Output = io::Result<usize>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // SAFETY: We don't move out of self.
        let this = unsafe { self.get_unchecked_mut() };
        if this.key.is_none() {
            let entry = opcode::Recv::new(
                Fd(this.fd),
                this.buf.as_mut_ptr(),
                this.buf.len() as u32,
            )
            .build();
            with_reactor(|r| {
                let key = r.next_key();
                // SAFETY: buf lives as long as this future (borrow).
                unsafe { r.submit_sqe(entry.user_data(key.0)) };
                this.key = Some(key);
            });
        }
        let key = this.key.unwrap();
        with_reactor(|r| match r.register_waker(key, cx.waker().clone()) {
            Some(Completion { result, .. }) if result >= 0 => {
                Poll::Ready(Ok(result as usize))
            }
            Some(Completion { result, .. }) => {
                Poll::Ready(Err(io::Error::from_raw_os_error(-result)))
            }
            None => Poll::Pending,
        })
    }
}

pub(crate) struct SendFuture<'a> {
    fd: RawFd,
    buf: &'a [u8],
    key: Option<OpKey>,
}

impl<'a> Future for SendFuture<'a> {
    type Output = io::Result<usize>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        if this.key.is_none() {
            let entry = opcode::Send::new(
                Fd(this.fd),
                this.buf.as_ptr(),
                this.buf.len() as u32,
            )
            .build();
            with_reactor(|r| {
                let key = r.next_key();
                // SAFETY: buf lives as long as this future (borrow).
                unsafe { r.submit_sqe(entry.user_data(key.0)) };
                this.key = Some(key);
            });
        }
        let key = this.key.unwrap();
        with_reactor(|r| match r.register_waker(key, cx.waker().clone()) {
            Some(Completion { result, .. }) if result >= 0 => {
                Poll::Ready(Ok(result as usize))
            }
            Some(Completion { result, .. }) => {
                Poll::Ready(Err(io::Error::from_raw_os_error(-result)))
            }
            None => Poll::Pending,
        })
    }
}

fn sockaddr_to_socketaddr(
    storage: &libc::sockaddr_storage,
    _len: libc::socklen_t,
) -> SocketAddr {
    // SAFETY: We trust the kernel filled in a valid sockaddr.
    let sa = unsafe { SockAddr::new(*storage, _len) };
    sa.as_socket().expect("not an IP socket address")
}
