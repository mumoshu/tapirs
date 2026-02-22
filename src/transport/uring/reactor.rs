use super::task::Executor;
use super::timer::TimerWheel;
use io_uring::{opcode, types, IoUring};
use std::cell::RefCell;
use std::collections::HashMap;
use std::task::Waker;
use std::time::SystemTime;

/// Opaque key identifying an in-flight io_uring operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct OpKey(pub u64);

/// Completion result from a CQE.
pub(crate) struct Completion {
    pub result: i32,
    pub flags: u32,
}

enum OpState {
    /// SQE submitted, no waker yet.
    Submitted,
    /// Waiting for CQE, waker registered.
    Waiting(Waker),
    /// CQE arrived, result stored.
    Completed(Completion),
}

/// Per-thread reactor owning one io_uring instance.
pub(crate) struct Reactor {
    ring: IoUring,
    next_op_key: u64,
    ops: HashMap<u64, OpState>,
    pub executor: Executor,
    pub timer_wheel: TimerWheel,
}

impl Reactor {
    pub fn new(ring_size: u32) -> Self {
        let ring = IoUring::new(ring_size).expect("failed to create io_uring");
        Self {
            ring,
            next_op_key: 0,
            ops: HashMap::new(),
            executor: Executor::new(),
            timer_wheel: TimerWheel::new(),
        }
    }

    /// Allocate a new OpKey and mark it as Submitted.
    pub fn next_key(&mut self) -> OpKey {
        let key = OpKey(self.next_op_key);
        self.next_op_key += 1;
        self.ops.insert(key.0, OpState::Submitted);
        key
    }

    /// Push an SQE to the submission queue. Flushes if full.
    ///
    /// # Safety
    /// The caller must ensure the SQE references (buffers, fds) remain
    /// valid until the corresponding CQE is reaped.
    pub unsafe fn submit_sqe(&mut self, entry: io_uring::squeue::Entry) {
        loop {
            // SAFETY: caller guarantees buffer/fd lifetimes.
            match unsafe { self.ring.submission().push(&entry) } {
                Ok(()) => break,
                Err(_) => {
                    // SQ full — flush and retry.
                    self.ring.submit().expect("submit failed");
                }
            }
        }
    }

    /// Register a waker for an OpKey, or return the completion if ready.
    pub fn register_waker(
        &mut self,
        key: OpKey,
        waker: Waker,
    ) -> Option<Completion> {
        match self.ops.remove(&key.0) {
            Some(OpState::Completed(c)) => Some(c),
            Some(OpState::Submitted) | Some(OpState::Waiting(_)) => {
                self.ops.insert(key.0, OpState::Waiting(waker));
                None
            }
            None => None,
        }
    }

    /// Reap all available CQEs and wake corresponding futures.
    fn reap_cqes(&mut self) {
        while let Some(cqe) = self.ring.completion().next() {
            let key = cqe.user_data();
            let completion = Completion {
                result: cqe.result(),
                flags: cqe.flags(),
            };
            match self.ops.remove(&key) {
                Some(OpState::Waiting(waker)) => {
                    self.ops
                        .insert(key, OpState::Completed(completion));
                    waker.wake();
                }
                Some(OpState::Submitted) => {
                    self.ops
                        .insert(key, OpState::Completed(completion));
                }
                _ => {}
            }
        }
    }

    /// Main event loop. Runs forever (until the thread is terminated).
    pub fn run(&mut self) -> ! {
        loop {
            // 1. Poll all runnable tasks.
            self.executor.poll_all();

            // 2. Advance timers.
            let now = now_nanos();
            self.timer_wheel.advance(now);

            // 3. Submit queued SQEs.
            if self.executor.has_runnable() {
                // Non-blocking submit.
                let _ = self.ring.submit();
            } else {
                // Block until at least one CQE or timer deadline.
                let timeout = self.timer_wheel.time_until_next(now);
                match timeout {
                    Some(d) if !d.is_zero() => {
                        let ts = types::Timespec::new()
                            .sec(d.as_secs())
                            .nsec(d.subsec_nanos());
                        let timeout_entry = opcode::Timeout::new(&ts)
                            .build()
                            .user_data(u64::MAX);
                        // SAFETY: Timespec is on the stack but we block
                        // inside submit_and_wait so it stays valid.
                        unsafe {
                            let _ = self.ring.submission().push(&timeout_entry);
                        }
                        let _ = self.ring.submit_and_wait(1);
                    }
                    _ => {
                        if self.executor.task_count() > 0 {
                            let _ = self.ring.submit_and_wait(1);
                        } else {
                            // No tasks, no timers — yield CPU briefly.
                            std::thread::yield_now();
                        }
                    }
                }
            }

            // 4. Reap CQEs.
            self.reap_cqes();
        }
    }
}

thread_local! {
    static REACTOR: RefCell<Option<Reactor>> = RefCell::new(None);
}

/// Access the thread-local reactor. Panics if not initialized.
pub(crate) fn with_reactor<R>(f: impl FnOnce(&mut Reactor) -> R) -> R {
    REACTOR.with(|cell| {
        let mut borrow = cell.borrow_mut();
        let reactor = borrow
            .as_mut()
            .expect("reactor not initialized on this thread");
        f(reactor)
    })
}

/// Initialize the thread-local reactor (called once per core thread).
pub(crate) fn init_reactor(ring_size: u32) {
    REACTOR.with(|cell| {
        let mut borrow = cell.borrow_mut();
        assert!(borrow.is_none(), "reactor already initialized");
        *borrow = Some(Reactor::new(ring_size));
    });
}

/// Try to access the thread-local reactor. Returns None if not initialized.
pub(crate) fn try_with_reactor<R>(f: impl FnOnce(&mut Reactor) -> R) -> Option<R> {
    REACTOR.with(|cell| {
        let mut borrow = cell.borrow_mut();
        borrow.as_mut().map(f)
    })
}

/// Ensure the thread-local reactor is initialized, creating one if needed.
/// Idempotent: no-op if already initialized.
pub(crate) fn ensure_reactor(ring_size: u32) {
    REACTOR.with(|cell| {
        let mut borrow = cell.borrow_mut();
        if borrow.is_none() {
            *borrow = Some(Reactor::new(ring_size));
        }
    });
}

/// Block on a future by driving the io_uring ring inline.
///
/// Polls the future, then submits SQEs and reaps CQEs in a loop until
/// the future completes. Used by `UringDirectIo::block_on` as the
/// replacement for `futures::executor::block_on`.
///
/// Lazy-initializes the thread-local reactor if not yet present (ring
/// size 256, matching `CoreConfig::default()`).
pub(crate) fn uring_block_on<F: std::future::Future>(fut: F) -> F::Output {
    ensure_reactor(256);

    let mut fut = std::pin::pin!(fut);
    let waker = std::task::Waker::noop();
    let mut cx = std::task::Context::from_waker(&waker);

    loop {
        match fut.as_mut().poll(&mut cx) {
            std::task::Poll::Ready(output) => return output,
            std::task::Poll::Pending => {}
        }

        with_reactor(|r| {
            let _ = r.ring.submit_and_wait(1);
            r.reap_cqes();
        });
    }
}

/// Current time in nanoseconds since epoch.
pub(crate) fn now_nanos() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}
