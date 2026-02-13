use super::task::Executor;
use super::timer::TimerWheel;
use std::cell::RefCell;
use std::time::SystemTime;

/// Per-thread reactor owning executor and timer wheel.
/// The io_uring instance is added in subtask 3.
pub(crate) struct Reactor {
    pub executor: Executor,
    pub timer_wheel: TimerWheel,
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
pub(crate) fn init_reactor() {
    REACTOR.with(|cell| {
        let mut borrow = cell.borrow_mut();
        assert!(borrow.is_none(), "reactor already initialized");
        *borrow = Some(Reactor {
            executor: Executor::new(),
            timer_wheel: TimerWheel::new(),
        });
    });
}

/// Current time in nanoseconds since epoch.
pub(crate) fn now_nanos() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}
