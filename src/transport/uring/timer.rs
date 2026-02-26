use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

/// Hierarchical timer wheel keyed by deadline (nanos since epoch).
pub(crate) struct TimerWheel {
    next_id: u64,
    /// deadline_nanos -> entries with that deadline
    timers: BTreeMap<u64, Vec<TimerEntry>>,
    /// timer_id -> deadline (for cancellation)
    id_to_deadline: BTreeMap<u64, u64>,
}

struct TimerEntry {
    id: u64,
    waker: Waker,
}

impl TimerWheel {
    pub fn new() -> Self {
        Self {
            next_id: 0,
            timers: BTreeMap::new(),
            id_to_deadline: BTreeMap::new(),
        }
    }

    /// Register a timer. Returns a timer ID for later cancellation.
    pub fn register(&mut self, deadline_nanos: u64, waker: Waker) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        self.id_to_deadline.insert(id, deadline_nanos);
        self.timers
            .entry(deadline_nanos)
            .or_default()
            .push(TimerEntry { id, waker });
        id
    }

    /// Update the waker for an existing timer.
    pub fn update_waker(&mut self, id: u64, waker: Waker) {
        if let Some(&deadline) = self.id_to_deadline.get(&id) {
            if let Some(entries) = self.timers.get_mut(&deadline) {
                if let Some(entry) = entries.iter_mut().find(|e| e.id == id) {
                    entry.waker = waker;
                }
            }
        }
    }

    /// Cancel a timer by ID.
    pub fn cancel(&mut self, id: u64) {
        if let Some(deadline) = self.id_to_deadline.remove(&id) {
            if let Some(entries) = self.timers.get_mut(&deadline) {
                entries.retain(|e| e.id != id);
                if entries.is_empty() {
                    self.timers.remove(&deadline);
                }
            }
        }
    }

    /// Fire all timers with deadline <= now. Calls waker.wake().
    pub fn advance(&mut self, now_nanos: u64) {
        let expired: Vec<u64> = self
            .timers
            .range(..=now_nanos)
            .map(|(&k, _)| k)
            .collect();
        for deadline in expired {
            if let Some(entries) = self.timers.remove(&deadline) {
                for entry in entries {
                    self.id_to_deadline.remove(&entry.id);
                    entry.waker.wake();
                }
            }
        }
    }

    /// Duration until the next timer fires, or None if empty.
    pub fn time_until_next(&self, now_nanos: u64) -> Option<Duration> {
        self.timers.keys().next().map(|&deadline| {
            let nanos = deadline.saturating_sub(now_nanos);
            Duration::from_nanos(nanos)
        })
    }
}

/// State for the UringSleep future.
enum SleepState {
    /// Not yet registered with the timer wheel.
    Init { deadline_nanos: u64 },
    /// Registered, waiting for timer to fire.
    Waiting { timer_id: u64 },
    /// Timer has fired.
    Done,
}

/// A sleep future backed by the timer wheel in the thread-local reactor.
pub struct UringSleep {
    state: SleepState,
}

// SAFETY: UringSleep is only used within a single thread-per-core
// reactor. The Send bound is required by the Transport trait but
// cross-thread usage is prevented by the CoreLauncher architecture.
unsafe impl Send for UringSleep {}

impl UringSleep {
    pub fn new(duration: Duration) -> Self {
        let now = super::reactor::now_nanos();
        let deadline_nanos = now + duration.as_nanos() as u64;
        Self {
            state: SleepState::Init { deadline_nanos },
        }
    }
}

impl Future for UringSleep {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = self.get_mut();
        match &this.state {
            SleepState::Init { deadline_nanos } => {
                let deadline = *deadline_nanos;
                let now = super::reactor::now_nanos();
                if now >= deadline {
                    this.state = SleepState::Done;
                    return Poll::Ready(());
                }
                let timer_id = super::reactor::with_reactor(|r| {
                    r.timer_wheel.register(deadline, cx.waker().clone())
                });
                this.state = SleepState::Waiting { timer_id };
                Poll::Pending
            }
            SleepState::Waiting { timer_id } => {
                let id = *timer_id;
                // Check if time has elapsed
                let now = super::reactor::now_nanos();
                let deadline = super::reactor::with_reactor(|r| {
                    r.timer_wheel.id_to_deadline.get(&id).copied()
                });
                match deadline {
                    Some(d) if now >= d => {
                        super::reactor::with_reactor(|r| {
                            r.timer_wheel.cancel(id);
                        });
                        this.state = SleepState::Done;
                        Poll::Ready(())
                    }
                    Some(_) => {
                        super::reactor::with_reactor(|r| {
                            r.timer_wheel.update_waker(id, cx.waker().clone());
                        });
                        Poll::Pending
                    }
                    None => {
                        // Timer was already fired via advance()
                        this.state = SleepState::Done;
                        Poll::Ready(())
                    }
                }
            }
            SleepState::Done => Poll::Ready(()),
        }
    }
}

impl Drop for UringSleep {
    fn drop(&mut self) {
        if let SleepState::Waiting { timer_id } = &self.state {
            let id = *timer_id;
            // Cancel the timer in the reactor if still active.
            // Use try_with_reactor to avoid panic during shutdown.
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                super::reactor::with_reactor(|r| {
                    r.timer_wheel.cancel(id);
                });
            }));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::task::{RawWaker, RawWakerVTable};

    fn noop_waker() -> Waker {
        const VTABLE: RawWakerVTable = RawWakerVTable::new(
            |p| RawWaker::new(p, &VTABLE),
            |_| {},
            |_| {},
            |_| {},
        );
        unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &VTABLE)) }
    }

    #[test]
    fn timer_wheel_basic_fire() {
        let mut tw = TimerWheel::new();
        let w = noop_waker();
        let _id = tw.register(100, w);
        assert!(tw.time_until_next(0).is_some());
        tw.advance(50);
        assert!(tw.time_until_next(50).is_some());
        tw.advance(100);
        assert!(tw.time_until_next(100).is_none());
    }

    #[test]
    fn timer_wheel_cancel() {
        let mut tw = TimerWheel::new();
        let w = noop_waker();
        let id = tw.register(200, w);
        tw.cancel(id);
        assert!(tw.time_until_next(0).is_none());
    }

    #[test]
    fn time_until_next_duration() {
        let mut tw = TimerWheel::new();
        let w = noop_waker();
        tw.register(1_000_000, w);
        let d = tw.time_until_next(500_000).unwrap();
        assert_eq!(d, Duration::from_nanos(500_000));
    }
}
