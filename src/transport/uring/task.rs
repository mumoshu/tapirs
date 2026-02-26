use std::collections::{BTreeMap, HashSet, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

/// Unique identifier for a spawned task.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct TaskId(pub u64);

/// Cooperative, thread-local, `!Send` task scheduler.
pub(crate) struct Executor {
    next_task_id: u64,
    run_queue: VecDeque<TaskId>,
    tasks: BTreeMap<TaskId, Pin<Box<dyn Future<Output = ()> + 'static>>>,
    in_run_queue: HashSet<TaskId>,
}

impl Executor {
    pub fn new() -> Self {
        Self {
            next_task_id: 0,
            run_queue: VecDeque::new(),
            tasks: BTreeMap::new(),
            in_run_queue: HashSet::new(),
        }
    }

    /// Spawn a new task onto this executor.
    pub fn spawn(&mut self, future: impl Future<Output = ()> + 'static) {
        let id = TaskId(self.next_task_id);
        self.next_task_id += 1;
        self.tasks.insert(id, Box::pin(future));
        self.schedule(id);
    }

    /// Add a task to the run queue if not already queued.
    pub fn schedule(&mut self, id: TaskId) {
        if self.tasks.contains_key(&id) && self.in_run_queue.insert(id) {
            self.run_queue.push_back(id);
        }
    }

    /// Poll all currently runnable tasks once. Ready tasks are dropped.
    pub fn poll_all(&mut self) {
        let snapshot: Vec<TaskId> = self.run_queue.drain(..).collect();
        self.in_run_queue.clear();
        for id in snapshot {
            if let Some(mut future) = self.tasks.remove(&id) {
                let waker = make_waker(id);
                let mut cx = Context::from_waker(&waker);
                match future.as_mut().poll(&mut cx) {
                    Poll::Ready(()) => { /* task done, dropped */ }
                    Poll::Pending => {
                        self.tasks.insert(id, future);
                    }
                }
            }
        }
    }

    /// Returns true if there are tasks waiting to run.
    pub fn has_runnable(&self) -> bool {
        !self.run_queue.is_empty()
    }

    /// Returns total number of live tasks.
    pub fn task_count(&self) -> usize {
        self.tasks.len()
    }
}

// --- Waker implementation ---
// Encodes TaskId in the data pointer. wake() schedules the task
// via the thread-local reactor.

fn make_waker(id: TaskId) -> Waker {
    let data = id.0 as *const ();
    // SAFETY: Our RawWakerVTable functions correctly handle the
    // data pointer as an encoded TaskId.
    unsafe { Waker::from_raw(RawWaker::new(data, &VTABLE)) }
}

const VTABLE: RawWakerVTable = RawWakerVTable::new(
    waker_clone,
    waker_wake,
    waker_wake_by_ref,
    waker_drop,
);

unsafe fn waker_clone(data: *const ()) -> RawWaker {
    RawWaker::new(data, &VTABLE)
}

unsafe fn waker_wake(data: *const ()) {
    // SAFETY: Delegating to waker_wake_by_ref with the same data pointer.
    unsafe { waker_wake_by_ref(data) };
}

unsafe fn waker_wake_by_ref(data: *const ()) {
    let id = TaskId(data as u64);
    // Use try_with_reactor to avoid panic when reactor is not initialized
    // (e.g., in unit tests where the executor is used standalone).
    super::reactor::try_with_reactor(|r| r.executor.schedule(id));
}

unsafe fn waker_drop(_data: *const ()) {
    // No-op: data is a plain integer, not heap-allocated.
}

pub(crate) fn make_task_waker(id: TaskId) -> Waker {
    make_waker(id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;
    use std::rc::Rc;

    #[test]
    fn spawn_and_poll_ready() {
        let mut exec = Executor::new();
        let ran = Rc::new(Cell::new(false));
        let ran2 = ran.clone();
        exec.spawn(async move {
            ran2.set(true);
        });
        assert!(exec.has_runnable());
        exec.poll_all();
        assert!(ran.get());
        assert_eq!(exec.task_count(), 0);
    }

    #[test]
    fn pending_task_stays_alive() {
        let mut exec = Executor::new();
        let count = Rc::new(Cell::new(0u32));
        let count2 = count.clone();
        exec.spawn(async move {
            // Yield once, manually re-scheduling.
            let mut first = true;
            std::future::poll_fn(move |_cx| {
                count2.set(count2.get() + 1);
                if first {
                    first = false;
                    // Waker goes through reactor which may not exist;
                    // we manually schedule below instead.
                    Poll::Pending
                } else {
                    Poll::Ready(())
                }
            })
            .await;
        });
        exec.poll_all();
        assert_eq!(count.get(), 1);
        assert_eq!(exec.task_count(), 1);
        // Manually re-schedule since waker may not have reactor.
        exec.schedule(TaskId(0));
        exec.poll_all();
        assert_eq!(count.get(), 2);
        assert_eq!(exec.task_count(), 0);
    }
}
