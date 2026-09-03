use crossbeam_deque::Injector as GlobalQueue;
use crossbeam_deque::Steal;
use crossbeam_deque::Worker as LocalQueue;
use std::cell::Cell;
use std::pin::Pin;
use std::ptr::NonNull;
use std::sync::{LazyLock, Mutex};
use std::thread;

use crate::context::Context;
use crate::context::switch;
use crate::routine::RsRoutine;

thread_local! {
    static WORKER: Cell<Option<NonNull<Worker>>> = const { Cell::new(None) };
}

enum RunOutcome {
    Yielded,
    /// ParkRequest(ParkRequest)
    /// TODO: publish this result through a JoinHandle.
    Completed(std::thread::Result<()>),
}

/// `Wrapper around the routine Pin<Box<RsRoutine>>` keeps the routine from moving.
struct Task {
    routine: Pin<Box<RsRoutine>>,
    outcome: Option<RunOutcome>,
}

impl Task {
    fn new(routine: Pin<Box<RsRoutine>>) -> Self {
        Self {
            routine,
            outcome: None,
        }
    }
}

static RUNTIME: LazyLock<Runtime> = LazyLock::new(|| {
    let incoming_queue = GlobalQueue::new();
    let num_workers = num_cpus::get().max(1);
    let mut worker_threads = Vec::with_capacity(num_workers);
    for i in 0..num_workers {
        let local_queue = LocalQueue::new_fifo();
        let handle = thread::spawn(move || {
            let mut worker = Box::new(Worker {
                id: WorkerId(i),
                local_queue,
                tick: 0,
                context: Context::default(),
                current: None,
            });
            let worker_ptr = NonNull::from(worker.as_mut());
            WORKER.set(Some(worker_ptr));
            Worker::poll(worker_ptr);
            // if worker ever quits we should clean the pointer
            WORKER.set(None);
        });
        worker_threads.push(handle.thread().clone());
    }
    Runtime::new(incoming_queue, worker_threads)
});

struct Runtime {
    incoming_queue: GlobalQueue<Task>,
    worker_threads: Vec<thread::Thread>,
    idle_workers: Mutex<IdleWorkers>,
}

impl Runtime {
    fn new(incoming_queue: GlobalQueue<Task>, worker_threads: Vec<thread::Thread>) -> Self {
        let worker_count = worker_threads.len();
        Self {
            incoming_queue,
            worker_threads,
            idle_workers: Mutex::new(IdleWorkers::new(worker_count)),
        }
    }

    fn schedule(&self, task: Task) {
        let worker_to_wake = {
            let mut idle_workers = self.idle_workers.lock().expect("idle worker lock poisoned");
            self.incoming_queue.push(task);
            idle_workers
                .take_one()
                .map(|id| self.worker_threads[id.0].clone())
        };

        if let Some(worker) = worker_to_wake {
            worker.unpark();
        }
    }

    fn register_idle(&self, id: WorkerId) {
        self.idle_workers
            .lock()
            .expect("idle worker lock poisoned")
            .register(id);
    }

    fn cancel_idle(&self, id: WorkerId) {
        self.idle_workers
            .lock()
            .expect("idle worker lock poisoned")
            .cancel(id);
    }

    #[cfg(test)]
    fn idle_count(&self) -> usize {
        self.idle_workers
            .lock()
            .expect("idle worker lock poisoned")
            .ids
            .len()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct WorkerId(usize);

struct IdleWorkers {
    ids: Vec<WorkerId>,
    registered: Vec<bool>,
}

impl IdleWorkers {
    fn new(worker_count: usize) -> Self {
        Self {
            ids: Vec::with_capacity(worker_count),
            registered: vec![false; worker_count],
        }
    }

    fn register(&mut self, id: WorkerId) {
        assert!(id.0 < self.registered.len(), "invalid worker ID");
        assert!(!self.registered[id.0], "worker registered as idle twice");
        self.registered[id.0] = true;
        self.ids.push(id);
    }

    fn cancel(&mut self, id: WorkerId) {
        if !self.registered[id.0] {
            return;
        }

        let position = self
            .ids
            .iter()
            .position(|candidate| *candidate == id)
            .expect("registered idle worker must have an ID entry");
        self.ids.swap_remove(position);
        self.registered[id.0] = false;
    }

    fn take_one(&mut self) -> Option<WorkerId> {
        let id = self.ids.pop()?;
        assert!(self.registered[id.0]);
        self.registered[id.0] = false;
        Some(id)
    }
}

struct Worker {
    id: WorkerId,
    // This queue is deliberately private: once a task starts, values created on its stack need
    // not be `Send`, so a yielded task must resume on the same OS thread.
    local_queue: LocalQueue<Task>,
    // Alternate queue priority so neither new tasks nor yielded continuations can starve.
    tick: u32,
    // Worker context
    context: Context,
    current: Option<Task>,
}

fn suspend_current(outcome: RunOutcome) {
    let worker_ptr = WORKER.with(|slot| {
        slot.get()
            .expect("yield_now called outside runtime")
            .as_ptr()
    });
    let (from, to) = unsafe {
        let worker = &mut *worker_ptr;
        let task = worker.current.as_mut().expect("no running task to suspend");
        assert!(task.outcome.replace(outcome).is_none());
        let routine = task.routine.as_mut().get_unchecked_mut();
        (&raw mut routine.context, &raw const worker.context)
    };
    switch(from, to);
}

pub fn yield_now() {
    suspend_current(RunOutcome::Yielded);
}

pub fn spawn<F>(func: F)
where
    F: FnOnce() + Send + 'static,
{
    let routine = RsRoutine::new_pinned(Box::new(func), crate::context::bootstrap_entry_addr());
    RUNTIME.schedule(Task::new(routine));
}

pub(crate) fn complete_current(result: std::thread::Result<()>) -> ! {
    suspend_current(RunOutcome::Completed(result));
    unreachable!("completed task was resumed");
}

impl Worker {
    fn find_task(&mut self) -> Option<Task> {
        let prefer_local = self.tick.is_multiple_of(2);

        let task = if prefer_local {
            self.local_queue.pop().or_else(Self::find_incoming_task)
        } else {
            Self::find_incoming_task().or_else(|| self.local_queue.pop())
        };

        if task.is_some() {
            self.tick = self.tick.wrapping_add(1);
        };
        task
    }

    fn find_incoming_task() -> Option<Task> {
        loop {
            match RUNTIME.incoming_queue.steal() {
                Steal::Success(task) => return Some(task),
                Steal::Retry => continue,
                Steal::Empty => return None,
            }
        }
    }

    // Find next routine to run then execute
    fn poll(worker: NonNull<Self>) {
        let worker_ptr = worker.as_ptr();
        let worker_id = unsafe { (*worker_ptr).id };

        loop {
            let task = {
                let worker = unsafe { &mut *worker_ptr };
                worker.find_task()
            };

            let Some(task) = task else {
                RUNTIME.register_idle(worker_id);

                let task = {
                    let worker = unsafe { &mut *worker_ptr };
                    worker.find_task()
                };

                if let Some(task) = task {
                    RUNTIME.cancel_idle(worker_id);
                    Self::dispatch(worker, task);
                    continue;
                }

                thread::park();
                RUNTIME.cancel_idle(worker_id);
                continue;
            };

            Self::dispatch(worker, task);
        }
    }

    fn dispatch(worker: NonNull<Self>, task: Task) {
        let worker_ptr = worker.as_ptr();
        let (from, to) = {
            let worker = unsafe { &mut *worker_ptr };

            worker.current = Some(task);

            let task = worker.current.as_ref().expect("current task must exist");
            assert!(task.outcome.is_none());

            let routine = task.routine.as_ref().get_ref();
            (&raw mut worker.context, &raw const routine.context)
        };

        switch(from, to);

        let worker = unsafe { &mut *worker_ptr };
        let mut task = worker
            .current
            .take()
            .expect("current task must exist after dispatch");

        match task.outcome.take() {
            Some(RunOutcome::Yielded) => worker.local_queue.push(task),
            Some(RunOutcome::Completed(result)) => {
                drop(task);
                Self::discard_result(result);
            }
            None => panic!("routine returned without an outcome"),
        }
    }

    fn discard_result(result: std::thread::Result<()>) {
        let Err(payload) = result else {
            return;
        };

        // A malicious panic payload may itself panic when dropped. Keep that second panic from
        // terminating a runtime worker; leaking only that pathological payload is the last resort.
        if let Err(secondary_payload) =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(payload)))
        {
            std::mem::forget(secondary_payload);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        rc::Rc,
        sync::{Arc, Mutex, mpsc},
        time::Duration,
    };

    use super::*;

    static GLOBAL_RUNTIME_TEST_LOCK: Mutex<()> = Mutex::new(());

    struct WorkerTlsGuard(Option<NonNull<Worker>>);

    impl Drop for WorkerTlsGuard {
        fn drop(&mut self) {
            WORKER.set(self.0);
        }
    }

    fn run_test_tasks(tasks: Vec<Box<dyn FnOnce() + Send + 'static>>) {
        let local_queue = LocalQueue::new_fifo();
        for task in tasks {
            let routine = RsRoutine::new_pinned(task, crate::context::bootstrap_entry_addr());
            local_queue.push(Task::new(routine));
        }

        let mut worker = Box::new(Worker {
            id: WorkerId(0),
            local_queue,
            tick: 0,
            context: Context::default(),
            current: None,
        });
        let worker_ptr = NonNull::from(worker.as_mut());
        let previous_worker = WORKER.replace(Some(worker_ptr));
        let _guard = WorkerTlsGuard(previous_worker);
        assert!(
            previous_worker.is_none(),
            "nested test runtimes are unsupported"
        );

        while let Some(task) = worker.local_queue.pop() {
            Worker::dispatch(worker_ptr, task);
        }
    }

    fn wait_until_all_workers_idle(timeout: Duration) {
        LazyLock::force(&RUNTIME);
        let deadline = std::time::Instant::now() + timeout;

        while RUNTIME.idle_count() != RUNTIME.worker_threads.len() {
            assert!(
                std::time::Instant::now() < deadline,
                "workers did not become idle before timeout"
            );
            thread::yield_now();
        }
    }

    #[test]
    #[should_panic(expected = "yield_now called outside runtime")]
    fn yield_now_rejects_calls_outside_a_task() {
        yield_now();
    }

    #[test]
    fn tasks_yield_and_resume_in_fifo_order() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let first_events = Arc::clone(&events);
        let second_events = Arc::clone(&events);

        run_test_tasks(vec![
            Box::new(move || {
                first_events.lock().unwrap().push("first-before");
                yield_now();
                first_events.lock().unwrap().push("first-after");
            }),
            Box::new(move || {
                second_events.lock().unwrap().push("second-before");
                yield_now();
                second_events.lock().unwrap().push("second-after");
            }),
        ]);

        assert_eq!(
            *events.lock().unwrap(),
            [
                "first-before",
                "second-before",
                "first-after",
                "second-after"
            ]
        );
    }

    #[test]
    fn yield_preserves_stack_locals() {
        let observed = Arc::new(Mutex::new(None));
        let task_observed = Arc::clone(&observed);

        run_test_tasks(vec![Box::new(move || {
            let mut value = std::hint::black_box(vec![40, 2]);
            yield_now();
            value[0] += 1;
            yield_now();
            *task_observed.lock().unwrap() = Some(value);
        })]);

        assert_eq!(*observed.lock().unwrap(), Some(vec![41, 2]));
    }

    #[test]
    fn panicking_task_does_not_escape_the_routine_entrypoint() {
        let completed = Arc::new(Mutex::new(false));
        let task_completed = Arc::clone(&completed);

        run_test_tasks(vec![
            Box::new(|| panic!("expected task panic")),
            Box::new(move || *task_completed.lock().unwrap() = true),
        ]);

        assert!(*completed.lock().unwrap());
    }

    #[test]
    fn yielded_task_stays_on_its_original_worker() {
        let _guard = GLOBAL_RUNTIME_TEST_LOCK.lock().unwrap();
        wait_until_all_workers_idle(Duration::from_secs(2));

        let (sender, receiver) = mpsc::channel();
        spawn(move || {
            let original_thread = std::thread::current().id();
            let non_send_local = Rc::new(42);

            for _ in 0..32 {
                yield_now();
                assert_eq!(std::thread::current().id(), original_thread);
                assert_eq!(*non_send_local, 42);
            }

            sender.send(()).unwrap();
        });

        receiver.recv_timeout(Duration::from_secs(2)).unwrap();
        wait_until_all_workers_idle(Duration::from_secs(2));
    }

    #[test]
    fn spawn_wakes_a_parked_worker() {
        let _guard = GLOBAL_RUNTIME_TEST_LOCK.lock().unwrap();
        for _ in 0..16 {
            wait_until_all_workers_idle(Duration::from_secs(2));

            let (sender, receiver) = mpsc::channel();
            spawn(move || {
                sender.send("before").unwrap();
                yield_now();
                sender.send("after").unwrap();
            });

            assert_eq!(
                receiver.recv_timeout(Duration::from_secs(2)).unwrap(),
                "before"
            );
            assert_eq!(
                receiver.recv_timeout(Duration::from_secs(2)).unwrap(),
                "after"
            );
        }

        wait_until_all_workers_idle(Duration::from_secs(2));
    }
}
