use crossbeam_deque::Injector as GlobalQueue;
use crossbeam_deque::Steal;
use crossbeam_deque::Stealer;
use crossbeam_deque::Worker as LocalQueue;
use rand::random_range;
use std::cell::Cell;
use std::pin::Pin;
use std::ptr::NonNull;
use std::sync::LazyLock;
use std::thread;

use crate::context::Context;
use crate::context::switch;
use crate::routine::RsRoutine;

thread_local! {
    static WORKER: Cell<Option<NonNull<Worker>>> = Cell::new(None);
}

enum RunOutcome {
    Yielded,
    /// ParkRequest(ParkRequest)
    /// TODO: Completed(Result<T, E>)
    Completed,
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
    let mut stealers = Vec::with_capacity(num_workers);
    for i in 0..num_workers {
        let local_queue = LocalQueue::new_fifo();
        stealers.push(local_queue.stealer());
        thread::spawn(move || {
            let mut worker = Box::new(Worker {
                id: WorkerId(i),
                local_queue,
                context: Context::default(),
                current: None,
            });
            let worker_ptr = NonNull::from(worker.as_mut());
            WORKER.set(Some(worker_ptr));
            Worker::poll(worker_ptr);
            // if worker ever quits we should clean the pointer
            WORKER.set(None);
        });
    }
    Runtime::new(num_workers, incoming_queue, stealers)
});

struct Runtime {
    worker_count: usize,
    incoming_queue: GlobalQueue<Task>,
    stealers: Vec<Stealer<Task>>,
}

impl Runtime {
    fn new(
        worker_count: usize,
        incoming_queue: GlobalQueue<Task>,
        stealers: Vec<Stealer<Task>>,
    ) -> Self {
        Self {
            worker_count,
            incoming_queue,
            stealers,
        }
    }
}

struct WorkerId(usize);

struct Worker {
    id: WorkerId,
    local_queue: LocalQueue<Task>,
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

pub(crate) fn complete_current() -> ! {
    suspend_current(RunOutcome::Completed);
    unreachable!("completed task was resumed");
}

impl Worker {
    fn find_task(&mut self) -> Option<Task> {
        if let Some(task) = self.local_queue.pop() {
            return Some(task);
        }

        loop {
            match RUNTIME
                .incoming_queue
                .steal_batch_and_pop(&self.local_queue)
            {
                Steal::Success(task) => return Some(task),
                Steal::Retry => continue,
                Steal::Empty => {}
            }

            match self.steal_from_workers() {
                Steal::Success(task) => return Some(task),
                Steal::Retry => continue,
                Steal::Empty => return None,
            }
        }
    }

    fn steal_from_workers(&self) -> Steal<Task> {
        let len = RUNTIME.stealers.len();
        if len <= 1 {
            return Steal::Empty;
        }

        let start = random_range(0..len);
        let worker_id = self.id.0;
        let (left, right) = RUNTIME.stealers.split_at(start);

        right
            .iter()
            .chain(left.iter())
            .enumerate()
            .filter_map(|(offset, stealer)| {
                let index = (start + offset) % len;
                (index != worker_id).then(|| stealer.steal_batch_and_pop(&self.local_queue))
            })
            .collect()
    }

    // Find next routine to run then execute
    fn poll(worker: NonNull<Self>) {
        let worker_ptr = worker.as_ptr();

        loop {
            let task = {
                let worker = unsafe { &mut *worker_ptr };
                worker.find_task()
            };

            let Some(task) = task else {
                thread::park();
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
            Some(RunOutcome::Completed) => drop(task),
            None => panic!("routine returned without an outcome"),
        }
    }
}

#[cfg(test)]
pub(crate) fn run_test_tasks(tasks: Vec<Box<dyn FnOnce() + Send + 'static>>) {
    struct WorkerTlsGuard(Option<NonNull<Worker>>);

    impl Drop for WorkerTlsGuard {
        fn drop(&mut self) {
            WORKER.set(self.0);
        }
    }

    let local_queue = LocalQueue::new_fifo();
    for task in tasks {
        let routine = RsRoutine::new_pinned(task, crate::context::bootstrap_entry_addr());
        local_queue.push(Task::new(routine));
    }

    let mut worker = Box::new(Worker {
        id: WorkerId(0),
        local_queue,
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
