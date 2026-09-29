use std::{
    cell::Cell,
    collections::{HashMap, VecDeque},
    panic::{AssertUnwindSafe, catch_unwind},
    pin::Pin,
    ptr::NonNull,
    sync::{
        Arc, LazyLock, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    thread,
    thread::available_parallelism,
};

use crossbeam_deque::{Injector as GlobalQueue, Steal, Worker as LocalQueue};

use crate::{
    context::{Context, bootstrap_entry_addr, switch},
    join_handle::{JoinHandle, Packet},
    routine::{RsRoutine, RunnableFn},
};

type WakeQueue = Arc<Mutex<VecDeque<TaskId>>>;

thread_local! {
    static WORKER: Cell<Option<NonNull<Worker>>> = const { Cell::new(None) };
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct TaskId(u64);

impl TaskId {
    pub(crate) fn next() -> Self {
        let raw = NEXT_TASK_ID.fetch_add(1, Ordering::Relaxed);
        Self(raw)
    }
}

static NEXT_TASK_ID: AtomicU64 = AtomicU64::new(1);

enum RunOutcome {
    Yielded,
    Parked,
    Completed,
}

/// `Wrapper around the routine Pin<Box<RsRoutine>>` keeps the routine from moving.
struct Task {
    id: TaskId,
    routine: Pin<Box<RsRoutine>>,
    outcome: Option<RunOutcome>,
}

impl Task {
    fn new(routine: Pin<Box<RsRoutine>>) -> Self {
        let id = TaskId::next();
        Self {
            id,
            routine,
            outcome: None,
        }
    }
}

fn suspend_current(outcome: RunOutcome) {
    let worker_ptr = WORKER.with(|slot| {
        slot.get()
            .expect("yield_now called outside runtime")
            .as_ptr()
    });
    // SAFETY: WORKER was set by this OS thread to its own boxed Worker, which is never freed while
    // `poll` runs. This code runs on a task stack, so the scheduler is suspended inside the
    // `switch` in `dispatch` and holds no reference to the worker; this `&mut` is the only live
    // one, and it ends with this block. `get_unchecked_mut` is used only to take the address of
    // `routine.context`; the routine is not moved.
    let (from, to) = unsafe {
        let worker = &mut *worker_ptr;
        let task = worker.current.as_mut().expect("no running task to suspend");
        assert!(task.outcome.replace(outcome).is_none());
        let routine = task.routine.as_mut().get_unchecked_mut();
        (&raw mut routine.context, &raw const worker.context)
    };
    // SAFETY: `from` is the running task's context inside its pinned, boxed routine, which stays
    // in place until the scheduler resumes or drops the task. `to` is the worker context saved by
    // the `switch` in `dispatch` that started this task, so it points at the live worker stack.
    // No references into the worker are live across the switch.
    unsafe { switch(from, to) };
}

pub(crate) struct WakeHandle {
    worker: WorkerId,
    pub(crate) task: TaskId,
}

pub(crate) fn current_wake_handle() -> Option<WakeHandle> {
    WORKER.with(|worker_slot| {
        // None means this is an external OS thread.
        let worker_pointer = worker_slot.get()?;

        // SAFETY: WORKER points at the boxed Worker owned by this OS thread, which outlives `poll`.
        // This is called from task code, while the scheduler is suspended inside `switch` and
        // holds no `&mut` to the worker, so a short-lived shared reference cannot alias one.
        let worker = unsafe { worker_pointer.as_ref() };

        // WORKER exists while the scheduler is running too, so verify
        // that a green task is currently executing.
        let current_task = worker.current.as_ref()?;

        Some(WakeHandle {
            worker: worker.id,
            task: current_task.id,
        })
    })
}

pub fn yield_now() {
    suspend_current(RunOutcome::Yielded);
}

pub fn park_current() {
    suspend_current(RunOutcome::Parked);
}

pub fn spawn<F, T>(func: F) -> JoinHandle<T>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    let packet = Arc::new(Packet::<T>::new());
    let output_packet = Arc::clone(&packet);
    let runnable: RunnableFn = Box::new(move || {
        let result = catch_unwind(AssertUnwindSafe(func));
        let waiter = output_packet.complete(result);
        if let Some(waiter) = waiter {
            waiter.wake();
        }
    });
    let routine = RsRoutine::new_pinned(runnable, bootstrap_entry_addr());
    let task = Task::new(routine);
    let task_id = task.id;
    RUNTIME.schedule(task);

    JoinHandle {
        task: task_id,
        packet,
    }
}

pub(crate) fn complete_current() -> ! {
    suspend_current(RunOutcome::Completed);
    unreachable!("completed task was resumed");
}

pub(crate) static RUNTIME: LazyLock<Runtime> = LazyLock::new(|| {
    let incoming_queue = GlobalQueue::new();
    let num_workers = available_parallelism()
        .expect("Failed to retrieve available parallelism")
        .into();
    let mut worker_control = Vec::with_capacity(num_workers);
    for i in 0..num_workers {
        let wake_queue = Arc::new(Mutex::new(VecDeque::new()));
        let worker_wake_queue = Arc::clone(&wake_queue);
        let handle = thread::spawn(move || {
            let mut worker = Box::new(Worker::new(WorkerId(i), worker_wake_queue));
            let worker_ptr = NonNull::from(worker.as_mut());
            WORKER.set(Some(worker_ptr));
            // SAFETY: `worker_ptr` points at `worker`, which this thread owns and never touches
            // again, and `poll` never returns.
            unsafe { Worker::poll(worker_ptr) }
        });
        worker_control.push(WorkerControl::new(handle.thread().to_owned(), wake_queue));
    }
    Runtime::new(incoming_queue, worker_control)
});

// Shared wake queue needed for communication between worker and runtime
struct WorkerControl {
    thread: std::thread::Thread,
    wake_queue: WakeQueue,
}

impl WorkerControl {
    pub fn new(thread: std::thread::Thread, wake_queue: WakeQueue) -> Self {
        Self { thread, wake_queue }
    }
}

pub(crate) struct Runtime {
    incoming_queue: GlobalQueue<Task>,
    worker_control: Vec<WorkerControl>,
    idle_workers: Mutex<IdleWorkers>,
}

impl Runtime {
    fn new(incoming_queue: GlobalQueue<Task>, worker_control: Vec<WorkerControl>) -> Self {
        let worker_count = worker_control.len();
        Self {
            incoming_queue,
            worker_control,
            idle_workers: Mutex::new(IdleWorkers::new(worker_count)),
        }
    }

    fn schedule(&self, task: Task) {
        let worker_to_wake = {
            let mut idle_workers = self.idle_workers.lock().expect("idle worker lock poisoned");
            self.incoming_queue.push(task);
            idle_workers
                .take_one()
                .map(|id| self.worker_control[id.0].thread.clone())
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

    pub(crate) fn wake(&self, handle: WakeHandle) {
        let worker = self
            .worker_control
            .get(handle.worker.0)
            .expect("Invalid worker ID");
        worker.wake_queue.lock().unwrap().push_back(handle.task);
        worker.thread.unpark();
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct WorkerId(usize);

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
    parked_tasks: HashMap<TaskId, Task>,
    wake_queue: Arc<Mutex<VecDeque<TaskId>>>,
}

impl Worker {
    fn new(id: WorkerId, wake_queue: WakeQueue) -> Self {
        Self {
            id,
            local_queue: LocalQueue::new_fifo(),
            tick: 0,
            context: Context::default(),
            current: None,
            parked_tasks: HashMap::new(),
            wake_queue,
        }
    }

    fn find_task(&mut self) -> Option<Task> {
        self.drain_wakes();
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

    // Move all tasks to be awaken into the local queue
    fn drain_wakes(&mut self) {
        let woken = std::mem::take(&mut *self.wake_queue.lock().expect("wake queue lock poisoned"));
        for task in woken {
            if let Some(parked_task) = self.parked_tasks.remove(&task) {
                self.local_queue.push(parked_task);
            }
        }
    }

    fn wait_for_task(&mut self) -> Task {
        loop {
            if let Some(task) = self.find_task() {
                return task;
            }

            RUNTIME.register_idle(self.id);
            let task = self.find_task();
            if task.is_none() {
                thread::park();
            }
            RUNTIME.cancel_idle(self.id);

            if let Some(task) = task {
                return task;
            }
        }
    }

    /// # Safety
    ///
    /// `worker` must point at a live Worker owned by the calling thread and stored in its
    /// `WORKER`, and nothing else may access it except task code through `WORKER`.
    unsafe fn poll(worker: NonNull<Self>) -> ! {
        loop {
            // SAFETY: No task runs while `wait_for_task` runs, and the reference ends with this
            // statement, before `dispatch` switches.
            let task = unsafe { &mut *worker.as_ptr() }.wait_for_task();
            // SAFETY: Forwarded from this function's contract.
            unsafe { Self::dispatch(worker, task) };
        }
    }

    /// Runs `task` until it yields, parks, or completes, then files it accordingly.
    ///
    /// # Safety
    ///
    /// Same contract as `poll`, and the caller must not hold a reference to the worker.
    unsafe fn dispatch(worker: NonNull<Self>, task: Task) {
        let worker_ptr = worker.as_ptr();
        let (from, to) = {
            // SAFETY: No task is running and the caller holds no reference to the worker, so this
            // is the only one. It ends with this block, before the switch.
            let worker = unsafe { &mut *worker_ptr };
            let task = worker.current.insert(task);
            assert!(task.outcome.is_none());
            let routine = task.routine.as_ref().get_ref();
            (&raw mut worker.context, &raw const routine.context)
        };

        // SAFETY: `from` is the worker context inside the boxed Worker, which outlives this call.
        // `to` is the context of the task now stored in `worker.current`; its routine is pinned in
        // a Box and its stack stays mapped until the task is dropped below. That context was
        // either built by `Context::new_routine` or saved by the task's own `switch` in
        // `suspend_current`. No references to the worker are live across the switch.
        unsafe { switch(from, to) };

        // SAFETY: The task has switched back to us, and the `&mut` that `suspend_current` created
        // ended before its switch, so this is again the only reference to the worker.
        let worker = unsafe { &mut *worker_ptr };
        let mut task = worker
            .current
            .take()
            .expect("current task must exist after dispatch");

        match task.outcome.take() {
            Some(RunOutcome::Yielded) => worker.local_queue.push(task),
            Some(RunOutcome::Parked) => {
                worker.parked_tasks.insert(task.id, task);
            }
            Some(RunOutcome::Completed) => drop(task),
            None => panic!("routine returned without an outcome"),
        }
    }
}
