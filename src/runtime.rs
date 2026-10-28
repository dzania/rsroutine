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
    idle::{IdleWorkers, WORKER_COUNT_MAX},
    join_handle::{JoinHandle, Packet},
    routine::{RsRoutine, RunnableFn},
    routine_pool::RoutinePool,
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

enum TaskBody {
    // Not dispatched yet. The worker attaches a routine from its pool on first dispatch, so
    // queued tasks hold no stack and stacks are taken and returned on the same thread.
    Pending(RunnableFn),
    // `Pin<Box<RsRoutine>>` keeps the routine from moving while it has a live context.
    Started(Pin<Box<RsRoutine>>),
}

struct Task {
    id: TaskId,
    body: TaskBody,
    outcome: Option<RunOutcome>,
}

impl Task {
    fn new(func: RunnableFn) -> Self {
        let id = TaskId::next();
        Self {
            id,
            body: TaskBody::Pending(func),
            outcome: None,
        }
    }

    fn into_started(self, routine_pool: &mut RoutinePool) -> Self {
        match self.body {
            TaskBody::Started(_) => self,
            TaskBody::Pending(func) => {
                let mut routine = routine_pool.acquire();
                routine.as_mut().start(func, bootstrap_entry_addr());
                Self {
                    id: self.id,
                    body: TaskBody::Started(routine),
                    outcome: self.outcome,
                }
            }
        }
    }

    fn routine(&self) -> &RsRoutine {
        match &self.body {
            TaskBody::Started(routine) => routine.as_ref().get_ref(),
            TaskBody::Pending(_) => panic!("task has no routine before first dispatch"),
        }
    }

    fn routine_mut(&mut self) -> Pin<&mut RsRoutine> {
        match &mut self.body {
            TaskBody::Started(routine) => routine.as_mut(),
            TaskBody::Pending(_) => panic!("task has no routine before first dispatch"),
        }
    }

    fn into_routine(self) -> Pin<Box<RsRoutine>> {
        match self.body {
            TaskBody::Started(routine) => routine,
            TaskBody::Pending(_) => panic!("task has no routine before first dispatch"),
        }
    }
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
        let routine = task.routine_mut().get_unchecked_mut();
        (&raw mut routine.context, &raw const worker.context)
    };
    switch(from, to);
}

pub(crate) struct WakeHandle {
    worker: WorkerId,
    pub(crate) task: TaskId,
}

pub(crate) fn current_wake_handle() -> Option<WakeHandle> {
    WORKER.with(|worker_slot| {
        // None means this is an external OS thread.
        let worker_pointer = worker_slot.get()?;

        // SAFETY: WORKER points at the Worker owned by this OS thread.
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
    let task = Task::new(runnable);
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
    let num_workers =
        usize::from(available_parallelism().expect("Failed to retrieve available parallelism"))
            .min(WORKER_COUNT_MAX);
    let mut worker_control = Vec::with_capacity(num_workers);
    for i in 0..num_workers {
        let wake_queue = Arc::new(Mutex::new(VecDeque::new()));
        let worker_wake_queue = Arc::clone(&wake_queue);
        let handle = thread::spawn(move || {
            let mut worker = Box::new(Worker::new(WorkerId(i), worker_wake_queue));
            let worker_ptr = NonNull::from(worker.as_mut());
            WORKER.set(Some(worker_ptr));
            Worker::poll(worker_ptr);
            // if worker ever quits we should clean the pointer
            WORKER.set(None);
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
    idle_workers: IdleWorkers,
}

impl Runtime {
    fn new(incoming_queue: GlobalQueue<Task>, worker_control: Vec<WorkerControl>) -> Self {
        let worker_count = worker_control.len();
        Self {
            incoming_queue,
            worker_control,
            idle_workers: IdleWorkers::new(worker_count),
        }
    }

    fn schedule(&self, task: Task) {
        self.incoming_queue.push(task);
        self.notify_one();
    }

    // Unparks one parked worker if nobody is searching. Most calls return after one atomic
    // load, without locking or a system call.
    fn notify_one(&self) {
        if let Some(worker_id) = self.idle_workers.worker_to_notify() {
            self.worker_control[worker_id.0].thread.unpark();
        }
    }

    fn has_incoming(&self) -> bool {
        !self.incoming_queue.is_empty()
    }

    // Unparks the owning worker without touching `idle_workers`. If the worker is parked, it
    // finds itself still in the sleeper list on waking and fixes the counts in
    // `finish_parking`. If it is awake, the unpark token makes its next `park` return at once.
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
pub(crate) struct WorkerId(pub(crate) usize);

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
    routine_pool: RoutinePool,
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
            routine_pool: RoutinePool::new(),
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

    // Find next routine to run then execute
    fn poll(worker: NonNull<Self>) {
        let worker_ptr = worker.as_ptr();
        let worker_id = unsafe { (*worker_ptr).id };
        // Mirrors whether `idle_workers` counts this worker as searching. A worker woken by
        // `worker_to_notify` starts out searching.
        let mut is_searching = false;

        loop {
            let task = {
                let worker = unsafe { &mut *worker_ptr };
                worker.find_task()
            };

            // Only a searcher's scan of the injector counts as having looked before parking.
            let task = match task {
                Some(task) => Some(task),
                None => {
                    is_searching = is_searching || RUNTIME.idle_workers.try_start_searching();
                    if is_searching {
                        Self::find_incoming_task()
                    } else {
                        None
                    }
                }
            };

            let Some(task) = task else {
                is_searching = Self::park(worker_id, is_searching);
                continue;
            };

            if is_searching {
                is_searching = false;
                // Spawns that saw this worker searching skipped their wake. If it was the last
                // searcher, wake another worker so the rest of that work is not left waiting.
                if RUNTIME.idle_workers.stop_searching() {
                    RUNTIME.notify_one();
                }
            }

            Self::dispatch(worker, task);
        }
    }

    // Parks the thread until a spawner or a task wake unparks it. Returns whether the worker
    // woke up counted as searching.
    fn park(worker_id: WorkerId, is_searching: bool) -> bool {
        let is_last_searcher = RUNTIME.idle_workers.start_parking(worker_id, is_searching);
        // Spawners that pushed while this worker searched skipped the wake and relied on it
        // seeing their task, so the last searcher must look once more.
        if is_last_searcher && RUNTIME.has_incoming() {
            return RUNTIME.idle_workers.finish_parking(worker_id);
        }

        thread::park();
        RUNTIME.idle_workers.finish_parking(worker_id)
    }

    fn dispatch(worker: NonNull<Self>, task: Task) {
        let worker_ptr = worker.as_ptr();
        let (from, to) = {
            let worker = unsafe { &mut *worker_ptr };
            let task = task.into_started(&mut worker.routine_pool);
            let task = worker.current.insert(task);
            assert!(task.outcome.is_none());
            let routine = task.routine();
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
            Some(RunOutcome::Parked) => {
                worker.parked_tasks.insert(task.id, task);
            }
            // The routine stopped inside `complete_current` and is never resumed, so its stack
            // is free to run the next task on this worker.
            Some(RunOutcome::Completed) => worker.routine_pool.release(task.into_routine()),
            None => panic!("routine returned without an outcome"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routine_pool::ROUTINE_POOL_CAPACITY;

    // Each batch outlives the pool, so later batches run on reused stacks and on fresh ones.
    #[test]
    fn tasks_run_correctly_on_reused_routines() {
        let task_count = ROUTINE_POOL_CAPACITY * 4;
        let parent = spawn(move || {
            let mut total = 0;
            for batch in 0..8 {
                let children: Vec<_> = (0..task_count)
                    .map(|index| {
                        spawn(move || {
                            yield_now();
                            batch * task_count + index
                        })
                    })
                    .collect();
                for child in children {
                    total += child.join().expect("child task must not panic");
                }
            }
            total
        });

        let expected: usize = (0..8 * task_count).sum();
        assert_eq!(parent.join().expect("parent task must not panic"), expected);
    }

    // Each join from this OS thread waits long enough for idle workers to park, so every spawn
    // has to wake one. A lost wakeup hangs this test.
    #[test]
    fn spawn_wakes_parked_workers() {
        for index in 0..2_000_u32 {
            let task = spawn(move || index);
            assert_eq!(task.join().expect("task must not panic"), index);
        }
    }

    // Many OS threads spawn at once while workers keep going idle, so notifies, searches, and
    // parks race with each other.
    #[test]
    fn concurrent_spawners_all_complete() {
        let spawners: Vec<_> = (0..8_u64)
            .map(|spawner| {
                std::thread::spawn(move || {
                    let mut total = 0;
                    for index in 0..500_u64 {
                        total += spawn(move || spawner * 500 + index)
                            .join()
                            .expect("task must not panic");
                    }
                    total
                })
            })
            .collect();

        let total: u64 = spawners
            .into_iter()
            .map(|spawner| spawner.join().expect("spawner thread must not panic"))
            .sum();
        assert_eq!(total, (0..8 * 500).sum::<u64>());
    }

    // A panicking task still completes through `complete_current`, so its routine goes back to
    // the pool and must run the next task normally.
    #[test]
    fn panicked_task_routine_is_reusable() {
        for _ in 0..ROUTINE_POOL_CAPACITY * 2 {
            let failed = spawn(|| -> u32 { panic!("boom") });
            assert!(failed.join().is_err());

            let ok = spawn(|| 7_u32);
            assert_eq!(ok.join().expect("task must not panic"), 7);
        }
    }
}
