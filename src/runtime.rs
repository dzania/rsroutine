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
            let mut worker = Worker {
                id: WorkerId(i),
                local_queue,
                context: Context::default(),
                current: None,
            };
            WORKER.set(Some(NonNull::from(&mut worker)));
            worker.poll();
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

    fn yield_now(&mut self) {
        let mut task = self.current.take().expect("No task to yield");
        let to = &raw mut self.context;
        task.outcome = None;
        // TODO: Encapsulate this inside RsRoutine
        let from = unsafe { task.routine.as_mut().get_unchecked_mut() };
        switch(&raw mut from.context, to);
        task.outcome = Some(RunOutcome::Yielded)
    }

    // Find next routine to run then execvute
    fn poll(&mut self) {
        loop {
            let Some(task) = self.find_task() else {
                // TODO: Park here whatever da fuck it means
                continue;
            };

            self.current = Some(task);

            let to = {
                let task = self
                    .current
                    .as_ref()
                    .expect("current task must exist during dispatch");

                let routine = task.routine.as_ref().get_ref();
                &raw const routine.context
            };

            let from = &raw mut self.context;

            switch(from, to);

            // Execution reaches here only after the routine switches back.
            let mut task = self
                .current
                .take()
                .expect("current task must exist after dispatch");

            match task.outcome.take() {
                Some(RunOutcome::Yielded) => self.local_queue.push(task),
                Some(RunOutcome::Completed) => drop(task),
                None => panic!("routine switched back without reporting an outcome"),
            }
        }
    }
}
