use crossbeam_deque::Injector as GlobalQueue;
use crossbeam_deque::Steal;
use crossbeam_deque::Stealer;
use crossbeam_deque::Worker as LocalQueue;
use rand::random_range;
use std::cell::Cell;
use std::ptr::NonNull;
use std::sync::LazyLock;
use std::thread;
use std::thread::sleep;
use std::time::Duration;

use crate::context::Context;
use crate::context::switch;
use crate::routine::RoutineState;
use crate::routine::RsRoutine;

thread_local! {
    static WORKER: Cell<Option<NonNull<Worker>>> = Cell::new(None);
}

static RUNTIME: LazyLock<Runtime> = LazyLock::new(|| {
    let incoming_queue: GlobalQueue<RsRoutine> = GlobalQueue::new();
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
    incoming_queue: GlobalQueue<RsRoutine>,
    stealers: Vec<Stealer<RsRoutine>>,
}

impl Runtime {
    fn new(
        worker_count: usize,
        incoming_queue: GlobalQueue<RsRoutine>,
        stealers: Vec<Stealer<RsRoutine>>,
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
    local_queue: LocalQueue<RsRoutine>,
    // Worker context
    context: Context,
    current: Option<RsRoutine>,
}

impl Worker {
    fn find_routine(&mut self) -> Option<RsRoutine> {
        if let Some(routine) = self.local_queue.pop() {
            return Some(routine);
        }

        loop {
            match RUNTIME
                .incoming_queue
                .steal_batch_and_pop(&self.local_queue)
            {
                Steal::Success(routine) => return Some(routine),
                Steal::Retry => continue,
                Steal::Empty => {}
            }

            match self.steal_from_workers() {
                Steal::Success(routine) => return Some(routine),
                Steal::Retry => continue,
                Steal::Empty => return None,
            }
        }
    }

    fn steal_from_workers(&self) -> Steal<RsRoutine> {
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

    // Find next routine to run
    fn poll(&mut self) {
        loop {
            if let Some(mut routine) = self.find_routine() {
                routine.set_state(RoutineState::Running);
                self.current = Some(routine);
                /// FIXME: UB
                unsafe {
                    debug_assert!(self.current.is_some());
                    let routine = self.current.as_ref().expect("Expected routine");
                    switch(&mut self.context, &routine.context);
                };
                let routine = self.current.take();
                debug_assert!(routine.is_some());
                match routine.as_ref().unwrap().state() {
                    RoutineState::Runnable => {
                        self.local_queue.push(routine.unwrap());
                    }
                    RoutineState::Finished => {
                        drop(routine);
                    }
                    RoutineState::Running => {
                        unreachable!()
                    }
                    RoutineState::Parked => {}
                }
            } else {
                // FIXME: Yield thread here or park
                sleep(Duration::from_millis(100));
                continue;
            }
        }
    }
}
