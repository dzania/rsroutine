use crate::scheduler::{CURRENT_SCHEDULER, Scheduler};

mod context;
mod func;
mod routine;
mod scheduler;
mod stack;

pub fn run<F>(func: F)
where
    F: FnOnce() + Send + 'static,
{
    let mut scheduler = Scheduler::new();
    scheduler.spawn(Box::new(func));
    scheduler.run();
}

pub fn yield_now() {
    let mut current_scheduler = CURRENT_SCHEDULER.get().unwrap();
    unsafe {
        current_scheduler.as_mut().yield_current();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    };

    #[test]
    fn run_executes_function_once() {
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_for_routine = Arc::clone(&calls);

        run(move || {
            calls_for_routine.fetch_add(1, Ordering::SeqCst);
        });

        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn run_allows_function_to_yield_and_resume() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let events_for_routine = Arc::clone(&events);

        run(move || {
            events_for_routine
                .lock()
                .expect("event log mutex must not be poisoned")
                .push("before yield");
            yield_now();
            events_for_routine
                .lock()
                .expect("event log mutex must not be poisoned")
                .push("after yield");
        });

        let events = events.lock().expect("event log mutex must not be poisoned");
        assert_eq!(&*events, &["before yield", "after yield"]);
    }

    #[test]
    #[should_panic(expected = "called `Option::unwrap()` on a `None` value")]
    fn yield_now_panics_outside_scheduler() {
        yield_now();
    }
}
