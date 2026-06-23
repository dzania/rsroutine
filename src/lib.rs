use crate::scheduler::Scheduler;

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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Arc,
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
}
