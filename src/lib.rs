mod context;
mod routine;
mod runtime;
mod stack;

pub use runtime::yield_now;

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;

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

        runtime::run_test_tasks(vec![
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

        runtime::run_test_tasks(vec![Box::new(move || {
            let mut value = std::hint::black_box(vec![40, 2]);
            yield_now();
            value[0] += 1;
            yield_now();
            *task_observed.lock().unwrap() = Some(value);
        })]);

        assert_eq!(*observed.lock().unwrap(), Some(vec![41, 2]));
    }
}
