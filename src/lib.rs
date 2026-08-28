mod context;
mod routine;
mod runtime;
mod stack;

pub use runtime::{spawn, yield_now};

#[cfg(test)]
mod tests {
    use std::{
        rc::Rc,
        sync::{Arc, Mutex, mpsc},
        thread,
        time::Duration,
    };

    use super::*;

    static GLOBAL_RUNTIME_TEST_LOCK: Mutex<()> = Mutex::new(());

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

    #[test]
    fn panicking_task_does_not_escape_the_routine_entrypoint() {
        let completed = Arc::new(Mutex::new(false));
        let task_completed = Arc::clone(&completed);

        runtime::run_test_tasks(vec![
            Box::new(|| panic!("expected task panic")),
            Box::new(move || *task_completed.lock().unwrap() = true),
        ]);

        assert!(*completed.lock().unwrap());
    }

    #[test]
    fn yielded_task_stays_on_its_original_worker() {
        let _guard = GLOBAL_RUNTIME_TEST_LOCK.lock().unwrap();
        runtime::wait_until_all_workers_idle(Duration::from_secs(2));

        let (sender, receiver) = mpsc::channel();
        spawn(move || {
            let original_thread = thread::current().id();
            let non_send_local = Rc::new(42);

            for _ in 0..32 {
                yield_now();
                assert_eq!(thread::current().id(), original_thread);
                assert_eq!(*non_send_local, 42);
            }

            sender.send(()).unwrap();
        });

        receiver.recv_timeout(Duration::from_secs(2)).unwrap();
        runtime::wait_until_all_workers_idle(Duration::from_secs(2));
    }

    #[test]
    fn spawn_wakes_a_parked_worker() {
        let _guard = GLOBAL_RUNTIME_TEST_LOCK.lock().unwrap();
        for _ in 0..16 {
            runtime::wait_until_all_workers_idle(Duration::from_secs(2));

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

        runtime::wait_until_all_workers_idle(Duration::from_secs(2));
    }
}
