use std::{
    sync::{Arc, Mutex},
    thread,
};

use crate::runtime::{RUNTIME, TaskId, WakeHandle, current_wake_handle, park_current};

pub(crate) enum Waiter {
    Green(WakeHandle),
    External(std::thread::Thread),
}

impl Waiter {
    pub(crate) fn wake(self) {
        match self {
            Waiter::External(thread) => thread.unpark(),
            Waiter::Green(wake_handle) => RUNTIME.wake(wake_handle),
        }
    }
}

enum JoinState<T> {
    Running { waiter: Option<Waiter> },
    Returned { output: T, waiter: Option<Waiter> },
    Ready(std::thread::Result<T>),
    Taken,
}

pub enum RegisterOutcome<T> {
    Ready(std::thread::Result<T>),
    Park,
}

pub struct Packet<T> {
    state: Mutex<JoinState<T>>,
}

impl<T> Packet<T> {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(JoinState::Running { waiter: None }),
        }
    }

    pub fn store_output(&self, output: T) {
        let mut state = self.state.lock().unwrap();

        match &mut *state {
            JoinState::Running { waiter } => {
                let waiter = waiter.take();

                *state = JoinState::Returned { output, waiter };
            }

            _ => panic!("output stored in invalid state"),
        }
    }

    fn register_waiter(&self, new_waiter: Waiter) -> RegisterOutcome<T> {
        let mut state = self.state.lock().unwrap();

        match &mut *state {
            JoinState::Running { waiter } | JoinState::Returned { waiter, .. } => {
                assert!(
                    waiter.replace(new_waiter).is_none(),
                    "join waiter registered twice"
                );

                RegisterOutcome::Park
            }

            JoinState::Ready(_) => {
                let previous = std::mem::replace(&mut *state, JoinState::Taken);

                let JoinState::Ready(result) = previous else {
                    unreachable!();
                };

                RegisterOutcome::Ready(result)
            }

            JoinState::Taken => {
                panic!("task result already taken")
            }
        }
    }

    pub(crate) fn finish(&self, runtime_result: std::thread::Result<()>) -> Option<Waiter> {
        let mut state = self.state.lock().unwrap();

        // Needed because we cannot move T out through a MutexGuard
        let previous = std::mem::replace(&mut *state, JoinState::Taken);

        let (result, waiter) = match (previous, runtime_result) {
            (JoinState::Returned { output, waiter }, Ok(())) => (Ok(output), waiter),

            (JoinState::Running { waiter }, Err(payload)) => (Err(payload), waiter),

            (JoinState::Running { .. }, Ok(())) => {
                panic!("task completed without storing output")
            }

            (JoinState::Returned { .. }, Err(_)) => {
                panic!("task panicked after storing output")
            }

            (JoinState::Ready(_), _) => {
                panic!("task finished twice")
            }

            (JoinState::Taken, _) => {
                panic!("task finished after result was taken")
            }
        };

        *state = JoinState::Ready(result);

        waiter
    }

    fn try_take_result(&self) -> Option<std::thread::Result<T>> {
        let mut state = self.state.lock().unwrap();

        match &*state {
            JoinState::Running { .. } | JoinState::Returned { .. } => {
                return None;
            }

            JoinState::Taken => {
                panic!("task result already taken");
            }

            JoinState::Ready(_) => {}
        }

        let previous = std::mem::replace(&mut *state, JoinState::Taken);

        let JoinState::Ready(result) = previous else {
            unreachable!();
        };

        Some(result)
    }
}

pub struct JoinHandle<T> {
    pub(crate) task: TaskId,
    pub(crate) packet: Arc<Packet<T>>,
}

impl<T> JoinHandle<T> {
    pub fn join(self) -> std::thread::Result<T> {
        if let Some(current) = current_wake_handle() {
            // Prevent task A from waiting on itself.
            if current.task == self.task {
                panic!("task attempted to join itself");
            }

            match self.packet.register_waiter(Waiter::Green(current)) {
                RegisterOutcome::Ready(result) => result,

                RegisterOutcome::Park => {
                    park_current();

                    // Execution resumes here after the owner worker
                    // moves this task back into its local queue.
                    self.packet
                        .try_take_result()
                        .expect("woken joiner must have a result")
                }
            }
        } else {
            // External OS-thread join path.
            match self
                .packet
                .register_waiter(Waiter::External(thread::current()))
            {
                RegisterOutcome::Ready(result) => result,
                RegisterOutcome::Park => loop {
                    std::thread::park();

                    if let Some(result) = self.packet.try_take_result() {
                        break result;
                    }
                },
            }
        }
    }
}
