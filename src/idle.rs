//! Tracks which workers are awake, which are searching for work, and which are parked.
//!
//! A worker is *unparked* while its thread is awake and *searching* while it is awake, has no
//! task, and is scanning the queues. Both counts live in one atomic word so the spawn path can
//! decide whether to wake anyone with a single load and no lock:
//!
//! ```text
//!  bits 63..16       bits 15..0
//! [ unparked_count ][ searching_count ]
//! ```
//!
//! The protocol, after tokio's multi-thread scheduler:
//!
//! - A spawner wakes a parked worker only when nobody is searching. A searching worker will see
//!   the new task, so waking another one would be wasted.
//! - A woken worker starts out counted as searching, so a burst of spawns wakes one worker,
//!   not one per spawn.
//! - When the last searcher finds a task, it wakes one more worker. Wakes chain one at a time
//!   and stop on their own when the work runs out.
//! - When the last searcher parks, it re-checks the injector. Spawners that pushed while it was
//!   searching skipped the wake and relied on it seeing their task.
//!
//! Lost wakeups are a store-buffering race: the spawner writes the queue and reads this state,
//! and a parking worker writes this state and reads the queue. Each side has a `fence(SeqCst)`
//! between its write and its read (`should_wake` and `start_parking`), which guarantees at
//! least one of them sees the other's write. Without either fence, the loom test below finds an
//! interleaving where the task is stranded.
//!
//! The unparked count and the sleeper list only change together under the `sleepers` lock, so
//! `unparked_count < worker_count` exactly when the list is non-empty. The searching count
//! changes without the lock.

#[cfg(loom)]
use loom::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering, fence},
};
#[cfg(not(loom))]
use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering, fence},
};

use crate::runtime::WorkerId;

const UNPARKED_SHIFT: u32 = 16;
const SEARCHING_MASK: usize = (1 << UNPARKED_SHIFT) - 1;
const ONE_SEARCHING: usize = 1;
const ONE_UNPARKED: usize = 1 << UNPARKED_SHIFT;

/// Highest worker count the packed state can represent without the searching count spilling
/// into the unparked count.
pub(crate) const WORKER_COUNT_MAX: usize = SEARCHING_MASK;

const _: () = assert!(usize::BITS > UNPARKED_SHIFT);

pub(crate) struct IdleWorkers {
    state: AtomicUsize,
    // Parked workers. Locked only when a worker parks or wakes, or a spawner must wake one.
    sleepers: Mutex<Vec<WorkerId>>,
    worker_count: usize,
}

impl IdleWorkers {
    /// Starts with every worker unparked and none searching.
    ///
    /// # Panics
    ///
    /// Panics if `worker_count` is zero or above `WORKER_COUNT_MAX`.
    pub(crate) fn new(worker_count: usize) -> Self {
        assert!(worker_count > 0, "runtime needs at least one worker");
        assert!(worker_count <= WORKER_COUNT_MAX, "too many workers");

        Self {
            state: AtomicUsize::new(worker_count << UNPARKED_SHIFT),
            sleepers: Mutex::new(Vec::with_capacity(worker_count)),
            worker_count,
        }
    }

    /// Called by a spawner after pushing to the injector. Returns the parked worker to unpark,
    /// already counted as unparked and searching, or `None` when no wake is needed.
    pub(crate) fn worker_to_notify(&self) -> Option<WorkerId> {
        if !self.should_wake() {
            return None;
        }

        let mut sleepers = self.sleepers.lock().expect("sleeper lock poisoned");
        // Another spawner may have woken a worker between the check above and the lock.
        if !self.should_wake() {
            return None;
        }

        let worker_id = sleepers
            .pop()
            .expect("unparked count below worker count implies a parked worker");
        self.state
            .fetch_add(ONE_UNPARKED | ONE_SEARCHING, Ordering::SeqCst);
        Some(worker_id)
    }

    /// Tries to count the calling worker as searching. Fails when half the workers already
    /// search, since more searchers only add contention on the injector.
    pub(crate) fn try_start_searching(&self) -> bool {
        let state = self.state.load(Ordering::SeqCst);
        if 2 * searching_count(state) >= self.worker_count {
            return false;
        }

        // The load and add are not one atomic step, so the limit can be overshot by a few
        // concurrent workers. That only costs contention, never correctness.
        //
        // No fence here: a scan that misses a fresh task is caught by the re-check in
        // `start_parking`. The loom test passes without a fence here.
        self.state.fetch_add(ONE_SEARCHING, Ordering::SeqCst);
        true
    }

    /// Called by a searching worker that found a task. Returns true when it was the last
    /// searcher, in which case the caller must notify another worker.
    pub(crate) fn stop_searching(&self) -> bool {
        let previous = self.state.fetch_sub(ONE_SEARCHING, Ordering::SeqCst);
        assert!(searching_count(previous) > 0, "searching count underflow");
        searching_count(previous) == 1
    }

    /// Registers the calling worker as parked. Returns true when it was the last searcher, in
    /// which case the caller must check the injector once more before parking the thread.
    ///
    /// On return, the caller's later loads of the injector are ordered after this change.
    pub(crate) fn start_parking(&self, worker_id: WorkerId, is_searching: bool) -> bool {
        let mut sleepers = self.sleepers.lock().expect("sleeper lock poisoned");
        assert!(!sleepers.contains(&worker_id), "worker parked twice");

        let decrement = if is_searching {
            ONE_UNPARKED | ONE_SEARCHING
        } else {
            ONE_UNPARKED
        };
        let previous = self.state.fetch_sub(decrement, Ordering::SeqCst);
        assert!(unparked_count(previous) > 0, "unparked count underflow");
        assert!(
            !is_searching || searching_count(previous) > 0,
            "searching count underflow"
        );
        sleepers.push(worker_id);
        drop(sleepers);

        // Pairs with the fence in `should_wake`; see the module docs.
        fence(Ordering::SeqCst);
        is_searching && searching_count(previous) == 1
    }

    /// Called by a worker after its thread wakes, or when it abandons parking. Returns whether
    /// the worker is now counted as searching.
    ///
    /// A worker still in the sleeper list woke for another reason: a direct wake of one of its
    /// own tasks, a spurious return from `park`, or a cancelled park. It removes itself and
    /// counts as unparked, not searching. A worker missing from the list was claimed by
    /// `worker_to_notify`, which already counted it as unparked and searching.
    pub(crate) fn finish_parking(&self, worker_id: WorkerId) -> bool {
        let mut sleepers = self.sleepers.lock().expect("sleeper lock poisoned");
        let Some(position) = sleepers.iter().position(|id| *id == worker_id) else {
            return true;
        };

        sleepers.swap_remove(position);
        self.state.fetch_add(ONE_UNPARKED, Ordering::SeqCst);
        false
    }

    fn should_wake(&self) -> bool {
        // Orders the caller's earlier injector push before this load; see the module docs.
        fence(Ordering::SeqCst);
        let state = self.state.load(Ordering::SeqCst);
        searching_count(state) == 0 && unparked_count(state) < self.worker_count
    }

    #[cfg(all(test, not(loom)))]
    fn counts(&self) -> (usize, usize) {
        let state = self.state.load(Ordering::SeqCst);
        (unparked_count(state), searching_count(state))
    }
}

fn searching_count(state: usize) -> usize {
    state & SEARCHING_MASK
}

fn unparked_count(state: usize) -> usize {
    state >> UNPARKED_SHIFT
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;

    const W0: WorkerId = WorkerId(0);
    const W1: WorkerId = WorkerId(1);

    #[test]
    fn new_counts_all_workers_unparked() {
        let idle = IdleWorkers::new(4);

        assert_eq!(idle.counts(), (4, 0));
        assert_eq!(idle.worker_to_notify(), None);
    }

    #[test]
    #[should_panic(expected = "runtime needs at least one worker")]
    fn new_rejects_zero_workers() {
        IdleWorkers::new(0);
    }

    #[test]
    #[should_panic(expected = "too many workers")]
    fn new_rejects_worker_count_above_max() {
        IdleWorkers::new(WORKER_COUNT_MAX + 1);
    }

    #[test]
    fn new_accepts_worker_count_max() {
        let idle = IdleWorkers::new(WORKER_COUNT_MAX);

        assert_eq!(idle.counts(), (WORKER_COUNT_MAX, 0));
    }

    #[test]
    fn notify_wakes_parked_worker_as_searching() {
        let idle = IdleWorkers::new(2);
        assert!(!idle.start_parking(W1, false));
        assert_eq!(idle.counts(), (1, 0));

        assert_eq!(idle.worker_to_notify(), Some(W1));
        assert_eq!(idle.counts(), (2, 1));
        assert!(
            idle.finish_parking(W1),
            "notified worker must wake searching"
        );
    }

    #[test]
    fn notify_skips_wake_while_someone_searches() {
        let idle = IdleWorkers::new(4);
        assert!(!idle.start_parking(W0, false));
        assert!(!idle.start_parking(W1, false));

        assert_eq!(idle.worker_to_notify(), Some(W1));
        // W1 now counts as searching, so a second spawn must not wake W0.
        assert_eq!(idle.worker_to_notify(), None);
        assert_eq!(idle.counts(), (3, 1));
    }

    #[test]
    fn searching_is_limited_to_half_the_workers() {
        let idle = IdleWorkers::new(4);

        assert!(idle.try_start_searching());
        assert!(idle.try_start_searching());
        assert!(!idle.try_start_searching());
        assert_eq!(idle.counts(), (4, 2));
    }

    #[test]
    fn single_worker_can_search() {
        let idle = IdleWorkers::new(1);

        assert!(idle.try_start_searching());
        assert!(!idle.try_start_searching());
    }

    #[test]
    fn stop_searching_reports_last_searcher() {
        let idle = IdleWorkers::new(4);
        assert!(idle.try_start_searching());
        assert!(idle.try_start_searching());

        assert!(!idle.stop_searching());
        assert!(idle.stop_searching());
        assert_eq!(idle.counts(), (4, 0));
    }

    #[test]
    #[should_panic(expected = "searching count underflow")]
    fn stop_searching_without_searchers_panics() {
        let idle = IdleWorkers::new(2);
        idle.stop_searching();
    }

    #[test]
    fn start_parking_reports_last_searcher() {
        let idle = IdleWorkers::new(4);
        assert!(idle.try_start_searching());
        assert!(idle.try_start_searching());

        assert!(!idle.start_parking(W0, true));
        assert!(idle.start_parking(W1, true));
        assert_eq!(idle.counts(), (2, 0));
    }

    #[test]
    fn start_parking_without_searching_is_never_last_searcher() {
        let idle = IdleWorkers::new(2);

        assert!(!idle.start_parking(W0, false));
        assert_eq!(idle.counts(), (1, 0));
    }

    #[test]
    #[should_panic(expected = "worker parked twice")]
    fn start_parking_twice_panics() {
        let idle = IdleWorkers::new(2);
        idle.start_parking(W0, false);
        idle.start_parking(W0, false);
    }

    #[test]
    fn finish_parking_without_notify_restores_unparked() {
        let idle = IdleWorkers::new(2);
        assert!(!idle.start_parking(W0, false));

        assert!(
            !idle.finish_parking(W0),
            "self-woken worker must not count as searching"
        );
        assert_eq!(idle.counts(), (2, 0));
        assert_eq!(idle.worker_to_notify(), None);
    }
}

#[cfg(all(test, loom))]
mod loom_tests {
    use loom::{
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        thread,
    };

    use super::*;

    struct Model {
        idle: IdleWorkers,
        // Stands in for the injector: the number of queued tasks.
        queued: AtomicUsize,
        // Workers that end up awake and owning the task: they took it, saw it while parking,
        // or were woken by a notify.
        responsible: AtomicUsize,
    }

    impl Model {
        fn try_take(&self) -> bool {
            self.queued
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |queued| {
                    queued.checked_sub(1)
                })
                .is_ok()
        }

        fn notify(&self) {
            if self.idle.worker_to_notify().is_some() {
                self.responsible.fetch_add(1, Ordering::Relaxed);
            }
        }

        // One idle pass of the worker loop: search, then park.
        fn worker_pass(&self, worker_id: WorkerId) {
            let is_searching = self.idle.try_start_searching();
            if is_searching && self.try_take() {
                self.responsible.fetch_add(1, Ordering::Relaxed);
                if self.idle.stop_searching() {
                    self.notify();
                }
                return;
            }

            let is_last_searcher = self.idle.start_parking(worker_id, is_searching);
            if is_last_searcher && self.queued.load(Ordering::Acquire) > 0 {
                // The real worker cancels parking here and loops back to take the task.
                self.responsible.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    // Two workers go idle while a spawner pushes one task. In every interleaving loom can
    // produce, some worker must be awake and responsible for the task, or it would sit in the
    // queue with everyone parked.
    #[test]
    fn pushed_task_is_never_stranded() {
        loom::model(|| {
            let model = Arc::new(Model {
                idle: IdleWorkers::new(2),
                queued: AtomicUsize::new(0),
                responsible: AtomicUsize::new(0),
            });

            let workers: Vec<_> = (0..2)
                .map(|index| {
                    let model = Arc::clone(&model);
                    thread::spawn(move || model.worker_pass(WorkerId(index)))
                })
                .collect();

            model.queued.fetch_add(1, Ordering::Release);
            model.notify();

            for worker in workers {
                worker.join().expect("model worker must not panic");
            }

            assert!(
                model.responsible.load(Ordering::Relaxed) > 0,
                "task stranded with every worker parked"
            );
        });
    }
}
