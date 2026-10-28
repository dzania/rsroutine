use std::pin::Pin;

use crate::routine::RsRoutine;

/// Idle routines a worker keeps for reuse. Each one holds a stack mapping (usable stack plus a
/// guard page), so this bounds retained memory per worker. It does not bound how many routines
/// can be live at once; past the cap, finished routines are unmapped.
pub(crate) const ROUTINE_POOL_CAPACITY: usize = 64;

/// Worker-local free list of finished routines.
///
/// Reusing a routine skips the `Box` allocation and the `mmap`/`mprotect`/`munmap` syscalls
/// that a fresh stack costs. The pool is owned by one worker thread and never shared, so it
/// needs no locking.
pub(crate) struct RoutinePool {
    idle: Vec<Pin<Box<RsRoutine>>>,
}

impl RoutinePool {
    pub(crate) fn new() -> Self {
        Self {
            idle: Vec::with_capacity(ROUTINE_POOL_CAPACITY),
        }
    }

    /// Returns an idle routine, allocating a new stack only when the pool is empty.
    pub(crate) fn acquire(&mut self) -> Pin<Box<RsRoutine>> {
        match self.idle.pop() {
            Some(routine) => routine,
            None => RsRoutine::new_pinned(),
        }
    }

    /// Takes back a routine that has finished running. The caller must guarantee the routine
    /// will never be switched into again until it is restarted.
    pub(crate) fn release(&mut self, routine: Pin<Box<RsRoutine>>) {
        assert!(routine.is_idle(), "released routine still holds a function");

        if self.idle.len() < ROUTINE_POOL_CAPACITY {
            self.idle.push(routine);
        }
        // Otherwise `routine` drops here and its stack is unmapped.
    }

    #[cfg(test)]
    fn idle_count(&self) -> usize {
        self.idle.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::bootstrap_entry_addr;

    fn routine_addr(routine: &Pin<Box<RsRoutine>>) -> usize {
        &**routine as *const RsRoutine as usize
    }

    #[test]
    fn acquire_from_empty_pool_allocates() {
        let mut pool = RoutinePool::new();
        let routine = pool.acquire();

        assert!(routine.is_idle());
        assert_eq!(pool.idle_count(), 0);
    }

    #[test]
    fn released_routine_is_reused() {
        let mut pool = RoutinePool::new();
        let routine = pool.acquire();
        let addr = routine_addr(&routine);

        pool.release(routine);
        assert_eq!(pool.idle_count(), 1);

        let reused = pool.acquire();
        assert_eq!(routine_addr(&reused), addr);
        assert_eq!(pool.idle_count(), 0);
    }

    #[test]
    fn release_past_capacity_drops_routine() {
        let mut pool = RoutinePool::new();
        let routines: Vec<_> = (0..ROUTINE_POOL_CAPACITY + 1)
            .map(|_| RsRoutine::new_pinned())
            .collect();

        for routine in routines {
            pool.release(routine);
        }

        assert_eq!(pool.idle_count(), ROUTINE_POOL_CAPACITY);
    }

    #[test]
    #[should_panic(expected = "released routine still holds a function")]
    fn release_of_unstarted_routine_panics() {
        let mut pool = RoutinePool::new();
        let mut routine = pool.acquire();
        routine
            .as_mut()
            .start(Box::new(|| {}), bootstrap_entry_addr());

        pool.release(routine);
    }
}
