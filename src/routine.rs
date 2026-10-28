use std::{marker::PhantomPinned, pin::Pin};

use crate::{context::Context, stack::Stack};

pub(crate) const DEFAULT_STACK_SIZE: usize = 32 * 1024;

pub(crate) type RunnableFn = Box<dyn FnOnce() + Send + 'static>;

/// A stack plus the saved context that runs on it. Routines are reusable: `start` resets the
/// context to the top of the stack, so a completed routine can run another function.
pub(crate) struct RsRoutine {
    stack: Stack,
    pub(crate) context: Context,
    func: Option<RunnableFn>,
    _unpin: PhantomPinned,
}

impl RsRoutine {
    fn new() -> Self {
        Self {
            stack: Stack::new(DEFAULT_STACK_SIZE),
            context: Context::default(),
            func: None,
            _unpin: PhantomPinned,
        }
    }

    pub(crate) fn new_pinned() -> Pin<Box<Self>> {
        Box::pin(Self::new())
    }

    /// True when no function is installed. Holds for a fresh routine and for one that has
    /// completed, but also for one that is mid-run, so callers must know it has stopped.
    pub(crate) fn is_idle(&self) -> bool {
        self.func.is_none()
    }

    /// Prepares the routine to run `func` from the top of its stack on the next switch into it.
    ///
    /// The previous run's frames are abandoned, not unwound. That is sound because a completed
    /// routine stops inside `complete_current`, and nothing below it owns a value with a `Drop`.
    ///
    /// # Panics
    ///
    /// Panics if a function is already installed and has not started yet.
    pub(crate) fn start(self: Pin<&mut Self>, func: RunnableFn, bootstrap_addr: usize) {
        // SAFETY: The routine is not moved out of the pin. Only `context` and `func` are
        // overwritten in place; `stack` keeps its mapping, so the address stays valid.
        let routine_ref: &mut RsRoutine = unsafe { Pin::get_unchecked_mut(self) };
        assert!(
            routine_ref.is_idle(),
            "routine started while a function is pending"
        );

        let routine_ptr: *mut RsRoutine = routine_ref as *mut RsRoutine;
        routine_ref.context = Context::new_routine(&routine_ref.stack, bootstrap_addr);
        routine_ref.context.x19 = routine_ptr as usize;
        routine_ref.context.x21 = routine_entry as *const () as usize;
        routine_ref.func = Some(func);
    }
}

extern "C" fn routine_entry(routine: *mut RsRoutine) -> ! {
    // SAFETY: `start` stores a valid pointer to the pinned routine before the routine can be
    // switched into.
    let func = unsafe {
        (*routine)
            .func
            .take()
            .expect("routine function must exist on first entry")
    };
    func();

    crate::runtime::complete_current()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::bootstrap_entry_addr;

    #[test]
    fn rsroutine_new_is_idle_with_stack() {
        let routine = RsRoutine::new_pinned();

        assert_eq!(routine.stack.len(), DEFAULT_STACK_SIZE);
        assert!(routine.is_idle());
    }

    #[test]
    fn rsroutine_start_builds_context_and_function() {
        let mut routine = RsRoutine::new_pinned();
        routine
            .as_mut()
            .start(Box::new(|| {}), bootstrap_entry_addr());

        let routine_addr = &*routine as *const RsRoutine as usize;
        assert_eq!(routine.context.sp, routine.stack.aligned_top());
        assert_eq!(routine.context.x29, routine.stack.aligned_top());
        assert_eq!(routine.context.x30, bootstrap_entry_addr());
        assert_eq!(routine.context.x19, routine_addr);
        assert!(!routine.is_idle());
    }

    #[test]
    fn rsroutine_start_resets_context_on_reuse() {
        let mut routine = RsRoutine::new_pinned();
        routine
            .as_mut()
            .start(Box::new(|| {}), bootstrap_entry_addr());

        // Simulate a finished run: the entry took the function and the stack pointer moved.
        // SAFETY: Test only touches fields in place and does not move the routine.
        let routine_mut = unsafe { routine.as_mut().get_unchecked_mut() };
        drop(routine_mut.func.take());
        routine_mut.context.sp -= 256;
        routine_mut.context.x22 = 0xdead;

        routine
            .as_mut()
            .start(Box::new(|| {}), bootstrap_entry_addr());

        assert_eq!(routine.context.sp, routine.stack.aligned_top());
        assert_eq!(routine.context.x22, 0);
        assert!(!routine.is_idle());
    }

    #[test]
    #[should_panic(expected = "routine started while a function is pending")]
    fn rsroutine_start_twice_without_running_panics() {
        let mut routine = RsRoutine::new_pinned();
        routine
            .as_mut()
            .start(Box::new(|| {}), bootstrap_entry_addr());
        routine
            .as_mut()
            .start(Box::new(|| {}), bootstrap_entry_addr());
    }
}
