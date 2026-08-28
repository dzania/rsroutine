use std::marker::PhantomPinned;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;

use crate::{context::Context, stack::Stack};

pub(crate) const DEFAULT_STACK_SIZE: usize = 32 * 1024;

pub(crate) struct RsRoutine {
    _stack: Stack,
    pub(crate) context: Context,
    func: Option<Box<dyn FnOnce() + Send + 'static>>,
    _unpin: PhantomPinned,
}

impl RsRoutine {
    fn new(func: Box<dyn FnOnce() + Send + 'static>, bootstrap_addr: usize) -> Self {
        let stack = Stack::new(DEFAULT_STACK_SIZE);
        let context = Context::new_routine(&stack, bootstrap_addr);

        Self {
            func: Some(func),
            _stack: stack,
            context,
            _unpin: PhantomPinned,
        }
    }

    pub(crate) fn new_pinned(
        func: Box<dyn FnOnce() + Send + 'static>,
        bootstrap_addr: usize,
    ) -> Pin<Box<Self>> {
        let mut routine = Box::pin(Self::new(func, bootstrap_addr));
        {
            let routine_mut_ref = routine.as_mut();
            routine_mut_ref.initialize_bootstrap();
        }
        routine
    }

    // The routine has already been pinned. The mutable reference is used only to obtain its stable address and update scalar context fields in place.
    // Neither the routine nor its address-sensitive fields are moved or replaced.
    pub(crate) fn initialize_bootstrap(self: Pin<&mut Self>) {
        let routine_ref: &mut RsRoutine = unsafe { Pin::get_unchecked_mut(self) };
        let routine_ptr: *mut RsRoutine = routine_ref as *mut RsRoutine;
        routine_ref.context.x19 = routine_ptr as usize;
        routine_ref.context.x21 = routine_entry as *const () as usize;
    }
}

extern "C" fn routine_entry(routine: *mut RsRoutine) -> ! {
    // SAFETY: `initialize_bootstrap` stores a valid pointer to the boxed routine before the
    // routine can be started.
    let func = unsafe {
        (*routine)
            .func
            .take()
            .expect("routine function must exist on first entry")
    };
    let result = catch_unwind(AssertUnwindSafe(func));

    crate::runtime::complete_current(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::bootstrap_entry_addr;

    #[test]
    fn rsroutine_new_builds_stack_context_and_function() {
        let routine = RsRoutine::new(Box::new(|| {}), bootstrap_entry_addr());

        assert_eq!(routine._stack.len(), DEFAULT_STACK_SIZE);
        assert_eq!(routine.context.sp, routine._stack.aligned_top());
        assert_eq!(routine.context.x29, routine._stack.aligned_top());
        assert_eq!(routine.context.x30, bootstrap_entry_addr());
        assert!(routine.func.is_some());
    }
}
