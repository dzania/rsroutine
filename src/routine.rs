use crate::{
    context::{Context, switch},
    func::Func,
    stack::Stack,
};

pub(crate) const DEFAULT_STACK_SIZE: usize = 32 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RoutineState {
    Runnable,
    Running,
    Parked,
    Finished,
}

pub(crate) struct RsRoutine {
    stack: Stack,
    pub(crate) context: Context,
    func: Func,
    state: RoutineState,
}

impl RsRoutine {
    pub(crate) fn new(func: Box<dyn FnOnce() + Send + 'static>, bootstrap_addr: usize) -> Self {
        let func = Func::new(func);
        let stack = Stack::new(DEFAULT_STACK_SIZE);
        let context = Context::new_routine(&stack, bootstrap_addr);

        Self {
            state: RoutineState::Runnable,
            func,
            stack,
            context,
        }
    }

    pub(crate) fn initialize_bootstrap(&mut self, scheduler_context: *mut Context) {
        let routine = self as *mut RsRoutine;
        self.context.x19 = routine as usize;
        self.context.x20 = scheduler_context as usize;
        self.context.x21 = routine_entry as *const () as usize;
    }

    pub(crate) fn state(&self) -> RoutineState {
        self.state
    }

    pub(crate) fn set_state(&mut self, state: RoutineState) {
        self.state = state;
    }
}

extern "C" fn routine_entry(routine: *mut RsRoutine, scheduler_context: *mut Context) -> ! {
    // SAFETY: `initialize_bootstrap` stores a valid pointer to the boxed routine in x19 before the
    // routine can be started.
    let routine = unsafe { &mut *routine };
    routine.func.call_once();
    routine.state = RoutineState::Finished;

    // SAFETY: `initialize_bootstrap` stores a valid scheduler context pointer in x20, and the
    // scheduler context outlives every routine it starts.
    unsafe {
        switch(&mut routine.context, &*scheduler_context);
    }

    unreachable!("finished routine resumed after switching back to scheduler")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::bootstrap_entry_addr;

    #[test]
    fn rsroutine_new_builds_stack_context_and_function() {
        let routine = RsRoutine::new(Box::new(|| {}), bootstrap_entry_addr());

        assert_eq!(routine.stack.len(), DEFAULT_STACK_SIZE);
        assert_eq!(routine.context.sp, routine.stack.aligned_top());
        assert_eq!(routine.context.x29, routine.stack.aligned_top());
        assert_eq!(routine.context.x30, bootstrap_entry_addr());
        assert!(routine.func.is_pending());
        assert_eq!(routine.state(), RoutineState::Runnable);
    }
}
