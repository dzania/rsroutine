use crate::{context::Context, func::Func, stack::Stack};

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
    context: Context,
    func: Func,
    state: RoutineState,
}

extern "C" fn trampoline() -> ! {
    loop {}
}

fn trampoline_addr() -> usize {
    trampoline as *const () as usize
}

impl RsRoutine {
    pub(crate) fn new(func: Box<dyn FnOnce() + Send + 'static>) -> Self {
        let func = Func::new(func);
        let stack = Stack::new(DEFAULT_STACK_SIZE);
        let context = Context::new(&stack, trampoline_addr());

        Self {
            state: RoutineState::Runnable,
            func,
            stack,
            context,
        }
    }

    pub(crate) fn state(&self) -> RoutineState {
        self.state
    }

    pub(crate) fn set_state(&mut self, state: RoutineState) {
        self.state = state;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rsroutine_new_builds_stack_context_and_function() {
        let routine = RsRoutine::new(Box::new(|| {}));

        assert_eq!(routine.stack.len(), DEFAULT_STACK_SIZE);
        assert_eq!(routine.context.sp, routine.stack.aligned_top());
        assert_eq!(routine.context.x29, routine.stack.aligned_top());
        assert_eq!(routine.context.x30, trampoline_addr());
        assert!(routine.func.is_pending());
        assert_eq!(routine.state(), RoutineState::Runnable);
    }
}
