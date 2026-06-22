use std::collections::VecDeque;

use crate::{
    context::Context,
    routine::{RoutineState, RsRoutine},
};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub(crate) struct RoutineId(usize);

impl RoutineId {
    fn inner(self) -> usize {
        self.0
    }
}

pub(crate) struct Scheduler {
    //context: Context,
    routines: Vec<RsRoutine>,
    run_queue: VecDeque<RoutineId>,
}

impl Scheduler {
    pub(crate) fn new() -> Self {
        Self {
            //context: Context::new(stack, entry_addr)
            routines: Vec::new(),
            run_queue: VecDeque::new(),
        }
    }

    pub(crate) fn spawn(&mut self, func: Box<dyn FnOnce() + Send + 'static>) -> RoutineId {
        let routine = RsRoutine::new(func);
        let routine_id = RoutineId(self.routines.len());
        self.routines.push(routine);
        self.run_queue.push_back(routine_id);
        routine_id
    }

    pub(crate) fn mark(&mut self, routine: RoutineId, state: RoutineState) -> Option<()> {
        self.routine_mut(routine).map(|routine| {
            routine.set_state(state);
        })
    }

    pub(crate) fn get_state(&self, routine: RoutineId) -> Option<RoutineState> {
        self.routine(routine).map(RsRoutine::state)
    }

    pub(crate) fn next(&mut self) -> Option<RoutineId> {
        if let Some(routine) = self.run_queue.pop_front() {
            debug_assert_eq!(self.get_state(routine), Some(RoutineState::Runnable));
            self.mark(routine, RoutineState::Running)
                .expect("queued routine must exist");
            Some(routine)
        } else {
            None
        }
    }

    pub(crate) fn routine(&self, routine: RoutineId) -> Option<&RsRoutine> {
        self.routines.get(routine.inner())
    }

    pub(crate) fn routine_mut(&mut self, routine: RoutineId) -> Option<&mut RsRoutine> {
        self.routines.get_mut(routine.inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scheduler_new_starts_empty() {
        let mut scheduler = Scheduler::new();

        assert!(scheduler.routines.is_empty());
        assert!(scheduler.run_queue.is_empty());
        assert_eq!(scheduler.next(), None);
    }

    #[test]
    fn scheduler_spawn_stores_routine_and_returns_id() {
        let mut scheduler = Scheduler::new();

        let first_id = scheduler.spawn(Box::new(|| {}));
        let second_id = scheduler.spawn(Box::new(|| {}));

        assert_eq!(first_id, RoutineId(0));
        assert_eq!(second_id, RoutineId(1));
        assert_eq!(scheduler.routines.len(), 2);
        assert!(scheduler.routine(first_id).is_some());
        assert!(scheduler.routine(second_id).is_some());
    }

    #[test]
    fn scheduler_spawn_queues_routines_fifo() {
        let mut scheduler = Scheduler::new();
        let first_id = scheduler.spawn(Box::new(|| {}));
        let second_id = scheduler.spawn(Box::new(|| {}));

        assert_eq!(scheduler.next(), Some(first_id));
        assert_eq!(scheduler.get_state(first_id), Some(RoutineState::Running));
        assert_eq!(scheduler.next(), Some(second_id));
        assert_eq!(scheduler.get_state(second_id), Some(RoutineState::Running));
        assert_eq!(scheduler.next(), None);
    }

    #[test]
    fn scheduler_routine_accessors_reject_unknown_id() {
        let mut scheduler = Scheduler::new();
        let unknown_id = RoutineId(0);

        assert!(scheduler.routine(unknown_id).is_none());
        assert!(scheduler.routine_mut(unknown_id).is_none());
        assert_eq!(scheduler.get_state(unknown_id), None);
        assert_eq!(scheduler.mark(unknown_id, RoutineState::Running), None);
    }

    #[test]
    fn scheduler_mark_updates_routine_state() {
        let mut scheduler = Scheduler::new();
        let routine_id = scheduler.spawn(Box::new(|| {}));

        assert_eq!(
            scheduler.get_state(routine_id),
            Some(RoutineState::Runnable)
        );

        scheduler
            .mark(routine_id, RoutineState::Parked)
            .expect("spawned routine must be stored");
        assert_eq!(scheduler.get_state(routine_id), Some(RoutineState::Parked));

        scheduler
            .mark(routine_id, RoutineState::Finished)
            .expect("spawned routine must be stored");
        assert_eq!(
            scheduler.get_state(routine_id),
            Some(RoutineState::Finished)
        );
    }

    #[test]
    fn scheduler_routine_mut_allows_direct_state_update() {
        let mut scheduler = Scheduler::new();
        let routine_id = scheduler.spawn(Box::new(|| {}));
        let routine = scheduler
            .routine_mut(routine_id)
            .expect("spawned routine must be stored");

        routine.set_state(RoutineState::Running);

        let routine = scheduler
            .routine(routine_id)
            .expect("spawned routine must be stored");
        assert_eq!(routine.state(), RoutineState::Running);
    }
}
