pub(crate) struct Func {
    inner: Option<Box<dyn FnOnce() + Send + 'static>>,
}

impl Func {
    pub(crate) fn new(func: Box<dyn FnOnce() + Send + 'static>) -> Self {
        Self { inner: Some(func) }
    }

    pub(crate) fn is_pending(&self) -> bool {
        self.inner.is_some()
    }

    pub(crate) fn call_once(&mut self) {
        // TODO: Do not use expect.
        // And add catch_unwind
        let inner = self
            .inner
            .take()
            .expect("Func::call_once called after function was already consumed");

        inner();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    #[test]
    fn func_call_once_runs_and_consumes_inner_function() {
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_for_func = Arc::clone(&calls);
        let mut func = Func::new(Box::new(move || {
            calls_for_func.fetch_add(1, Ordering::SeqCst);
        }));

        func.call_once();

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(!func.is_pending());
    }
}
