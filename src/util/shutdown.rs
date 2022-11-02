use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
};

pub(crate) struct ShutdownNotifier {
    core: Arc<Mutex<Core>>,
}

#[derive(Clone)]
pub(crate) struct Shutdown {
    core: Arc<Mutex<Core>>,
}

struct Core {
    closed: bool,
    wakers: HashMap<usize, Waker>,
}

impl Shutdown {
    fn new(core: Arc<Mutex<Core>>) -> Shutdown {
        Shutdown { core }
    }

    pub(crate) fn is_terminated(&self) -> bool {
        self.core.lock().expect("Poisoned").closed
    }
}

impl Future for Shutdown {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let shut = self.get_mut();
        let mut core = shut.core.lock().unwrap();
        if core.closed {
            Poll::Ready(())
        } else {
            let id = shut as *const Shutdown as usize;
            core.wakers.insert(id, cx.waker().clone());
            Poll::Pending
        }
    }
}

impl ShutdownNotifier {
    pub(crate) fn new() -> Self {
        ShutdownNotifier::default()
    }

    pub(crate) fn subscribe(&self) -> Shutdown {
        Shutdown::new(self.core.clone())
    }

    pub(crate) fn terminate(&self) {
        let mut core = self.core.lock().unwrap();
        core.closed = true;
        for (_, waker) in std::mem::take(&mut core.wakers) {
            waker.wake();
        }
    }
}

impl Default for ShutdownNotifier {
    fn default() -> Self {
        ShutdownNotifier {
            core: Arc::new(Mutex::new(Core {
                closed: false,
                wakers: HashMap::default(),
            })),
        }
    }
}

impl Drop for ShutdownNotifier {
    fn drop(&mut self) {
        self.terminate();
    }
}

pub(crate) struct WithShutdown<'a, T: Future> {
    value: T,
    shutdown: &'a mut Shutdown,
}

impl<'a, T: Future> Future for WithShutdown<'a, T> {
    type Output = Option<T::Output>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // Safety: we never move `self.value`
        unsafe {
            let p = self.as_mut().map_unchecked_mut(|me| &mut me.value);
            if let Poll::Ready(v) = p.poll(cx) {
                return Poll::Ready(Some(v));
            }
        }

        // Safety: we never move `self.shutdown`
        unsafe {
            let p = self.as_mut().map_unchecked_mut(|me| me.shutdown);
            match p.poll(cx) {
                Poll::Ready(()) => Poll::Ready(None),
                Poll::Pending => Poll::Pending,
            }
        }
    }
}

pub(crate) fn with_shutdown<T: Future>(shutdown: &mut Shutdown, value: T) -> WithShutdown<T> {
    WithShutdown { value, shutdown }
}
