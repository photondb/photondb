#![allow(dead_code)]

use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::Mutex,
    task::{Context, Poll, Waker},
};

/// A counter which used to synchronize multiple tasks. It is a single-use
/// barrier.
pub(crate) struct Latch {
    expect: usize,
    core: Mutex<LatchCore>,
}

pub(crate) struct LatchCore {
    count: usize,
    next_id: usize,
    wakers: HashMap<usize, Waker>,
}

impl Latch {
    /// Create a new `Latch` that can block multiple tasks.
    pub(crate) fn new(expect: usize) -> Self {
        Latch {
            expect,
            core: Mutex::new(LatchCore {
                count: 0,
                next_id: 1,
                wakers: HashMap::default(),
            }),
        }
    }

    /// Blocks until the counter reaches zero.
    #[inline]
    pub(crate) fn wait(&self) -> LatchWaiter {
        LatchWaiter {
            latch: self,
            state: WaitState::Pending,
        }
    }

    /// Decrements the counter in no-blocking manager
    ///
    /// # Panic
    ///
    /// Panic if the latch count is zero.
    pub(crate) fn count_down(&self) {
        let wakers = {
            let mut core = self.core.lock().expect("Poisoned");
            core.count += 1;
            assert!(core.count <= self.expect);
            if core.count == self.expect {
                Some(std::mem::take(&mut core.wakers))
            } else {
                None
            }
        };

        if let Some(wakers) = wakers {
            wakers.into_values().for_each(Waker::wake);
        }
    }
}

#[derive(Debug)]
enum WaitState {
    Pending,
    Waiting(usize),
    Done,
}

pub(crate) struct LatchWaiter<'a> {
    latch: &'a Latch,
    state: WaitState,
}

impl<'a> LatchWaiter<'a> {
    fn poll_wait(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = self.get_mut();
        match this.state {
            WaitState::Pending => {
                let mut core = this.latch.core.lock().expect("Poisoned");
                if core.count == this.latch.expect {
                    this.state = WaitState::Done;
                    Poll::Ready(())
                } else {
                    let id = core.next_id;
                    core.next_id += 1;
                    core.wakers.insert(id, cx.waker().clone());
                    this.state = WaitState::Waiting(id);
                    Poll::Pending
                }
            }
            WaitState::Waiting(id) => {
                let mut core = this.latch.core.lock().expect("Poisoned");
                if core.count == this.latch.expect {
                    this.state = WaitState::Done;
                    Poll::Ready(())
                } else {
                    core.wakers.insert(id, cx.waker().clone());
                    Poll::Pending
                }
            }
            WaitState::Done => Poll::Ready(()),
        }
    }
}

impl<'a> Future for LatchWaiter<'a> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.poll_wait(cx)
    }
}

impl<'a> Drop for LatchWaiter<'a> {
    fn drop(&mut self) {
        if let WaitState::Waiting(id) = self.state {
            let mut core = self.latch.core.lock().expect("Poisoned");
            core.wakers.remove(&id);
        }
    }
}
