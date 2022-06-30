use std::{
    mem::MaybeUninit,
    ops::Index,
    ptr::null_mut,
    sync::{
        atomic::{AtomicPtr, AtomicUsize, Ordering},
        Arc,
    },
};

use crossbeam_epoch::Guard;

const MIN_ID: usize = 0;
const MAX_ID: usize = L2_MAX - 1;
const INF_ID: usize = L2_MAX;

#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct PageId(usize);

impl PageId {
    pub const fn min() -> Self {
        Self(MIN_ID)
    }

    pub const fn max() -> Self {
        Self(MAX_ID)
    }
}

pub struct PageTable {
    inner: Arc<Inner>,
}

impl PageTable {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Inner::new()),
        }
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn get(&self, id: PageId) -> usize {
        self.inner.index(id.0).load(Ordering::Acquire)
    }

    pub fn cas(&self, id: PageId, old: usize, new: usize) -> Option<usize> {
        match self
            .inner
            .index(id.0)
            .compare_exchange(old, new, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => None,
            Err(actual) => Some(actual),
        }
    }

    pub fn install(&self, new: usize, _: &Guard) -> Option<PageId> {
        if let Some(id) = self.inner.alloc() {
            self.inner.index(id).store(new, Ordering::Release);
            Some(PageId(id))
        } else {
            None
        }
    }

    pub fn uninstall(&self, id: PageId, guard: &Guard) {
        let inner = self.inner.clone();
        // Prevents the id from being reused in the same epoch to protect the free list.
        guard.defer(move || {
            inner.dealloc(id.0);
        });
    }
}

struct Inner {
    // Level 0: [0, L0_MAX)
    l0: Box<L0<L0_LEN>>,
    // Level 1: [L0_MAX, L1_MAX)
    l1: Box<L1<L1_LEN>>,
    // Level 2: [L1_MAX, L2_MAX)
    l2: Box<L2<L2_LEN>>,
    // The number of ids in use.
    len: AtomicUsize,
    // The next id to allocate.
    next: AtomicUsize,
    // The head of the free list.
    free: AtomicUsize,
}

impl Inner {
    fn new() -> Self {
        Self {
            l0: Box::default(),
            l1: Box::default(),
            l2: Box::default(),
            len: AtomicUsize::new(0),
            next: AtomicUsize::new(0),
            free: AtomicUsize::new(INF_ID),
        }
    }

    fn len(&self) -> usize {
        self.len.load(Ordering::Relaxed)
    }

    fn alloc(&self) -> Option<usize> {
        let mut id = self.free.load(Ordering::Acquire);
        while id != INF_ID {
            let next = self.index(id).load(Ordering::Acquire);
            match self
                .free
                .compare_exchange(id, next, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => break,
                Err(actual) => id = actual,
            }
        }
        if id == INF_ID {
            let next = self.next.load(Ordering::Relaxed);
            if next <= MAX_ID {
                id = self.next.fetch_add(1, Ordering::Relaxed);
            }
        }
        if id <= MAX_ID {
            self.len.fetch_add(1, Ordering::Relaxed);
            Some(id)
        } else {
            None
        }
    }

    fn dealloc(&self, id: usize) {
        let head = self.index(id);
        let mut next = self.free.load(Ordering::Acquire);
        loop {
            head.store(next, Ordering::Release);
            match self
                .free
                .compare_exchange(next, id, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => break,
                Err(actual) => next = actual,
            }
        }
        self.len.fetch_sub(1, Ordering::Relaxed);
    }
}

impl Default for Inner {
    fn default() -> Self {
        Self::new()
    }
}

impl Index<usize> for Inner {
    type Output = AtomicUsize;

    fn index(&self, index: usize) -> &Self::Output {
        if index < L0_MAX {
            self.l0.index(index)
        } else if index < L1_MAX {
            self.l1.index(index - L0_MAX)
        } else if index < L2_MAX {
            self.l2.index(index - L1_MAX)
        } else {
            unreachable!()
        }
    }
}

struct L0<const N: usize>([AtomicUsize; N]);

impl<const N: usize> Default for L0<N> {
    fn default() -> Self {
        Self(unsafe { MaybeUninit::zeroed().assume_init() })
    }
}

impl<const N: usize> Index<usize> for L0<N> {
    type Output = AtomicUsize;

    fn index(&self, index: usize) -> &Self::Output {
        &self.0[index]
    }
}

macro_rules! define_level {
    ($level:ident, $child:ty, $fanout:expr) => {
        struct $level<const N: usize>([AtomicPtr<$child>; N]);

        impl<const N: usize> Default for $level<N> {
            fn default() -> Self {
                Self(unsafe { MaybeUninit::zeroed().assume_init() })
            }
        }

        impl<const N: usize> Drop for $level<N> {
            fn drop(&mut self) {
                for child in &self.0 {
                    let ptr = child.load(Ordering::Relaxed);
                    if !ptr.is_null() {
                        unsafe {
                            Box::from_raw(ptr);
                        }
                    }
                }
            }
        }

        impl<const N: usize> Index<usize> for $level<N> {
            type Output = AtomicUsize;

            fn index(&self, index: usize) -> &Self::Output {
                let i = index / $fanout;
                let j = index % $fanout;
                let p = self.0[i].load(Ordering::Relaxed);
                let child = unsafe {
                    p.as_ref()
                        .unwrap_or_else(|| self.install_or_acquire_child(i))
                };
                child.index(j)
            }
        }

        impl<const N: usize> $level<N> {
            #[cold]
            fn install_or_acquire_child(&self, index: usize) -> &$child {
                let mut child = Box::into_raw(Box::default());
                if let Err(current) = self.0[index].compare_exchange(
                    null_mut(),
                    child,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    unsafe {
                        Box::from_raw(child);
                    }
                    child = current;
                }
                unsafe { &*child }
            }
        }
    };
}

const FANOUT: usize = 1 << 16;
const L0_LEN: usize = FANOUT;
const L1_LEN: usize = FANOUT - 1;
const L2_LEN: usize = FANOUT - 1;
const L0_MAX: usize = FANOUT;
const L1_MAX: usize = L0_MAX * FANOUT;
const L2_MAX: usize = L1_MAX * FANOUT;

define_level!(L1, L0<FANOUT>, L0_MAX);
define_level!(L2, L1<FANOUT>, L1_MAX);

#[cfg(test)]
mod test {
    extern crate test;

    use test::{black_box, Bencher};

    use super::*;

    const N: usize = 1 << 10;

    #[test]
    fn test_alloc() {
        let inner = Inner::default();
        let id1 = MIN_ID;
        let id2 = MIN_ID + 1;
        assert_eq!(inner.alloc(), Some(id1));
        assert_eq!(inner.alloc(), Some(id2));
        assert_eq!(inner.len(), 2);
        inner.dealloc(id1);
        inner.dealloc(id2);
        assert_eq!(inner.len(), 0);
        assert_eq!(inner.alloc(), Some(id2));
        assert_eq!(inner.alloc(), Some(id1));
    }

    #[test]
    fn test_index() {
        let inner = Inner::default();
        for i in [0, L0_MAX - 1, L0_MAX, L1_MAX - 1, L1_MAX, L2_MAX - 1] {
            inner.index(i).store(i, Ordering::Relaxed);
            assert_eq!(inner.index(i).load(Ordering::Relaxed), i);
        }
    }

    fn bench<T: Default + Index<usize>>(b: &mut Bencher, start: usize) {
        let l: Box<T> = Box::default();
        for i in start..(start + N) {
            l.index(i);
        }
        b.iter(|| {
            for i in start..(start + N) {
                black_box(l.index(i));
            }
        })
    }

    #[bench]
    fn bench_l0(b: &mut Bencher) {
        bench::<L0<FANOUT>>(b, 0);
    }

    #[bench]
    fn bench_l1(b: &mut Bencher) {
        bench::<L1<FANOUT>>(b, L0_MAX);
    }

    #[bench]
    fn bench_l2(b: &mut Bencher) {
        bench::<L2<FANOUT>>(b, L1_MAX);
    }

    #[bench]
    fn bench_inner0(b: &mut Bencher) {
        bench::<Inner>(b, 0);
    }

    #[bench]
    fn bench_inner1(b: &mut Bencher) {
        bench::<Inner>(b, L0_MAX);
    }

    #[bench]
    fn bench_inner2(b: &mut Bencher) {
        bench::<Inner>(b, L1_MAX);
    }
}
