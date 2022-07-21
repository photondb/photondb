use std::{
    mem::MaybeUninit,
    ptr::null_mut,
    sync::{
        atomic::{AtomicPtr, AtomicU64, Ordering},
        Arc,
    },
};

use super::Guard;

#[derive(Clone, Default)]
pub struct PageTable {
    inner: Arc<Inner>,
}

impl PageTable {
    pub fn get(&self, id: u64) -> u64 {
        self.inner.index(id).load(Ordering::Acquire)
    }

    pub fn set(&self, id: u64, ptr: u64) {
        self.inner.index(id).store(ptr, Ordering::Release);
    }

    pub fn cas(&self, id: u64, old: u64, new: u64) -> Option<u64> {
        match self
            .inner
            .index(id)
            .compare_exchange(old, new, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => None,
            Err(now) => Some(now),
        }
    }

    pub fn alloc(&self, _: &Guard) -> Option<u64> {
        self.inner.alloc()
    }

    pub fn dealloc(&self, id: u64, guard: &Guard) {
        let inner = self.inner.clone();
        guard.defer(move || {
            inner.dealloc(id);
        })
    }
}

struct Inner {
    // Level 0: [0, L0_MAX)
    l0: Box<L0<L0_LEN>>,
    // Level 1: [L0_MAX, L1_MAX)
    l1: Box<L1<L1_LEN>>,
    // Level 2: [L1_MAX, L2_MAX)
    l2: Box<L2<L2_LEN>>,
    // The next id to allocate.
    next: AtomicU64,
    // The head of the free list.
    // The list uses epoch-based reclaimation to prevent the ABA problem.
    free: AtomicU64,
}

impl Default for Inner {
    fn default() -> Self {
        Self {
            l0: Box::default(),
            l1: Box::default(),
            l2: Box::default(),
            next: AtomicU64::new(0),
            free: AtomicU64::new(L2_MAX),
        }
    }
}

impl Inner {
    fn index(&self, index: u64) -> &AtomicU64 {
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

    fn alloc(&self) -> Option<u64> {
        let mut id = self.free.load(Ordering::Acquire);
        while id != L2_MAX {
            let next = self.index(id).load(Ordering::Acquire);
            match self
                .free
                .compare_exchange(id, next, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => break,
                Err(actual) => id = actual,
            }
        }
        if id == L2_MAX {
            id = self.next.load(Ordering::Relaxed);
            if id < L2_MAX {
                id = self.next.fetch_add(1, Ordering::Relaxed);
            }
        }
        if id < L2_MAX {
            Some(id)
        } else {
            None
        }
    }

    fn dealloc(&self, id: u64) {
        let mut next = self.free.load(Ordering::Acquire);
        loop {
            self.index(id).store(next, Ordering::Release);
            match self
                .free
                .compare_exchange(next, id, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => break,
                Err(actual) => next = actual,
            }
        }
    }
}

struct L0<const N: usize>([AtomicU64; N]);

impl<const N: usize> Default for L0<N> {
    fn default() -> Self {
        Self(unsafe { MaybeUninit::zeroed().assume_init() })
    }
}

impl<const N: usize> L0<N> {
    fn index(&self, index: u64) -> &AtomicU64 {
        &self.0[index as usize]
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
                    let ptr = child.load(Ordering::Acquire);
                    if !ptr.is_null() {
                        unsafe {
                            Box::from_raw(ptr);
                        }
                    }
                }
            }
        }

        impl<const N: usize> $level<N> {
            fn index(&self, index: u64) -> &AtomicU64 {
                let i = index / $fanout;
                let j = index % $fanout;
                let p = self.0[i as usize].load(Ordering::Relaxed);
                let child = unsafe {
                    p.as_ref()
                        .unwrap_or_else(|| self.install_or_acquire_child(i as usize))
                };
                child.index(j)
            }

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
const L0_MAX: u64 = FANOUT as u64;
const L1_MAX: u64 = L0_MAX * L0_MAX;
const L2_MAX: u64 = L1_MAX * L0_MAX;

define_level!(L1, L0<FANOUT>, L0_MAX);
define_level!(L2, L1<FANOUT>, L1_MAX);

#[cfg(test)]
mod test {
    use crossbeam_epoch::unprotected;

    use super::*;

    #[test]
    fn test_alloc() {
        let guard = unsafe { unprotected() };
        let table = PageTable::default();
        assert_eq!(table.alloc(guard), Some(0));
        assert_eq!(table.alloc(guard), Some(1));
        table.dealloc(0, guard);
        table.dealloc(1, guard);
        assert_eq!(table.alloc(guard), Some(1));
        assert_eq!(table.alloc(guard), Some(0));
    }

    #[test]
    fn test_index() {
        let table = PageTable::default();
        for i in [0, L0_MAX - 1, L0_MAX, L1_MAX - 1, L1_MAX, L2_MAX - 1] {
            table.set(i, i);
            assert_eq!(table.get(i), i);
        }
    }
}
