use std::{
    mem::MaybeUninit,
    ptr::null_mut,
    sync::{
        atomic::{fence, AtomicPtr, AtomicU64, Ordering},
        Arc,
    },
};

pub(crate) const NAN_ID: u64 = 0;
pub(crate) const MIN_ID: u64 = 1;
pub(crate) const MAX_ID: u64 = L2_FANOUT - 1;

/// Builds a new [`PageTable`] from existing mappings.
#[derive(Default)]
pub(crate) struct PageTableBuilder {
    inner: Inner,
    max_id: u64,
}

impl PageTableBuilder {
    pub(crate) fn set(&mut self, id: u64, addr: u64) {
        if addr <= self.inner.index(id).load(Ordering::Relaxed) {
            panic!("set page id {id} with addr {addr} who small than exists");
        }

        self.inner.index(id).store(addr, Ordering::Relaxed);
        self.max_id = self.max_id.max(id);
    }

    pub(crate) fn build(mut self) -> PageTable {
        let mut free = NAN_ID;
        // We prefer smaller ids so we scan backward to build the free list.
        for id in (MIN_ID..=self.max_id).rev() {
            if self.inner.index(id).load(Ordering::Relaxed) == 0 {
                self.inner.index(id).store(free, Ordering::Relaxed);
                free = id;
            }
        }
        self.inner.free = AtomicU64::new(free);
        self.inner.next = AtomicU64::new(self.max_id + 1);
        // Make sure all writes are visible before we publish the table.
        fence(Ordering::SeqCst);
        PageTable {
            inner: Arc::new(self.inner),
        }
    }
}

/// A table that maps page ids to page addresses.
#[derive(Clone, Default)]
pub(crate) struct PageTable {
    inner: Arc<Inner>,
}

impl PageTable {
    /// Returns the address of the page with the given id.
    pub(crate) fn get(&self, id: u64) -> u64 {
        self.inner.index(id).load(Ordering::Acquire)
    }

    /// Updates the address of the page with the given id.
    pub(crate) fn set(&self, id: u64, new: u64) {
        self.inner.index(id).store(new, Ordering::Release)
    }

    pub(crate) fn cas(&self, id: u64, old: u64, new: u64) -> Result<u64, u64> {
        self.inner
            .index(id)
            .compare_exchange(old, new, Ordering::AcqRel, Ordering::Acquire)
    }

    pub(crate) unsafe fn alloc(&self) -> Option<u64> {
        self.inner.alloc()
    }

    pub(crate) unsafe fn dealloc(&self, id: u64) {
        self.inner.dealloc(id)
    }
}

struct Inner {
    // Level 0: [0, L0_FANOUT)
    l0: Box<L0<L0_LEN>>,
    // Level 1: [L0_FANOUT, L1_FANOUT)
    l1: Box<L1<L1_LEN>>,
    // Level 2: [L1_FANOUT, L2_FANOUT)
    l2: Box<L2<L2_LEN>>,
    // The next id to allocate.
    next: AtomicU64,
    // The head of the free list.
    // The list uses epoch-based reclamation to prevent the ABA problem.
    free: AtomicU64,
}

impl Default for Inner {
    fn default() -> Self {
        Self {
            l0: Box::default(),
            l1: Box::default(),
            l2: Box::default(),
            next: AtomicU64::new(MIN_ID),
            free: AtomicU64::new(NAN_ID),
        }
    }
}

impl Inner {
    fn index(&self, index: u64) -> &AtomicU64 {
        if index < L0_FANOUT {
            self.l0.index(index)
        } else if index < L1_FANOUT {
            self.l1.index(index - L0_FANOUT)
        } else if index < L2_FANOUT {
            self.l2.index(index - L1_FANOUT)
        } else {
            unreachable!()
        }
    }

    fn alloc(&self) -> Option<u64> {
        let mut id = self.free.load(Ordering::Acquire);
        while id != NAN_ID {
            let next = self.index(id).load(Ordering::Acquire);
            match self
                .free
                .compare_exchange(id, next, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => break,
                Err(actual) => id = actual,
            }
        }
        if id == NAN_ID {
            id = self.next.load(Ordering::Relaxed);
            if id < MAX_ID {
                id = self.next.fetch_add(1, Ordering::Relaxed);
            }
        }
        if id < MAX_ID {
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
                            drop(Box::from_raw(ptr));
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
                        drop(Box::from_raw(child));
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
const L0_FANOUT: u64 = FANOUT as u64;
const L1_FANOUT: u64 = L0_FANOUT * FANOUT as u64;
const L2_FANOUT: u64 = L1_FANOUT * FANOUT as u64;

define_level!(L1, L0<FANOUT>, L0_FANOUT);
define_level!(L2, L1<FANOUT>, L1_FANOUT);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alloc() {
        let table = PageTable::default();
        unsafe {
            assert_eq!(table.alloc(), Some(1));
            assert_eq!(table.alloc(), Some(2));
            table.dealloc(1);
            table.dealloc(2);
            assert_eq!(table.alloc(), Some(2));
            assert_eq!(table.alloc(), Some(1));
        }
    }

    #[test]
    fn index() {
        let table = PageTable::default();
        for i in [
            0,
            L0_FANOUT - 1,
            L0_FANOUT,
            L1_FANOUT - 1,
            L1_FANOUT,
            L2_FANOUT - 1,
        ] {
            table.set(i, i);
            assert_eq!(table.get(i), i);
        }
    }

    #[test]
    fn recover() {
        let mut builder = PageTableBuilder::default();
        builder.set(1, 1);
        builder.set(3, 3);
        builder.set(5, 5);
        let table = builder.build();
        assert_eq!(table.get(1), 1);
        assert_eq!(table.get(3), 3);
        assert_eq!(table.get(5), 5);
        unsafe {
            assert_eq!(table.alloc(), Some(2));
            assert_eq!(table.alloc(), Some(4));
            assert_eq!(table.alloc(), Some(6));
            table.dealloc(2);
            assert_eq!(table.alloc(), Some(2));
        }
    }
}
