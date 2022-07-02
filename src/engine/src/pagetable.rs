use std::{
    mem::MaybeUninit,
    ops::Index,
    ptr::null_mut,
    sync::atomic::{AtomicPtr, AtomicUsize, Ordering},
};

pub struct PageTable {
    // Level 0: [0, L0_MAX)
    l0: Box<L0<L0_LEN>>,
    // Level 1: [L0_MAX, L1_MAX)
    l1: Box<L1<L1_LEN>>,
    // Level 2: [L1_MAX, L2_MAX)
    l2: Box<L2<L2_LEN>>,
    // The next id to allocate.
    next: AtomicUsize,
}

impl PageTable {
    pub fn new() -> Self {
        Self {
            l0: Box::default(),
            l1: Box::default(),
            l2: Box::default(),
            next: AtomicUsize::new(0),
        }
    }

    pub const fn nan() -> usize {
        L2_MAX
    }

    pub fn alloc(&self) -> Option<usize> {
        let mut id = self.next.load(Ordering::Relaxed);
        if id < L2_MAX {
            id = self.next.fetch_add(1, Ordering::Relaxed);
        }
        if id < L2_MAX {
            Some(id)
        } else {
            None
        }
    }
}

impl Default for PageTable {
    fn default() -> Self {
        Self::new()
    }
}

impl Index<usize> for PageTable {
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
        let table = PageTable::default();
        assert_eq!(table.alloc(), Some(0));
        assert_eq!(table.alloc(), Some(1));
    }

    #[test]
    fn test_index() {
        let table = PageTable::default();
        for i in [0, L0_MAX - 1, L0_MAX, L1_MAX - 1, L1_MAX, L2_MAX - 1] {
            table[i].store(i, Ordering::Relaxed);
            assert_eq!(table[i].load(Ordering::Relaxed), i);
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
    fn bench_table_l0(b: &mut Bencher) {
        bench::<PageTable>(b, 0);
    }

    #[bench]
    fn bench_table_l1(b: &mut Bencher) {
        bench::<PageTable>(b, L0_MAX);
    }

    #[bench]
    fn bench_table_l2(b: &mut Bencher) {
        bench::<PageTable>(b, L1_MAX);
    }
}
