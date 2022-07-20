use std::{
    alloc::{GlobalAlloc, Layout},
    ptr::null_mut,
    sync::atomic::{AtomicUsize, Ordering},
};

use jemallocator::{usable_size, Jemalloc};

pub struct PageCache {
    size: AtomicUsize,
    limit: usize,
}

impl Default for PageCache {
    fn default() -> Self {
        Self::with_limit(0)
    }
}

impl PageCache {
    pub fn with_limit(limit: usize) -> Self {
        Self {
            size: AtomicUsize::new(0),
            limit,
        }
    }
}

unsafe impl GlobalAlloc for PageCache {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if self.size.load(Ordering::Relaxed) + layout.size() > self.limit {
            null_mut()
        } else {
            let ptr = Jemalloc.alloc(layout);
            self.size.fetch_add(usable_size(ptr), Ordering::Relaxed);
            ptr
        }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        self.size.fetch_sub(usable_size(ptr), Ordering::Relaxed);
        Jemalloc.dealloc(ptr, layout);
    }
}
