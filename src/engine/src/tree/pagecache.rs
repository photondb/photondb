use std::{
    alloc::GlobalAlloc,
    ptr::null_mut,
    sync::atomic::{AtomicUsize, Ordering},
};

use jemallocator::{usable_size, Jemalloc};

use super::page::UnsafeAlloc;

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

unsafe impl UnsafeAlloc for PageCache {
    unsafe fn alloc(&self, size: usize) -> *mut u8 {
        if self.size.load(Ordering::Relaxed) + size > self.limit {
            null_mut()
        } else {
            let ptr = Jemalloc.alloc(Self::alloc_layout(size));
            self.size.fetch_add(usable_size(ptr), Ordering::Relaxed);
            ptr
        }
    }

    unsafe fn dealloc(&self, ptr: *mut u8) {
        let size = usable_size(ptr);
        self.size.fetch_sub(size, Ordering::Relaxed);
        Jemalloc.dealloc(ptr, Self::alloc_layout(size));
    }
}
