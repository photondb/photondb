use std::{
    alloc::{GlobalAlloc, Layout},
    sync::atomic::{AtomicUsize, Ordering},
};

use jemallocator::{usable_size, Jemalloc};

use super::page::{PageAlloc, PagePtr, PAGE_ALIGNMENT};

pub struct PageCache {
    size: AtomicUsize,
    limit: usize,
}

impl Default for PageCache {
    fn default() -> Self {
        Self::with_limit(usize::MAX)
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

unsafe impl PageAlloc for PageCache {
    unsafe fn alloc(&self, size: usize) -> Option<PagePtr> {
        if self.size.load(Ordering::Relaxed) + size > self.limit {
            None
        } else {
            let ptr = Jemalloc.alloc(alloc_layout(size));
            self.size.fetch_add(usable_size(ptr), Ordering::Relaxed);
            PagePtr::new(ptr)
        }
    }

    unsafe fn dealloc(&self, page: PagePtr) {
        let ptr = page.as_ptr();
        let size = usable_size(ptr);
        self.size.fetch_sub(size, Ordering::Relaxed);
        Jemalloc.dealloc(ptr, alloc_layout(size));
    }
}

unsafe fn alloc_layout(size: usize) -> Layout {
    Layout::from_size_align_unchecked(size, PAGE_ALIGNMENT)
}
