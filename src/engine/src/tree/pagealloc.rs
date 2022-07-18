use std::{
    alloc::{GlobalAlloc, Layout},
    slice,
    sync::atomic::{AtomicUsize, Ordering},
};

use jemallocator::{usable_size, Jemalloc};

use super::page::PageBuf;

pub struct PageAlloc {
    size: AtomicUsize,
    limit: usize,
}

impl Default for PageAlloc {
    fn default() -> Self {
        Self::with_limit(0)
    }
}

impl PageAlloc {
    pub fn with_limit(limit: usize) -> Self {
        Self {
            size: AtomicUsize::new(0),
            limit,
        }
    }

    pub fn alloc(&self, size: usize) -> Option<PageBuf> {
        let layout = page_layout(size);
        if self.size.load(Ordering::Relaxed) + size > self.limit {
            return None;
        }
        unsafe {
            let ptr = Jemalloc.alloc(layout);
            self.size.fetch_add(usable_size(ptr), Ordering::Relaxed);
            let buf = Box::from_raw(slice::from_raw_parts_mut(ptr, size));
            Some(buf.into())
        }
    }

    pub fn dealloc(&self, page: PageBuf) {
        let buf: Box<[u8]> = page.into();
        let layout = page_layout(buf.len());
        unsafe {
            let ptr = Box::into_raw(buf) as *mut u8;
            self.size.fetch_sub(usable_size(ptr), Ordering::Relaxed);
            Jemalloc.dealloc(ptr, layout);
        }
    }
}

fn page_layout(size: usize) -> Layout {
    unsafe { Layout::from_size_align_unchecked(size, 8) }
}
