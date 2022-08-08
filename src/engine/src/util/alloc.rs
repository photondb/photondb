use std::{
    alloc::{self, GlobalAlloc, Layout},
    mem::size_of,
};

/// An allocator that knows the size of allocated objects.
///
/// # Safety
///
/// Check `std::alloc::GlobalAlloc` for more details.
pub unsafe trait SizedAlloc: GlobalAlloc {
    /// Returns the size allocated to the object.
    ///
    /// # Safety
    ///
    /// The caller must ensure that the object was allocated via this allocator.
    unsafe fn alloc_size(&self, ptr: *mut u8) -> usize;
}

#[derive(Copy, Clone)]
pub struct Sysalloc;

unsafe impl SizedAlloc for Sysalloc {
    unsafe fn alloc_size(&self, ptr: *mut u8) -> usize {
        (ptr as *mut usize).sub(1).read()
    }
}

unsafe impl GlobalAlloc for Sysalloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        assert_eq!(layout.align(), size_of::<usize>());
        let size = size_of::<usize>() + layout.size();
        let layout = Layout::from_size_align_unchecked(size, layout.align());
        let ptr = alloc::alloc(layout) as *mut usize;
        ptr.write(size);
        ptr.add(1) as *mut u8
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        assert_eq!(layout.align(), size_of::<usize>());
        let ptr = (ptr as *mut usize).sub(1);
        let size = ptr.read();
        let layout = Layout::from_size_align_unchecked(size, layout.align());
        alloc::dealloc(ptr as *mut u8, layout);
    }
}

#[derive(Copy, Clone)]
pub struct Jemalloc;

unsafe impl SizedAlloc for Jemalloc {
    unsafe fn alloc_size(&self, ptr: *mut u8) -> usize {
        jemallocator::usable_size(ptr)
    }
}

unsafe impl GlobalAlloc for Jemalloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        jemallocator::Jemalloc.alloc(layout)
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        jemallocator::Jemalloc.dealloc(ptr, layout)
    }
}
