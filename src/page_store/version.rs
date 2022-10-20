use std::{
    cell::RefCell,
    collections::{HashMap, HashSet},
    ops::Deref,
    rc::Rc,
    sync::{
        atomic::{AtomicPtr, Ordering},
        Arc,
    },
};

use crossbeam_epoch::Guard;

use super::{FileInfo, Result, WriteBuffer};

thread_local! {
    static VERSION: RefCell<Option<Rc<Version>>> = RefCell::new(None);
}

#[derive(Clone)]
pub(crate) struct Version {
    pub buffer_set: Arc<BufferSet>,
    pub files: HashMap<u32, FileInfo>,

    next: Arc<NextVersion>,
}

pub(crate) struct DeltaVersion {
    pub new_files: Vec<(u32, FileInfo)>,
    pub deleted_files: HashSet<u32>,
    pub deleted_pages: Vec<u64>,
}

#[derive(Default)]
pub(crate) struct NextVersion {
    raw_version: AtomicPtr<Version>,
}

pub(crate) struct BufferSet {
    write_buffer_capacity: u32,

    current: AtomicPtr<BufferSetVersion>,
}

pub(crate) struct BufferSetVersion {
    /// The range of the buffers referenced by the version, include
    /// `current_buffer`.
    buffers_range: std::ops::Range<u32>,
    sealed_buffers: Vec<Arc<WriteBuffer>>,

    /// The last write buffer, maybe it's already sealed.
    current_buffer: Arc<WriteBuffer>,
}

pub(crate) struct BufferSetRef<'a> {
    version: &'a BufferSetVersion,
    // `guard` is used to ensure that the referenced `BufferSetVersion` will not be released early.
    #[allow(unused)]
    guard: Guard,
}

impl Version {
    #[allow(unused)]
    pub(crate) fn new(write_buffer_capacity: u32) -> Self {
        Version {
            buffer_set: Arc::new(BufferSet::new(write_buffer_capacity)),
            files: HashMap::default(),
            next: Arc::default(),
        }
    }

    /// Construct [`Version`] from thread local storage.
    pub(crate) fn from_local() -> Option<Rc<Self>> {
        let current = Self::get_local();
        if let Some(version) = &current {
            if let Some(new) = version.next.refresh() {
                Self::set_local(new.clone());
                return Some(new);
            }
        }
        current
    }

    #[inline]
    pub(crate) fn set_local(version: Rc<Version>) {
        VERSION.with(move |v| {
            *v.borrow_mut() = Some(version);
        });
    }

    #[inline]
    fn get_local() -> Option<Rc<Self>> {
        VERSION.with(|v| v.borrow().clone())
    }

    /// Wait and construct next [`Version`].
    pub(crate) async fn wait_next_version(&self) -> Self {
        todo!()
    }

    pub(crate) fn active_write_buffer_id(&self) -> u32 {
        self.buffer_set.current().current_buffer.file_id()
    }

    /// Try install new version into
    pub(crate) fn install(&self, delta: DeltaVersion) -> Result<()> {
        todo!()
    }

    /// Fetch the files which obsolated but referenced by the [`Version`].
    pub(crate) fn deleted_files(&self) -> Vec<u32> {
        todo!()
    }
}

impl NextVersion {
    fn refresh(&self) -> Option<Rc<Version>> {
        let mut new: Option<Rc<Version>> = None;
        let mut raw = self.raw_version.load(Ordering::Acquire);
        loop {
            // Safety:
            // 1. It is valid and initialized since obtained from [`Box::into_raw`].
            // 2. All references are immutable.
            match unsafe { raw.as_ref() } {
                None => break,
                Some(version) => {
                    let version = Rc::new(version.clone());
                    raw = version.next.raw_version.load(Ordering::Acquire);
                    new = Some(version);
                }
            }
        }
        new
    }
}

impl Drop for NextVersion {
    fn drop(&mut self) {
        let raw = self.raw_version.load(Ordering::SeqCst);
        if !raw.is_null() {
            unsafe {
                // Safety: the backing memory is obtained from [`Box::into_raw`] and there no
                // any references to the memory.
                drop(Box::from_raw(raw));
            }
        }
    }
}

impl BufferSet {
    pub(crate) fn new(write_buffer_capacity: u32) -> BufferSet {
        let min_file_id = 0;
        let buf = WriteBuffer::with_capacity(min_file_id, write_buffer_capacity);
        let version = Box::new(BufferSetVersion {
            buffers_range: min_file_id..(min_file_id + 1),
            sealed_buffers: Vec::default(),
            current_buffer: Arc::new(buf),
        });
        let raw = Box::leak(version);
        BufferSet {
            write_buffer_capacity,
            current: AtomicPtr::new(raw),
        }
    }

    #[inline]
    pub(crate) fn write_buffer_capacity(&self) -> u32 {
        self.write_buffer_capacity
    }

    /// Obtains a reference of current [`BufferSetVersion`].
    pub(crate) fn current(&self) -> BufferSetRef<'_> {
        let guard = buffer_set_guard::pin();
        let current = unsafe { self.current_without_guard() };
        BufferSetRef {
            version: current,
            guard,
        }
    }

    /// Install new [`BufferSetVersion`] by replacing `current_buffer` to new
    /// [`WriteBuffer`].
    ///
    /// There are no concurrent requests here, because only the routine that
    /// seals the previous [`WriteBuffer`] can install the new [`WriteBuffer`].
    ///
    /// # Panic
    ///
    /// Panic if file IDs are not consecutive.
    pub(crate) fn install(&self, write_buffer: Arc<WriteBuffer>) {
        let guard = buffer_set_guard::pin();

        // Safety: guard by `buffer_set_guard::pin`.
        let current = unsafe { self.current_without_guard() };
        let next_file_id = current.buffers_range.end;
        let new_file_id = write_buffer.file_id();
        if new_file_id != next_file_id {
            panic!("the buffer {new_file_id} to be installed is not a successor of the previous buffers, expect {next_file_id}.");
        }

        let mut sealed_buffers = current.sealed_buffers.clone();
        sealed_buffers.push(current.current_buffer.clone());
        let new = Box::new(BufferSetVersion {
            buffers_range: current.buffers_range.start..(next_file_id + 1),
            sealed_buffers,
            current_buffer: write_buffer,
        });

        let current_ptr = current as *const _ as usize;
        self.current.store(Box::into_raw(new), Ordering::Release);
        guard.defer(move || unsafe {
            // Safety: the backing memory is obtained from [`Box::into_raw`] and there no
            // any references to the memory, which guarrantted by epoch based reclamation.
            drop(Box::from_raw(current_ptr as *mut BufferSetVersion));
        });
    }

    pub(crate) fn on_flushed(&self, files: &[u32]) {
        todo!()
    }

    pub(crate) async fn wait_flushable(&self) {
        todo!()
    }

    pub(crate) fn notify_flush_job(&self) {
        todo!()
    }

    /// Obtain current [`BufferSetVersion`].
    ///
    /// # Safety
    ///
    /// This should be guard by `buffer_set_guard::pin`.
    unsafe fn current_without_guard(&self) -> &BufferSetVersion {
        // Safety:
        // 1. Obtained from `Box::new`, so it is aligned and not null.
        // 2. There is not mutable references pointer to it.
        &*self.current.load(Ordering::Acquire)
    }
}

impl Drop for BufferSet {
    fn drop(&mut self) {
        let raw = self.current.load(Ordering::SeqCst);
        if !raw.is_null() {
            unsafe {
                // Safety: the backing memory is obtained from [`Box::into_raw`] and there no
                // any references to the memory, which guarrantted by
                // [`BufferSetRef`].
                drop(Box::from_raw(raw));
            }
        }
    }
}

impl BufferSetVersion {
    /// Read [`WriteBuffer`] of the specified `file_id`.
    ///
    /// If the user needs to access the [`WriteBuffer`] for a long time, use
    /// `clone` to make a copy.
    pub(crate) fn write_buffer(&self, file_id: u32) -> Option<&Arc<WriteBuffer>> {
        todo!()
    }

    #[inline]
    pub(crate) fn min_file_id(&self) -> u32 {
        self.buffers_range.start
    }

    #[inline]
    pub(crate) fn next_file_id(&self) -> u32 {
        self.buffers_range.end
    }
}

impl<'a> Deref for BufferSetRef<'a> {
    type Target = BufferSetVersion;

    fn deref(&self) -> &Self::Target {
        self.version
    }
}

mod buffer_set_guard {
    use crossbeam_epoch::{Collector, Guard, LocalHandle};
    use once_cell::sync::Lazy;

    static COLLECTOR: Lazy<Collector> = Lazy::new(Collector::new);

    thread_local! {
        static HANDLE: LocalHandle = COLLECTOR.register();
    }

    /// Pins the current thread.
    #[inline]
    pub(super) fn pin() -> Guard {
        with_handle(|handle| handle.pin())
    }

    /// Returns `true` if the current thread is pinned.
    #[allow(dead_code)]
    #[inline]
    pub(super) fn is_pinned() -> bool {
        with_handle(|handle| handle.is_pinned())
    }

    #[inline]
    fn with_handle<F, R>(mut f: F) -> R
    where
        F: FnMut(&LocalHandle) -> R,
    {
        HANDLE
            .try_with(|h| f(h))
            .unwrap_or_else(|_| f(&COLLECTOR.register()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn buffer_set_construct_and_drop() {
        drop(BufferSet::new(1 << 10));
    }

    #[test]
    fn buffer_set_write_buffer_install() {
        let buffer_set = BufferSet::new(1 << 10);
        let file_id = buffer_set.current().next_file_id();
        let buf = WriteBuffer::with_capacity(file_id, buffer_set.write_buffer_capacity());
        buffer_set.install(Arc::new(buf));
    }
}
