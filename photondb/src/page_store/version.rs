use std::{
    collections::{HashMap, HashSet},
    sync::{
        atomic::{AtomicPtr, Ordering},
        Arc, Mutex,
    },
};

use crossbeam_epoch::Guard;
use futures::channel::oneshot;
use log::debug;

use super::{buffer_set::*, FileInfo, MapFileMeta, WriteBuffer};
use crate::util::latch::Latch;

pub(crate) struct VersionOwner {
    raw: AtomicPtr<Arc<Version>>,
}

pub(crate) struct Version {
    pub(crate) buffer_set: Arc<BufferSet>,

    page_files: HashMap<u32, FileInfo>,
    map_files: HashMap<u32, Arc<MapFileMeta>>,

    // The id of the first buffer this version could access.
    first_buffer_id: u32,

    /// Records the ID of the file that can be deleted.
    obsoleted_files: HashSet<u32>,

    next_version: AtomicPtr<Arc<Version>>,
    new_version_latch: Latch,

    _cleanup_guard: oneshot::Sender<()>,
    cleanup_handle: Mutex<Option<oneshot::Receiver<()>>>,
}

pub(crate) struct DeltaVersion {
    pub(crate) file_id: u32,
    pub(crate) page_files: HashMap<u32, FileInfo>,
    pub(crate) map_files: HashMap<u32, Arc<MapFileMeta>>,
    pub(crate) obsoleted_files: HashSet<u32>,
}

impl VersionOwner {
    pub(crate) fn new(version: Version) -> Self {
        let version = Box::new(Arc::new(version));
        VersionOwner {
            raw: AtomicPtr::new(Box::into_raw(version)),
        }
    }

    /// Obtains a reference of current [`Version`].
    #[inline]
    pub(crate) fn current(&self) -> Arc<Version> {
        let _guard = version_guard::pin();
        // Safety: guard by `buffer_set_guard::pin`.
        unsafe { self.current_without_guard() }.clone()
    }

    /// Obtain current [`Version`].
    ///
    /// # Safety
    ///
    /// This should be guard by `version_guard::pin`.
    #[inline]
    unsafe fn current_without_guard(&self) -> &Arc<Version> {
        // Safety:
        // 1. Obtained from `Box::new`, so it is aligned and not null.
        // 2. There is not mutable references pointer to it.
        &*self.raw.load(Ordering::Acquire)
    }

    /// Try install new version into version chains.
    ///
    /// TODO: It is assumed that all installations come in [`WriteBuffer`]
    /// order, so there is no need to consider concurrency issues.
    pub(crate) fn install(&self, delta: DeltaVersion) {
        let guard = version_guard::pin();

        // Safety: guard by `buffer_set_guard::pin`.
        let current = unsafe { self.current_without_guard() };
        if current.first_buffer_id != delta.file_id {
            panic!(
                "Version should be installed in the order of write buffers, expect {}, but got {}",
                current.first_buffer_id, delta.file_id
            );
        }

        debug!("Install new version with file {}", delta.file_id);

        let buffer_set = current.buffer_set.clone();
        let first_buffer_id = current.first_buffer_id + 1;
        let new = Version::with_buffer_set(
            first_buffer_id,
            buffer_set,
            delta.page_files,
            delta.map_files,
            delta.obsoleted_files,
        );
        let new = Box::new(Arc::new(new));
        self.switch_version(new, guard);
    }

    /// Switch to new version.
    ///
    /// # Panic
    ///
    /// Panic if there has already exists a version.
    #[allow(clippy::redundant_allocation)]
    fn switch_version(&self, next: Box<Arc<Version>>, guard: Guard) {
        let raw_former = self.raw.load(Ordering::Acquire);
        let raw_next = Box::into_raw(next.clone());
        self.raw
            .compare_exchange(raw_former, raw_next, Ordering::AcqRel, Ordering::Acquire)
            .expect("There has already exists a version");

        // Safety:
        // 1. Obtained from `Box::new`, so it is aligned and not null.
        // 2. There is not mutable references pointer to it.
        let former = unsafe { &*raw_former };

        former
            .next_version
            .compare_exchange(
                std::ptr::null_mut(),
                Box::into_raw(next),
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .expect("There has already exists a version");
        former.new_version_latch.count_down();

        let raw_former = raw_former as usize;
        guard.defer(move || {
            // Safety:
            // 1. Obtained from `Box::new`, so it is aligned and not null.
            // 2. There is not mutable references pointer to it.
            drop(unsafe { Box::from_raw(raw_former as *mut Arc<Version>) });
        });
        // Get the defer function executed as soon as possible.
        guard.flush();
    }
}

impl Drop for VersionOwner {
    fn drop(&mut self) {
        let raw = self.raw.load(Ordering::SeqCst);
        if !raw.is_null() {
            unsafe {
                // Safety: the backing memory is obtained from [`Box::into_raw`] and there no
                // any references to the memory.
                drop(Box::from_raw(raw));
            }
        }
    }
}

impl Version {
    pub(crate) fn new(
        buffer_capacity: u32,
        next_file_id: u32,
        max_sealed_buffers: usize,
        page_files: HashMap<u32, FileInfo>,
        map_files: HashMap<u32, Arc<MapFileMeta>>,
        obsoleted_files: HashSet<u32>,
    ) -> Self {
        let buffer_set = Arc::new(BufferSet::new(
            next_file_id,
            buffer_capacity,
            max_sealed_buffers,
        ));
        Self::with_buffer_set(
            next_file_id,
            buffer_set,
            page_files,
            map_files,
            obsoleted_files,
        )
    }

    pub(crate) fn with_buffer_set(
        first_buffer_id: u32,
        buffer_set: Arc<BufferSet>,
        page_files: HashMap<u32, FileInfo>,
        map_files: HashMap<u32, Arc<MapFileMeta>>,
        obsoleted_files: HashSet<u32>,
    ) -> Self {
        let (sender, receiver) = oneshot::channel();
        Version {
            first_buffer_id,
            page_files,
            map_files,
            obsoleted_files,
            buffer_set,

            next_version: AtomicPtr::default(),
            new_version_latch: Latch::new(1),
            _cleanup_guard: sender,
            cleanup_handle: Mutex::new(Some(receiver)),
        }
    }

    /// Wait and construct next [`Version`].
    pub(crate) async fn wait_next_version(&self) -> Arc<Self> {
        self.new_version_latch.wait().await;
        self.try_next().expect("New version has been installed")
    }

    /// Wait until all reference pointed to the [`Version`] has been released.
    ///
    /// There can only be one waiter per [`Version`].
    pub(crate) async fn wait_version_released(self: Arc<Self>) {
        let handle = {
            self.cleanup_handle
                .lock()
                .expect("Poisoned")
                .take()
                .expect("There can only be one waiter per version")
        };
        drop(self);
        handle.await.unwrap_or_default();
    }

    /// Fetch the files which obsoleted but referenced by former [`Version`]s.
    #[inline]
    pub(crate) fn obsoleted_files(&self) -> Vec<u32> {
        self.obsoleted_files.iter().cloned().collect()
    }

    #[inline]
    pub(crate) fn page_files(&self) -> &HashMap<u32, FileInfo> {
        &self.page_files
    }

    #[allow(unused)]
    #[inline]
    pub(crate) fn map_files(&self) -> &HashMap<u32, Arc<MapFileMeta>> {
        &self.map_files
    }

    #[inline]
    pub(crate) fn get(&self, file_id: u32) -> Option<BufferRef> {
        if self.first_buffer_id <= file_id {
            self.buffer_set.get(file_id)
        } else {
            None
        }
    }

    pub(crate) fn min_write_buffer(&self) -> Arc<WriteBuffer> {
        let current = self.buffer_set.current();
        current
            .get(self.first_buffer_id)
            .expect("The buffer of first buffer id must exists")
            .clone()
    }

    /// Release all previous writer buffers which is invisible.
    pub(crate) fn release_previous_buffers(&self) {
        self.buffer_set.release_until(self.first_buffer_id);
    }

    #[inline]
    pub(crate) fn has_next_version(&self) -> bool {
        !self.next_version.load(Ordering::Acquire).is_null()
    }

    #[inline]
    pub(crate) fn try_next(&self) -> Option<Arc<Version>> {
        Self::try_next_impl(self.next_version.load(Ordering::Acquire))
    }

    #[inline]
    pub(crate) fn refresh(&self) -> Option<Arc<Version>> {
        let mut new: Option<Arc<Version>> = None;
        let mut raw = self.next_version.load(Ordering::Acquire);
        loop {
            match Self::try_next_impl(raw) {
                None => break,
                Some(version) => {
                    raw = version.next_version.load(Ordering::Acquire);
                    new = Some(version);
                }
            }
        }
        new
    }

    #[inline]
    fn try_next_impl(raw: *mut Arc<Version>) -> Option<Arc<Version>> {
        // Safety:
        // 1. It is valid and initialized since obtained from [`Box::into_raw`].
        // 2. All references are immutable.
        unsafe { raw.as_ref() }.cloned()
    }
}

impl Drop for Version {
    fn drop(&mut self) {
        let raw = self.next_version.load(Ordering::SeqCst);
        if !raw.is_null() {
            unsafe {
                // Safety: the backing memory is obtained from [`Box::into_raw`] and there no
                // any references to the memory.
                drop(Box::from_raw(raw));
            }
        }
    }
}

mod version_guard {
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
    fn version_access_newly_buffers() {
        let version = Version::new(
            1 << 10,
            1,
            8,
            HashMap::default(),
            HashMap::default(),
            HashSet::new(),
        );
        let buffer_id = version.first_buffer_id;
        for i in 1..100 {
            let buf = Arc::new(WriteBuffer::with_capacity(buffer_id + i, 1 << 10));
            version.buffer_set.install(buf);
        }

        // All buffers are accessable from this version.
        for i in 0..100 {
            assert!(version.get(buffer_id + i).is_some());
        }
    }

    #[test]
    fn version_access_unguarded_buffers() {
        let version = Version::new(
            1 << 10,
            1,
            8,
            HashMap::default(),
            HashMap::default(),
            HashSet::new(),
        );
        let owner = VersionOwner::new(version);
        let version = owner.current();
        let buffer_id = {
            let current = version.buffer_set.current();
            let buf = current.last_writer_buffer();
            buf.seal().unwrap();
            buf.file_id()
        };

        // install new buffer.
        {
            let buf = Arc::new(WriteBuffer::with_capacity(buffer_id + 1, 32));
            version.buffer_set.install(buf);
        }
        drop(version);

        // install new version
        owner.install(DeltaVersion {
            file_id: buffer_id,
            page_files: HashMap::default(),
            map_files: HashMap::default(),
            obsoleted_files: HashSet::new(),
        });

        // now latest version could not access former buffer since no guard held.
        let version = owner.current();
        assert!(version.get(buffer_id).is_none());
        assert!(version.get(buffer_id + 1).is_some());
    }
}
