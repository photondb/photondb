use std::{
    collections::{HashMap, HashSet},
    ops::Deref,
    sync::{
        atomic::{AtomicPtr, Ordering},
        Arc, Mutex,
    },
};

use crossbeam_epoch::Guard;
use futures::channel::oneshot;
use log::debug;

use super::{FileInfo, WriteBuffer};
use crate::util::notify::Notify;

pub(crate) struct VersionOwner {
    raw: AtomicPtr<Arc<Version>>,
}

pub(crate) struct Version {
    pub(crate) buffer_set: Arc<BufferSet>,

    files: HashMap<u32, FileInfo>,

    // The id of the first buffer this version could access.
    first_buffer_id: u32,

    /// Records the ID of the file that can be deleted.
    obsoleted_files: HashSet<u32>,

    next_version: AtomicPtr<Arc<Version>>,
    new_version_notify: Notify,

    _cleanup_guard: oneshot::Sender<()>,
    cleanup_handle: Mutex<Option<oneshot::Receiver<()>>>,
}

pub(crate) struct DeltaVersion {
    pub(crate) file_id: u32,
    pub(crate) files: HashMap<u32, FileInfo>,
    pub(crate) obsoleted_files: HashSet<u32>,
}

pub(crate) struct BufferSet {
    write_buffer_capacity: u32,

    current: AtomicPtr<BufferSetVersion>,

    flush_notify: Notify,
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
    _guard: Guard,
}

pub(crate) struct BufferRef<'a> {
    buffer: &'a Arc<WriteBuffer>,
    _guard: Guard,
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
        let (sender, receiver) = oneshot::channel();
        let new = Box::new(Arc::new(Version {
            first_buffer_id,
            files: delta.files,
            obsoleted_files: delta.obsoleted_files,
            buffer_set,

            next_version: AtomicPtr::default(),
            new_version_notify: Notify::default(),
            _cleanup_guard: sender,
            cleanup_handle: Mutex::new(Some(receiver)),
        }));
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
        former.new_version_notify.notify_all();

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
        write_buffer_capacity: u32,
        next_file_id: u32,
        files: HashMap<u32, FileInfo>,
        obsoleted_files: HashSet<u32>,
    ) -> Self {
        let buffer_set = Arc::new(BufferSet::new(next_file_id, write_buffer_capacity));
        let first_buffer_id = next_file_id;
        let (sender, receiver) = oneshot::channel();
        Version {
            first_buffer_id,
            files,
            obsoleted_files,
            buffer_set,

            next_version: AtomicPtr::default(),
            new_version_notify: Notify::default(),
            _cleanup_guard: sender,
            cleanup_handle: Mutex::new(Some(receiver)),
        }
    }

    /// Wait and construct next [`Version`].
    pub(crate) async fn wait_next_version(&self) -> Arc<Self> {
        self.new_version_notify.notified().await;
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
    pub(crate) fn files(&self) -> &HashMap<u32, FileInfo> {
        &self.files
    }

    #[inline]
    pub(crate) fn get(&self, file_id: u32) -> Option<BufferRef> {
        self.buffer_set.get(file_id)
    }

    pub(crate) fn min_write_buffer(&self) -> Arc<WriteBuffer> {
        let current = self.buffer_set.current();
        current
            .get(self.first_buffer_id)
            .expect("First WriteBuffer referenced by this version must exists")
            .clone()
    }

    /// Release all previous writer buffers which is invisible.
    pub(crate) fn release_previous_buffers(&self) {
        self.buffer_set.release_until(self.first_buffer_id);
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

impl BufferSet {
    pub(crate) fn new(next_file_id: u32, write_buffer_capacity: u32) -> BufferSet {
        let buf = WriteBuffer::with_capacity(next_file_id, write_buffer_capacity);
        let version = Box::new(BufferSetVersion {
            buffers_range: next_file_id..(next_file_id + 1),
            sealed_buffers: Vec::default(),
            current_buffer: Arc::new(buf),
        });
        let raw = Box::leak(version);
        BufferSet {
            write_buffer_capacity,
            current: AtomicPtr::new(raw),
            flush_notify: Notify::new(),
        }
    }

    #[inline]
    pub(crate) fn write_buffer_capacity(&self) -> u32 {
        self.write_buffer_capacity
    }

    /// Obtains a reference of current [`BufferSetVersion`].
    pub(crate) fn current<'a>(&self) -> BufferSetRef<'a> {
        let guard = buffer_set_guard::pin();
        // Safety: guard by `buffer_set_guard::pin`.
        let current = unsafe { self.current_without_guard() };
        BufferSetRef {
            version: current,
            _guard: guard,
        }
    }

    /// Returns a reference to the write buffer corresponding to the `file_id`.
    pub(crate) fn get<'a>(&self, file_id: u32) -> Option<BufferRef<'a>> {
        let guard = buffer_set_guard::pin();
        // Safety: guard by `buffer_set_guard::pin`.
        let current = unsafe { self.current_without_guard() };
        let buffer = current.get(file_id)?;
        Some(BufferRef {
            buffer,
            _guard: guard,
        })
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
        let mut guard = buffer_set_guard::pin();

        // Safety: guard by `buffer_set_guard::pin`.
        let mut current = unsafe { self.current_without_guard() };
        loop {
            let next_file_id = current.buffers_range.end;
            let new_file_id = write_buffer.file_id();
            if new_file_id != next_file_id {
                panic!("the buffer {new_file_id} to be installed is not a successor of the previous buffers, expect {next_file_id}.");
            }

            let sealed_buffers = current.snapshot();
            let new = Box::new(BufferSetVersion {
                buffers_range: current.buffers_range.start..(next_file_id + 1),
                sealed_buffers,
                current_buffer: write_buffer.clone(),
            });

            debug!(
                "Install new buffer {}, buffer set range {:?}",
                new_file_id, new.buffers_range
            );

            match self.switch_version(guard, current, new) {
                Ok(_) => break,
                Err(v) => (guard, current) = v,
            };
        }
    }

    pub(crate) fn release_until(&self, first_buffer_id: u32) {
        debug!("Release write buffer until {first_buffer_id}");

        let mut guard = buffer_set_guard::pin();
        // Safety: guarded by `buffer_set_guard::pin`.
        let mut current = unsafe { self.current_without_guard() };
        loop {
            // Because versions are released one by one, should not be a gap in theory here.
            if current.min_buffer_id() + 1 != first_buffer_id {
                panic!(
                    "Release buffers not in order, current min is {}, but release until {}",
                    current.min_buffer_id(),
                    first_buffer_id
                );
            }

            let sealed_buffers = current
                .sealed_buffers
                .iter()
                .filter(|v| v.file_id() >= first_buffer_id)
                .cloned()
                .collect();
            let current_buffer = current.current_buffer.clone();
            let buffers_range = first_buffer_id..current.buffers_range.end;
            let new = Box::new(BufferSetVersion {
                buffers_range,
                sealed_buffers,
                current_buffer,
            });

            match self.switch_version(guard, current, new) {
                Ok(_) => break,
                Err(v) => (guard, current) = v,
            }
        }
    }

    #[inline]
    pub(crate) async fn wait_flushable(&self) {
        self.flush_notify.notified().await;
    }

    #[inline]
    pub(crate) fn notify_flush_job(&self) {
        log::info!("notify one flush job");
        self.flush_notify.notify_one();
    }

    /// Obtain current [`BufferSetVersion`].
    ///
    /// # Safety
    ///
    /// This should be guard by `buffer_set_guard::pin`.
    unsafe fn current_without_guard<'a>(&self) -> &'a BufferSetVersion {
        // Safety:
        // 1. Obtained from `Box::new`, so it is aligned and not null.
        // 2. There is not mutable references pointer to it.
        &*self.current.load(Ordering::Acquire)
    }

    /// Switch to the specified `BufferSetVersion`.
    ///
    /// [`Err`] is returned if the `current` has been changed.
    fn switch_version(
        &self,
        guard: Guard,
        current: &BufferSetVersion,
        new: Box<BufferSetVersion>,
    ) -> Result<(), (Guard, &BufferSetVersion)> {
        let new = Box::into_raw(new);
        let current = current as *const _ as usize;

        // No ABA problem here, since the `guard` guarantees that the `current` will not
        // be reclamation.
        match self.current.compare_exchange(
            current as *mut BufferSetVersion,
            new,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => {
                guard.defer(move || {
                    // Safety: the backing memory is obtained from [`Box::into_raw`] and there no
                    // any references to the memory, which guarrantted by epoch based reclamation.
                    drop(unsafe { Box::from_raw(current as *mut BufferSetVersion) });
                });

                // Get the defer function executed as soon as possible.
                guard.flush();
                Ok(())
            }
            Err(actual) => {
                // Safety: X_X
                drop(unsafe { Box::from_raw(new) });

                // Safety:
                // 1. Obtained from `Box::new`, so it is aligned and not null.
                // 2. There is not mutable references pointer to it.
                Err((guard, unsafe { &*actual }))
            }
        }
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
    #[inline]
    pub(crate) fn last_writer_buffer(&self) -> &WriteBuffer {
        self.current_buffer.as_ref()
    }

    /// Read [`WriteBuffer`] of the specified `file_id`.
    ///
    /// If the user needs to access the [`WriteBuffer`] for a long time, use
    /// `clone` to make a copy.
    pub(crate) fn get(&self, file_id: u32) -> Option<&Arc<WriteBuffer>> {
        use std::cmp::Ordering;

        if !self.buffers_range.contains(&file_id) {
            return None;
        }

        let index = (file_id - self.buffers_range.start) as usize;
        match index.cmp(&self.sealed_buffers.len()) {
            Ordering::Less => Some(&self.sealed_buffers[index]),
            Ordering::Equal => Some(&self.current_buffer),
            Ordering::Greater => panic!("buffers_range is invalid"),
        }
    }

    #[inline]
    pub(crate) fn min_buffer_id(&self) -> u32 {
        self.buffers_range.start
    }

    #[inline]
    pub(crate) fn next_buffer_id(&self) -> u32 {
        self.buffers_range.end
    }

    fn snapshot(&self) -> Vec<Arc<WriteBuffer>> {
        let mut buffers = self.sealed_buffers.clone();
        buffers.push(self.current_buffer.clone());
        buffers
    }
}

impl<'a> Deref for BufferSetRef<'a> {
    type Target = BufferSetVersion;

    fn deref(&self) -> &Self::Target {
        self.version
    }
}

impl<'a> Deref for BufferRef<'a> {
    type Target = Arc<WriteBuffer>;

    fn deref(&self) -> &Self::Target {
        self.buffer
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
    use std::sync::atomic::AtomicU32;

    use super::*;

    #[test]
    fn buffer_set_construct_and_drop() {
        drop(BufferSet::new(1, 1 << 10));
    }

    #[test]
    fn buffer_set_write_buffer_install() {
        let buffer_set = BufferSet::new(1, 1 << 10);
        let file_id = buffer_set.current().next_buffer_id();
        let buf = WriteBuffer::with_capacity(file_id, buffer_set.write_buffer_capacity());
        buffer_set.install(Arc::new(buf));
    }

    #[test]
    fn buffer_set_write_buffer_install_and_release() {
        let buffer_set = BufferSet::new(1, 1 << 10);
        let file_id = buffer_set.current().last_writer_buffer().file_id();

        // 1. seal current.
        {
            buffer_set.current().last_writer_buffer().seal().unwrap();
        }

        // 2. install
        {
            let buf = WriteBuffer::with_capacity(file_id + 1, buffer_set.write_buffer_capacity());
            buffer_set.install(Arc::new(buf));
        }

        // 3. seal current again.
        {
            buffer_set.current().last_writer_buffer().seal().unwrap();
        }

        // 4. release former buffer.
        buffer_set.release_until(file_id + 1);

        assert!(buffer_set.current().get(file_id).is_none());
        assert!(buffer_set.current().get(file_id + 1).is_some());
    }

    #[photonio::test]
    async fn buffer_set_concurrent_update() {
        let buffer_set = Arc::new(BufferSet::new(1, 1 << 10));
        let file_id = buffer_set.current().last_writer_buffer().file_id();
        let first_active_buffer_id = Arc::new(AtomicU32::new(file_id));
        let cloned_first_active_buffer_id = first_active_buffer_id.clone();
        let cloned_buffer_set = buffer_set.clone();
        let handle_1 = photonio::task::spawn(async move {
            let mut file_id = file_id;
            while file_id < 100000 {
                cloned_buffer_set
                    .current()
                    .last_writer_buffer()
                    .seal()
                    .unwrap();

                file_id += 1;
                let buf = WriteBuffer::with_capacity(file_id, 1 << 10);
                cloned_buffer_set.install(Arc::new(buf));
                cloned_first_active_buffer_id.store(file_id, Ordering::Release);
                photonio::task::yield_now().await;
            }
        });
        let handle_2 = photonio::task::spawn(async move {
            let mut file_id = file_id;
            while file_id < 100000 {
                while first_active_buffer_id.load(Ordering::Acquire) <= file_id {
                    photonio::task::yield_now().await;
                }

                let buf = buffer_set
                    .current()
                    .get(file_id)
                    .cloned()
                    .expect("WriteBuffer must exists");
                assert!(buf.is_flushable());
                buffer_set.release_until(file_id + 1);
                file_id += 1;
            }
        });
        handle_1.await.unwrap_or_default();
        handle_2.await.unwrap_or_default();
    }

    #[photonio::test]
    async fn buffer_set_write_buffer_flush_wait_and_notify() {
        let buffer_set = Arc::new(BufferSet::new(1, 1 << 10));
        let cloned_buffer_set = buffer_set.clone();
        let handle = photonio::task::spawn(async move {
            cloned_buffer_set.wait_flushable().await;
        });

        // 1. seal previous buffer.
        buffer_set.current().current_buffer.seal().unwrap();

        let file_id = buffer_set.current().next_buffer_id();
        let buf = WriteBuffer::with_capacity(file_id, buffer_set.write_buffer_capacity());
        buffer_set.install(Arc::new(buf));

        buffer_set.notify_flush_job();
        handle.await.unwrap();
    }

    #[test]
    fn buffer_set_write_buffer_switch_release() {
        let buffer_set = BufferSet::new(1, 1 << 10);
        let (file_id, buf) = {
            let current = buffer_set.current();
            let buf = current.last_writer_buffer();
            let file_id = buf.file_id();
            (file_id, current.get(file_id).unwrap().clone())
        };
        assert_eq!(Arc::strong_count(&buf), 2);

        buf.seal().unwrap();

        // Install new buf.
        buffer_set.install(Arc::new(WriteBuffer::with_capacity(
            file_id + 1,
            buffer_set.write_buffer_capacity(),
        )));
        buffer_set.release_until(file_id + 1);

        // Advance epoch and reclaim [`BufferSetVersion`].
        loop {
            let guard = buffer_set_guard::pin();
            guard.flush();
            if Arc::strong_count(&buf) == 1 {
                break;
            }

            // Wait until other processes touching buffer_set_guard have released the guard
            // so the epoch can be advanced.
            std::thread::yield_now();
        }

        assert_eq!(Arc::strong_count(&buf), 1);
    }

    #[test]
    fn version_access_newly_buffers() {
        let version = Version::new(1 << 10, 1, HashMap::default(), HashSet::new());
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
}
