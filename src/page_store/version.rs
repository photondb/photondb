use std::{
    cell::RefCell,
    collections::{HashMap, HashSet},
    ops::Deref,
    sync::{
        atomic::{AtomicPtr, Ordering},
        Arc, Mutex,
    },
};

use crossbeam_epoch::Guard;
use futures::channel::oneshot;

use super::{FileInfo, Result, WriteBuffer};
use crate::util::notify::Notify;

thread_local! {
    static VERSION: RefCell<Option<Arc<Version>>> = RefCell::new(None);
}

#[derive(Clone)]
pub(crate) struct Version {
    pub(crate) buffer_set: Arc<BufferSet>,

    inner: Arc<VersionInner>,
    next: Arc<NextVersion>,
}

struct VersionInner {
    buffers_range: std::ops::Range<u32>,

    /// Holds a reference to all [`WriteBuffer`]s at the time of the version
    /// creation.
    ///
    /// The difference with [`BufferSet`] is that the [`WriteBuffer`] held by
    /// [`VersionInner`] may have been flushed and [`BufferSet`] has lost access
    /// to these [`WriteBuffers`]. The advantage of this is that
    /// implementation don't have to hold the [`BufferSetRef`] for a long time
    /// throughout the lifetime of the [`Version`].
    write_buffers: Vec<Arc<WriteBuffer>>,
    files: HashMap<u32, FileInfo>,
    deleted_files: HashSet<u32>,
}

pub(crate) struct DeltaVersion {
    pub(crate) files: HashMap<u32, FileInfo>,
    pub(crate) deleted_files: HashSet<u32>,
}

pub(crate) struct NextVersion {
    raw_version: AtomicPtr<Version>,

    new_version_notify: Notify,

    _cleanup_guard: oneshot::Sender<()>,
    cleanup_handle: Mutex<Option<oneshot::Receiver<()>>>,
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

impl Version {
    pub(crate) fn new(
        write_buffer_capacity: u32,
        next_file_id: u32,
        files: HashMap<u32, FileInfo>,
        deleted_files: HashSet<u32>,
    ) -> Self {
        let buffer_set = Arc::new(BufferSet::new(next_file_id, write_buffer_capacity));
        let (buffers_range, write_buffers) = {
            let current = buffer_set.current();
            (current.buffers_range.clone(), current.snapshot())
        };
        let inner = Arc::new(VersionInner {
            buffers_range,
            write_buffers,
            files,
            deleted_files,
        });
        Version {
            buffer_set,
            inner,
            next: Arc::default(),
        }
    }

    /// Try install new version into version chains.
    ///
    /// TODO: It is assumed that all installations come in [`WriteBuffer`]
    /// order, so there is no need to consider concurrency issues.
    pub(crate) fn install(version: &Version, delta: DeltaVersion) -> Result<()> {
        let current = version;
        let (buffers_range, write_buffers) = {
            let buffers_ref = current.buffer_set.current();
            (buffers_ref.buffers_range.clone(), buffers_ref.snapshot())
        };

        let inner = Arc::new(VersionInner {
            buffers_range,
            write_buffers,
            files: delta.files,
            deleted_files: delta.deleted_files,
        });
        let new = Box::new(Version {
            buffer_set: current.buffer_set.clone(),
            inner,
            next: Arc::default(),
        });
        current.next.install(new);
        Ok(())
    }

    /// Construct [`Version`] from thread local storage.
    pub(crate) fn from_local() -> Option<Arc<Self>> {
        let current = Self::get_local();
        if let Some(version) = &current {
            if let Some(new) = version.next.refresh() {
                let new = Arc::new(new);
                Self::set_local(new.clone());
                return Some(new);
            }
        }
        current
    }

    #[inline]
    pub(crate) fn set_local(version: Arc<Version>) {
        VERSION.with(move |v| {
            *v.borrow_mut() = Some(version);
        });
    }

    #[inline]
    fn get_local() -> Option<Arc<Self>> {
        VERSION.with(|v| v.borrow().clone())
    }

    #[inline]
    pub(crate) fn refresh(&self) -> Option<Version> {
        self.next.refresh()
    }

    /// Wait and construct next [`Version`].
    pub(crate) async fn wait_next_version(&self) -> Self {
        self.next.new_version_notify.notified().await;
        self.next
            .try_next()
            .expect("New version has been installed")
    }

    /// Wait until all reference pointed to the [`Version`] has been released.
    ///
    /// There can only be one waiter per [`Version`].
    pub(crate) async fn wait_version_released(self) {
        let handle = {
            self.next
                .cleanup_handle
                .lock()
                .expect("Poisoned")
                .take()
                .expect("There can only be one waiter per version")
        };
        drop(self);
        handle.await.unwrap_or_default();
    }

    /// Fetch the files which obsolated but referenced by the [`Version`].
    #[inline]
    pub(crate) fn deleted_files(&self) -> Vec<u32> {
        self.inner.deleted_files.iter().cloned().collect()
    }

    #[inline]
    pub(crate) fn files(&self) -> &HashMap<u32, FileInfo> {
        &self.inner.files
    }

    #[inline]
    pub(crate) fn next_file_id(&self) -> u32 {
        self.inner.buffers_range.end
    }

    /// Invoke `f` with the specified [`WriteBuffer`].
    ///
    /// # Panic
    ///
    /// This function panics if the [`WriteBuffer`] of the specified `file_id`
    /// is not exists.
    pub(crate) fn with_write_buffer<F, O>(&self, file_id: u32, f: F) -> O
    where
        F: FnOnce(&WriteBuffer) -> O,
    {
        if self.inner.buffers_range.contains(&file_id) {
            let index = (file_id - self.inner.buffers_range.start) as usize;
            if index >= self.inner.write_buffers.len() {
                panic!("buffers_range is invalid");
            }
            return f(self.inner.write_buffers[index].as_ref());
        }

        // TODO: load write buffer from version directly.
        let current = self.buffer_set.current();
        let write_buffer = current.write_buffer(file_id).expect("No such write buffer");
        f(write_buffer.as_ref())
    }

    #[inline]
    pub(crate) fn contains_write_buffer(&self, file_id: u32) -> bool {
        self.inner.buffers_range.contains(&file_id)
            || self.buffer_set.current().write_buffer(file_id).is_some()
    }
}

impl NextVersion {
    /// Install new version.
    ///
    /// # Panic
    ///
    /// Panic if there has already exists a version.
    fn install(&self, version: Box<Version>) {
        let new = Box::into_raw(version);
        self.raw_version
            .compare_exchange(
                std::ptr::null_mut(),
                new,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .expect("There has already exists a version");

        self.new_version_notify.notify_all();
    }

    #[inline]
    fn try_next(&self) -> Option<Version> {
        Self::try_next_impl(self.raw_version.load(Ordering::Acquire))
    }

    #[inline]
    fn try_next_impl(raw: *mut Version) -> Option<Version> {
        // Safety:
        // 1. It is valid and initialized since obtained from [`Box::into_raw`].
        // 2. All references are immutable.
        unsafe { raw.as_ref() }.cloned()
    }

    fn refresh(&self) -> Option<Version> {
        let mut new: Option<Version> = None;
        let mut raw = self.raw_version.load(Ordering::Acquire);
        loop {
            match Self::try_next_impl(raw) {
                None => break,
                Some(version) => {
                    raw = version.next.raw_version.load(Ordering::Acquire);
                    new = Some(version);
                }
            }
        }
        new
    }
}

impl Default for NextVersion {
    fn default() -> Self {
        let (sender, receiver) = oneshot::channel();
        NextVersion {
            raw_version: AtomicPtr::default(),
            new_version_notify: Notify::default(),
            _cleanup_guard: sender,
            cleanup_handle: Mutex::new(Some(receiver)),
        }
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
        let current = unsafe { self.current_without_guard() };
        BufferSetRef {
            version: current,
            _guard: guard,
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

        let sealed_buffers = current.snapshot();
        let new = Box::new(BufferSetVersion {
            buffers_range: current.buffers_range.start..(next_file_id + 1),
            sealed_buffers,
            current_buffer: write_buffer,
        });

        self.switch_version(new, guard);
    }

    pub(crate) fn on_flushed(&self, file_id: u32) {
        let guard = buffer_set_guard::pin();
        // Safety: guarded by `buffer_set_guard::pin`.
        let current = unsafe { self.current_without_guard() };
        assert_eq!(current.min_file_id(), file_id);

        let sealed_buffers = {
            let mut buffers = current.sealed_buffers.clone();
            let buffer = buffers.pop().expect("Flushable WriteBuffer must be sealed");
            assert_eq!(buffer.file_id(), file_id);
            buffers
        };
        let current_buffer = current.current_buffer.clone();
        let new = Box::new(BufferSetVersion {
            buffers_range: (file_id + 1)..current.buffers_range.end,
            sealed_buffers,
            current_buffer,
        });

        self.switch_version(new, guard);
    }

    #[inline]
    pub(crate) async fn wait_flushable(&self) {
        self.flush_notify.notified().await;
    }

    #[inline]
    pub(crate) fn notify_flush_job(&self) {
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
    fn switch_version(&self, new: Box<BufferSetVersion>, guard: Guard) {
        let current = self.current.load(Ordering::Acquire) as usize;
        self.current.store(Box::into_raw(new), Ordering::Release);
        guard.defer(move || unsafe {
            // Safety: the backing memory is obtained from [`Box::into_raw`] and there no
            // any references to the memory, which guarrantted by epoch based reclamation.
            drop(Box::from_raw(current as *mut BufferSetVersion));
        });

        // Get the defer function executed as soon as possible.
        guard.flush();
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
    pub(crate) fn write_buffer(&self, file_id: u32) -> Option<&Arc<WriteBuffer>> {
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
    pub(crate) fn min_file_id(&self) -> u32 {
        self.buffers_range.start
    }

    #[cfg(test)]
    #[inline]
    pub(crate) fn next_file_id(&self) -> u32 {
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
        drop(BufferSet::new(1, 1 << 10));
    }

    #[test]
    fn buffer_set_write_buffer_install() {
        let buffer_set = BufferSet::new(1, 1 << 10);
        let file_id = buffer_set.current().next_file_id();
        let buf = WriteBuffer::with_capacity(file_id, buffer_set.write_buffer_capacity());
        buffer_set.install(Arc::new(buf));
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

        let file_id = buffer_set.current().next_file_id();
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
            (file_id, current.write_buffer(file_id).unwrap().clone())
        };
        assert_eq!(Arc::strong_count(&buf), 2);

        buf.seal().unwrap();

        // Install new buf.
        buffer_set.install(Arc::new(WriteBuffer::with_capacity(
            file_id + 1,
            buffer_set.write_buffer_capacity(),
        )));
        buffer_set.on_flushed(file_id);

        // Advance epoch and reclaim [`BufferSetVersion`].
        for _ in 0..128 {
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
}
