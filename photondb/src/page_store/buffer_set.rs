use std::{
    sync::{
        atomic::{AtomicPtr, Ordering},
        Arc,
    },
    time::Instant,
};

use crossbeam_epoch::Guard;
use log::{debug, info};

use super::{
    stats::{AtomicBufferSetStats, BufferSetStats},
    write_buffer::ReleaseState,
    FlushOptions, Result, WriteBuffer,
};
use crate::util::notify::Notify;

pub(crate) struct BufferSet {
    buffer_capacity: u32,
    max_sealed_buffers: usize,

    current: AtomicPtr<BufferSetVersion>,

    flush_notify: Notify,
    write_buffer_permits: buffer_permits::WriteBufferPermits,

    stats: AtomicBufferSetStats,
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

impl BufferSet {
    pub(crate) fn new(
        next_file_id: u32,
        buffer_capacity: u32,
        max_sealed_buffers: usize,
    ) -> BufferSet {
        let buf = WriteBuffer::with_capacity(next_file_id, buffer_capacity);
        let version = Box::new(BufferSetVersion {
            buffers_range: next_file_id..(next_file_id + 1),
            sealed_buffers: Vec::default(),
            current_buffer: Arc::new(buf),
        });
        let raw = Box::leak(version);

        // Acquire permit for `buf`.
        let permits = max_sealed_buffers - 1;
        let write_buffer_permits = buffer_permits::WriteBufferPermits::new(permits);
        BufferSet {
            buffer_capacity,
            max_sealed_buffers,
            current: AtomicPtr::new(raw),
            flush_notify: Notify::new(),
            write_buffer_permits,
            stats: AtomicBufferSetStats::default(),
        }
    }

    #[inline]
    pub(crate) fn stats(&self) -> BufferSetStats {
        self.stats.snapshot()
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
                .collect::<Vec<_>>();
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

    /// Release a permit of write buffer, and wait new buffer to be installed.
    ///
    /// NOTE: This function is only called by flush job.
    pub(crate) async fn release_permit_and_wait(&self, file_id: u32) {
        self.write_buffer_permits.release();
        self.write_buffer_permits
            .wait(buffer_permits::WaitKind::NoWaiter)
            .await;

        // Make sure that the successor buffer is installed, because `Version` requires
        // that the correspnding write buffer of `min_buffer_id` must exists.
        loop {
            {
                let buffer_set = self.current();
                if buffer_set.current_buffer.file_id() > file_id {
                    return;
                }
            }
            photonio::task::yield_now().await;
        }
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

    /// Acquire the buffer id of the active buffer.
    ///
    /// if the active buffer is not installed, wait for it to be installed.
    pub(crate) async fn acquire_active_buffer_id(&self) -> u32 {
        if let Some(id) = self.acquire_active_buffer_id_fast() {
            return id;
        }

        self.acquire_active_buffer_id_slow().await
    }

    fn acquire_active_buffer_id_fast(&self) -> Option<u32> {
        for _ in 0..16 {
            let buffer_set = self.current();
            if !buffer_set.current_buffer.is_sealed() {
                return Some(buffer_set.current_buffer.file_id());
            }
        }
        None
    }

    async fn acquire_active_buffer_id_slow(&self) -> u32 {
        loop {
            {
                let buffer_set = self.current();
                if !buffer_set.current_buffer.is_sealed() {
                    return buffer_set.current_buffer.file_id();
                }
            }

            if self
                .write_buffer_permits
                .wait(buffer_permits::WaitKind::NoWaiter)
                .await
            {
                // Maybe the new `WriteBuffer` is not installed yet.
                photonio::task::yield_now().await;
            }
        }
    }

    /// Like `switch_buffer` but no write stalling will occurs.
    pub(crate) async fn switch_buffer_without_stalling(&self, file_id: u32) {
        // Since a buffer can only be sealed once, if a buffer is sealed and has
        // available permits before sealing, then the switch buffer will not trigger
        // write stalling.
        self.write_buffer_permits
            .wait(buffer_permits::WaitKind::HasPermits)
            .await;
        self.switch_buffer(file_id).await;
    }

    /// Seal the corresponding write buffer and switch active buffer to new one.
    pub(crate) async fn switch_buffer(&self, file_id: u32) {
        let Some(release_state) = self.seal_buffer(file_id) else { return };
        self.install_successor(file_id).await;
        if matches!(release_state, ReleaseState::Flush) {
            self.notify_flush_job();
        }
    }

    /// Install the corresponding successor of `file_id`.
    async fn install_successor(&self, file_id: u32) {
        if self.write_buffer_permits.try_acquire().is_none() {
            info!(
                "Stalling writes because we have {} sealed write buffers (wait for flush)",
                self.max_sealed_buffers
            );
            let start_at = Instant::now();
            self.write_buffer_permits.acquire().await;
            self.stats.stall_writes.inc();
            self.stats
                .stall_intervals_ms
                .add(start_at.elapsed().as_millis() as u64);
        }

        let write_buffer = WriteBuffer::with_capacity(file_id + 1, self.buffer_capacity);
        self.install(Arc::new(write_buffer));
    }

    /// Seal the corresponding buffer.
    fn seal_buffer(&self, file_id: u32) -> Option<ReleaseState> {
        let buffer_set = self.current();
        let write_buffer = buffer_set
            .get(file_id)
            .expect("The write buffer should exists");

        write_buffer.seal().ok()
    }

    /// Seal the current write buffer and switch to new one, so the sealed
    /// buffer will be flushed by flusher.
    pub(crate) async fn flush_active_buffer(&self, opts: &FlushOptions) {
        let buffer = {
            let current = self.current();
            if current.current_buffer.is_empty() {
                return;
            }
            current.current_buffer.clone()
        };

        let file_id = buffer.file_id();
        if opts.allow_write_stall {
            self.switch_buffer(file_id).await;
        } else {
            self.switch_buffer_without_stalling(file_id).await;
        }

        if opts.wait {
            buffer.wait_flushed().await;
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

impl<'a> std::ops::Deref for BufferSetRef<'a> {
    type Target = BufferSetVersion;

    fn deref(&self) -> &Self::Target {
        self.version
    }
}

impl<'a> std::ops::Deref for BufferRef<'a> {
    type Target = Arc<WriteBuffer>;

    fn deref(&self) -> &Self::Target {
        self.buffer
    }
}

mod buffer_permits {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use crate::util::notify::Notify;

    /// A permit for maxmum sealed buffers.
    pub(crate) struct WriteBufferPermits {
        // cases
        // - 0: has waiter
        // - 1: no waiter but no available permits
        // - 2 or larger: has permits
        permits: AtomicUsize,
        notify: Notify,
    }

    enum AcquireKind {
        None,
        AddWaiter,
    }

    pub(crate) enum WaitKind {
        /// Wait if there no available permits.
        HasPermits,
        /// Wait if exists a waiter.
        NoWaiter,
    }

    impl WriteBufferPermits {
        pub(crate) fn new(permits: usize) -> WriteBufferPermits {
            // Add one for recording waiter.
            let permits = permits + 1;
            WriteBufferPermits {
                permits: AtomicUsize::new(permits),
                notify: Notify::default(),
            }
        }

        /// Not acquire a permit, but wait until the contdition are satisfied.
        pub(crate) async fn wait(&self, kind: WaitKind) -> bool {
            let acquired = match kind {
                WaitKind::HasPermits => 1,
                WaitKind::NoWaiter => 0,
            };
            if self.permits.load(Ordering::Acquire) > acquired {
                true
            } else {
                self.wait_cond(|| self.permits.load(Ordering::Acquire) > acquired)
                    .await;
                false
            }
        }

        /// Try acquire a permit, [`None`] is returned if no available permits.
        pub(crate) fn try_acquire(&self) -> Option<()> {
            self.acquire_fast(AcquireKind::None)
        }

        /// Acquire a permit, wait if there no available permits.
        pub(crate) async fn acquire(&self) {
            if self.acquire_fast(AcquireKind::AddWaiter).is_some() {
                return;
            }

            self.acquire_slow().await
        }

        /// Release the acquired permit and wake waiters if any.
        pub(crate) fn release(&self) {
            let mut current = self.permits.load(Ordering::Acquire);
            loop {
                let new = if current == 0 { 2 } else { current + 1 };
                current = match self.permits.compare_exchange(
                    current,
                    new,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => {
                        break;
                    }
                    Err(permits) => permits,
                }
            }

            if current == 0 {
                self.notify.notify_waiters();
            }
        }

        fn acquire_fast(&self, kind: AcquireKind) -> Option<()> {
            let mut current = self.permits.load(Ordering::Acquire);
            let min_permit = match kind {
                AcquireKind::None => 1,
                AcquireKind::AddWaiter => 0,
            };
            while min_permit < current {
                let new = current - 1;
                current = match self.permits.compare_exchange(
                    current,
                    new,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => break,
                    Err(permits) => permits,
                };
            }
            if current > 1 {
                Some(())
            } else {
                None
            }
        }

        async fn acquire_slow(&self) {
            self.wait_cond(|| self.acquire_fast(AcquireKind::AddWaiter).is_some())
                .await
        }

        async fn wait_cond<F>(&self, cond_fn: F)
        where
            F: Fn() -> bool,
        {
            let notified = self.notify.notified();
            futures::pin_mut!(notified);
            loop {
                // Make sure that no wake-up is lost.
                notified.as_mut().enable();
                if cond_fn() {
                    return;
                }

                notified.as_mut().await;
                notified.set(self.notify.notified());
            }
        }
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
    use std::sync::atomic::AtomicU32;

    use futures::{channel::mpsc, SinkExt, StreamExt};

    use super::*;

    impl BufferSetVersion {
        #[inline]
        pub(crate) fn last_writer_buffer(&self) -> &WriteBuffer {
            self.current_buffer.as_ref()
        }
    }

    #[test]
    fn buffer_set_construct_and_drop() {
        drop(BufferSet::new(1, 1 << 10, 8));
    }

    #[test]
    fn buffer_set_write_buffer_install() {
        let buffer_set = BufferSet::new(1, 1 << 10, 8);
        let file_id = buffer_set.current().next_buffer_id();
        let buf = WriteBuffer::with_capacity(file_id, buffer_set.buffer_capacity);
        buffer_set.install(Arc::new(buf));
    }

    #[test]
    fn buffer_set_write_buffer_install_and_release() {
        let buffer_set = BufferSet::new(1, 1 << 10, 8);
        let file_id = buffer_set.current().last_writer_buffer().file_id();

        // 1. seal current.
        {
            buffer_set.current().last_writer_buffer().seal().unwrap();
        }

        // 2. install
        {
            let buf = WriteBuffer::with_capacity(file_id + 1, buffer_set.buffer_capacity);
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
        let buffer_set = Arc::new(BufferSet::new(1, 32, 8));
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

                // Avoid contention.
                while file_id - cloned_buffer_set.current().min_buffer_id() > 100 {
                    photonio::task::yield_now().await;
                }
                let buf = WriteBuffer::with_capacity(file_id, 32);
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
                file_id += 1;
                buffer_set.release_until(file_id);
                photonio::task::yield_now().await;
            }
        });
        handle_1.await.unwrap_or_default();
        handle_2.await.unwrap_or_default();
    }

    #[photonio::test]
    async fn buffer_set_write_buffer_flush_wait_and_notify() {
        let buffer_set = Arc::new(BufferSet::new(1, 1 << 10, 8));
        let cloned_buffer_set = buffer_set.clone();
        let handle = photonio::task::spawn(async move {
            cloned_buffer_set.wait_flushable().await;
        });

        // 1. seal previous buffer.
        buffer_set.current().current_buffer.seal().unwrap();

        let file_id = buffer_set.current().next_buffer_id();
        let buf = WriteBuffer::with_capacity(file_id, buffer_set.buffer_capacity);
        buffer_set.install(Arc::new(buf));

        buffer_set.notify_flush_job();
        handle.await.unwrap();
    }

    #[test]
    fn buffer_set_write_buffer_switch_release() {
        let buffer_set = BufferSet::new(1, 1 << 10, 8);
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
            buffer_set.buffer_capacity,
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

    #[photonio::test]
    async fn buffer_set_flush_active_buffer() {
        let buffer_set = BufferSet::new(1, 1 << 10, 8);

        let opts = FlushOptions {
            wait: false,
            allow_write_stall: false,
        };
        buffer_set.flush_active_buffer(&opts).await;

        {
            let current = buffer_set.current();
            let buf = current.last_writer_buffer();
            unsafe { buf.alloc_page(1, 32, false).unwrap() };
        };
        buffer_set.flush_active_buffer(&opts).await;
    }

    #[photonio::test]
    async fn buffer_set_flush_active_buffer_and_wait() {
        let buffer_set = BufferSet::new(1, 1 << 10, 8);

        {
            let current = buffer_set.current();
            let buf = current.current_buffer.clone();
            unsafe { buf.alloc_page(1, 32, false).unwrap() };
            buf.on_flushed();
        }

        let opts = FlushOptions {
            wait: true,
            allow_write_stall: false,
        };

        buffer_set.flush_active_buffer(&opts).await;
    }

    #[photonio::test]
    async fn write_buffer_permits_basic() {
        let write_permits = Arc::new(buffer_permits::WriteBufferPermits::new(2));
        write_permits.acquire().await;
        write_permits.acquire().await;

        // Return immediately if there no waiter.
        assert!(write_permits.wait(buffer_permits::WaitKind::NoWaiter).await);

        let (tx, mut rx) = mpsc::channel(4);
        let mut tx_1 = tx.clone();
        let mut tx_2 = tx.clone();

        let cloned_write_permits = write_permits.clone();
        let acquire_task_1 = photonio::task::spawn(async move {
            tx_1.send(()).await.unwrap_or_default();
            cloned_write_permits.acquire().await;
        });

        let cloned_write_permits = write_permits.clone();
        let acquire_task_2 = photonio::task::spawn(async move {
            tx_2.send(()).await.unwrap_or_default();
            cloned_write_permits.acquire().await;
        });

        let cloned_write_permits = write_permits.clone();
        let wait_task = photonio::task::spawn(async move {
            cloned_write_permits
                .wait(buffer_permits::WaitKind::NoWaiter)
                .await;
        });

        photonio::task::yield_now().await;
        photonio::task::yield_now().await;
        photonio::task::yield_now().await;

        rx.next().await;
        rx.next().await;

        // Wake tasks
        write_permits.release();
        write_permits.release();
        wait_task.await.unwrap_or_default();
        acquire_task_1.await.unwrap_or_default();
        acquire_task_2.await.unwrap_or_default();

        assert!(write_permits.wait(buffer_permits::WaitKind::NoWaiter).await);
    }
}
