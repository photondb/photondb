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

use super::{FileInfo, PageHandle, Result, WriteBuffer};
use crate::page::PagePtr;

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
    min_file_id: u32,

    sealed_buffers: Vec<Arc<WriteBuffer>>,
    active_buffer: Arc<WriteBuffer>,
}

pub(crate) struct BufferSetRef<'a> {
    version: &'a BufferSetVersion,
    // `guard` is used to ensure that the referenced `BufferSetVersion` will not be released early.
    guard: Guard,
}

impl Version {
    pub fn new(write_buffer_capacity: u32) -> Self {
        Version {
            buffer_set: Arc::new(BufferSet::new(write_buffer_capacity)),
            files: HashMap::default(),
            next: Arc::default(),
        }
    }

    /// Construct [`Version`] from thread local storage.
    pub fn from_local() -> Rc<Self> {
        // TODO: refresh next version.
        todo!()
    }

    /// Wait and construct next [`Version`].
    pub async fn wait_next_version(&self) -> Self {
        todo!()
    }

    pub fn active_write_buffer_id(&self) -> u32 {
        todo!()
    }

    /// Try install new version into
    pub fn install(&self, delta: DeltaVersion) -> Result<()> {
        todo!()
    }

    /// Fetch the files which obsolated but referenced by the [`Version`].
    pub fn deleted_files(&self) -> Vec<u32> {
        todo!()
    }
}

impl Drop for NextVersion {
    fn drop(&mut self) {
        todo!("drop raw_version if it is not null")
    }
}

impl BufferSet {
    pub fn new(write_buffer_capacity: u32) -> BufferSet {
        let min_file_id = 0;
        let buf = WriteBuffer::with_capacity(min_file_id, write_buffer_capacity);
        let version = Box::new(BufferSetVersion {
            min_file_id,
            sealed_buffers: Vec::default(),
            active_buffer: Arc::new(buf),
        });
        let raw = Box::leak(version);
        BufferSet {
            write_buffer_capacity,
            current: AtomicPtr::new(raw),
        }
    }

    pub fn current(&self) -> BufferSetRef<'_> {
        todo!()
    }

    pub fn install(&self, write_buffer: Arc<WriteBuffer>) {
        // TODO: the file ID should be continuous.
        todo!("install new version via CAS operation")
    }

    pub fn on_flushed(&self, files: &[u32]) {
        todo!()
    }

    pub async fn wait_flushable(&self) {
        todo!()
    }
}

impl Drop for BufferSet {
    fn drop(&mut self) {
        let raw = self.current.load(Ordering::SeqCst);
        if !raw.is_null() {
            unsafe {
                // Safety: the backing memory is obtained from [`Box::into_raw`] and there no any
                // references to the memory, which guarrantted by [`BufferSetRef`].
                drop(Box::from_raw(raw));
            }
        }
    }
}

impl BufferSetVersion {
    /// Read [`WriteBuffer`] of the specified `file_id`.
    ///
    /// If the user needs to access the [`WriteBuffer`] for a long time, use `clone` to make a copy.
    pub fn write_buffer(&self, file_id: u32) -> Option<&Arc<WriteBuffer>> {
        todo!()
    }

    #[inline]
    pub fn min_file_id(&self) -> u32 {
        self.min_file_id
    }
}

impl<'a> Deref for BufferSetRef<'a> {
    type Target = BufferSetVersion;

    fn deref(&self) -> &Self::Target {
        self.version
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn buffer_set_construct_and_drop() {
        drop(BufferSet::new(1 << 10));
    }
}
