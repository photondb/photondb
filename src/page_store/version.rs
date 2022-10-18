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

use super::{WriteBuffer, FileInfo, PageHandle, Result};
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

pub(crate) struct NextVersion {
    raw_version: AtomicPtr<Version>,
}

pub(crate) struct BufferSet {
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
        todo!("drop internal buffer set version")
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
