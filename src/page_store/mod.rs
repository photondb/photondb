use std::{
    path::Path,
    rc::Rc,
    sync::{Arc, Mutex},
};

use crate::{env::Env, Options};

mod error;
pub(crate) use error::{Error, Result};

mod page_txn;
pub(crate) use page_txn::{Guard, PageTxn};

mod page_table;
use page_table::PageTable;

mod meta;

mod version;
use version::Version;

mod jobs;
mod write_buffer;
pub(crate) use write_buffer::{DeletedPagesRecordRef, RecordRef, WriteBuffer};

mod manifest;
mod page_file;
pub(crate) use page_file::{FileInfo, FileMeta, PageFiles, PageHandle};

pub(crate) struct PageStore<E> {
    options: Options,
    env: E,
    table: PageTable,

    /// The global [`Version`] of [`PageStore`], used when tls [`Version`] does
    /// not exist. It needs to be updated every time a new [`Version`] is
    /// installed.
    version: Mutex<Version>,

    page_files: Arc<PageFiles>,
}

impl<E: Env> PageStore<E> {
    #[allow(unused)]
    pub(crate) async fn open<P: AsRef<Path>>(env: E, path: P, options: Options) -> Result<Self> {
        todo!()
    }

    pub(crate) fn guard(&self) -> Guard {
        Guard::new(self.current_version(), self.table.clone())
    }

    #[inline]
    fn current_version(&self) -> Rc<Version> {
        Version::from_local().unwrap_or_else(|| {
            let version = { Rc::new(self.version.lock().expect("Poisoned").clone()) };
            Version::set_local(version);
            Version::from_local().expect("Already installed")
        })
    }
}
