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
pub(crate) use page_table::{MAX_ID, MIN_ID, NAN_ID};

mod meta;
pub(crate) use meta::VersionEdit;

mod version;
use version::Version;

mod jobs;
mod write_buffer;
pub(crate) use write_buffer::{RecordRef, WriteBuffer};

mod manifest;
pub(crate) use manifest::Manifest;

mod page_file;
pub(crate) use page_file::{FileInfo, FileMeta, PageFiles, PageHandle};

pub(crate) struct PageStore<E> {
    options: Options,
    env: E,
    table: PageTable,

    /// The global [`Version`] of [`PageStore`], used when tls [`Version`] does
    /// not exist. It needs to be updated every time a new [`Version`] is
    /// installed.
    version: Arc<Mutex<Version>>,

    page_files: Arc<PageFiles>,
    manifest: Arc<futures::lock::Mutex<Manifest>>,
}

impl<E: Env> PageStore<E> {
    #[allow(unused)]
    pub(crate) async fn open<P: AsRef<Path>>(env: E, path: P, options: Options) -> Result<Self> {
        todo!()
    }

    pub(crate) fn guard(&self) -> Guard {
        Guard::new(
            self.current_version(),
            self.table.clone(),
            self.page_files.clone(),
        )
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
