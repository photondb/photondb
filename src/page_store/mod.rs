use std::{path::Path, sync::Mutex};

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
use write_buffer::WriteBuffer;

mod manifest;
mod page_file;
use page_file::{FileInfo, FileMeta, PageHandle};

pub(crate) struct PageStore<E> {
    options: Options,
    env: E,
    table: PageTable,

    /// The global [`Version`] of [`PageStore`], used when tls [`Version`] does
    /// not exist. It needs to be updated every time a new [`Version`] is
    /// installed.
    version: Mutex<Version>,
}

impl<E: Env> PageStore<E> {
    #[allow(unused)]
    pub(crate) async fn open<P: AsRef<Path>>(env: E, path: P, options: Options) -> Result<Self> {
        todo!()
    }

    pub(crate) fn guard(&self) -> Guard {
        Guard::new(Version::from_local(), self.table.clone())
    }
}
