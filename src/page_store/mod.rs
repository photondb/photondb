use std::path::Path;

use crate::env::Env;

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

mod page_file;
use page_file::{FileInfo, FileMeta, PageHandle};

pub(crate) struct PageStore<E> {
    env: E,
    table: PageTable,
}

impl<E: Env> PageStore<E> {
    pub(crate) async fn open<P: AsRef<Path>>(env: E, path: P) -> Result<Self> {
        todo!()
    }

    pub(crate) fn guard(&self) -> Guard {
        Guard::new(Version::from_local(), self.table.clone())
    }
}
