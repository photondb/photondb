mod error;
pub(crate) use error::{Error, Result};

mod page_txn;
pub(crate) use page_txn::PageTxn;

mod page_table;
use page_table::PageTable;

use crate::env::Env;

pub(crate) struct PageStore<E> {
    env: E,
    table: PageTable,
}

impl<E: Env> PageStore<E> {
    pub(crate) async fn open(env: E) -> Result<Self> {
        todo!()
    }

    pub(crate) fn begin(&self) -> PageTxn {
        todo!()
    }
}
