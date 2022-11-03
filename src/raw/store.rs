use std::path::Path;

use super::Table;
use crate::{env::Env, Options, Result};

/// A persistent key-value store that manages multiple tables.
#[allow(dead_code)]
#[doc(hidden)]
#[derive(Clone, Debug)]
pub struct Store<E: Env> {
    table: Table<E>,
}

#[allow(dead_code)]
impl<E: Env> Store<E> {
    /// Opens a store in the path.
    pub async fn open<P: AsRef<Path>>(_env: E, _path: P) -> Result<Self> {
        todo!()
    }

    /// Returns the table with the given name.
    pub fn table(&self, _name: &str) -> Option<Table<E>> {
        todo!()
    }

    /// Returns an iterator over tables in the store.
    pub fn tables(&self) -> impl Iterator<Item = Table<E>> {
        std::iter::empty()
    }

    /// Creates a table with the given name and options.
    pub async fn create_table(&self, _name: &str, _options: Options) -> Result<Table<E>> {
        todo!()
    }

    /// Deletes the table with the given name.
    pub async fn delete_table(&self, _name: &str) -> Result<()> {
        todo!()
    }
}
