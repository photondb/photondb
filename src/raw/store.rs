use std::path::Path;

use super::Table;
use crate::{env::Env, Options, Result};

#[allow(dead_code)]
pub struct Store<E: Env> {
    table: Table<E>,
}

#[allow(dead_code)]
impl<E: Env> Store<E> {
    pub async fn open<P: AsRef<Path>>(_env: E, _path: P) -> Result<Self> {
        todo!()
    }

    pub fn table(&self, _name: &str) -> Option<Table<E>> {
        todo!()
    }

    pub fn tables(&self) -> impl Iterator<Item = Table<E>> {
        std::iter::empty()
    }

    pub async fn create_table(&self, _name: &str, _options: Options) -> Result<Table<E>> {
        todo!()
    }

    pub async fn delete_table(&self, _name: &str) -> Result<()> {
        todo!()
    }
}
