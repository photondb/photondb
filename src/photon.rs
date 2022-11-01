use std::{ops::Deref, path::Path};

use crate::{env::Photon, raw, Result, TableOptions};

pub struct Store(raw::Store<Photon>);

pub struct Table(raw::Table<Photon>);

impl Table {
    pub async fn open<P: AsRef<Path>>(path: P, options: TableOptions) -> Result<Self> {
        let table = raw::Table::open(Photon, path, options).await?;
        Ok(Self(table))
    }

    pub async fn close(self) {
        self.0.close().await;
    }
}

impl Deref for Table {
    type Target = raw::Table<Photon>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
