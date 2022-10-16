use std::path::Path;

use crate::{env::Photon, table::Table, Options, Result};

pub struct Db {
    table: Table<Photon>,
}

impl Db {
    pub async fn open<P: AsRef<Path>>(root: P, options: Options) -> Result<Self> {
        let table = Table::open(Photon, root, options).await?;
        Ok(Db { table })
    }
}
