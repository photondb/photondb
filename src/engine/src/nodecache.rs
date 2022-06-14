use crossbeam_epoch::{Guard, Shared};

use crate::{Link, NodeTable};

pub struct NodeCache {
    table: NodeTable,
}

impl NodeCache {
    pub fn new() -> Self {
        Self {
            table: NodeTable::new(),
        }
    }

    pub fn allocate(&self) -> u64 {
        self.table.allocate()
    }

    pub fn load<'g>(&self, id: u64, _: &'g Guard) -> Shared<'g, Link> {
        let addr = self.table.load(id);
        (addr as *const Link).into()
    }

    pub fn update<'g>(
        &self,
        id: u64,
        old: Shared<'g, Link>,
        new: Shared<'g, Link>,
    ) -> Result<(), Shared<'g, Link>> {
        match self
            .table
            .compare_exchange(id, old.as_raw() as u64, new.as_raw() as u64)
        {
            Ok(_) => Ok(()),
            Err(addr) => Err((addr as *const Link).into()),
        }
    }
}
