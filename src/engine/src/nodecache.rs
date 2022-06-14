use crossbeam_epoch::Guard;

use crate::{Node, NodeTable};

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

    pub fn load<'g>(&self, id: u64, guard: &'g Guard) -> Node<'g> {
        let addr = self.table.load(id);
        Node::from_u64(addr, guard)
    }

    pub fn update<'g>(
        &self,
        id: u64,
        old: Node<'g>,
        new: Node<'g>,
        guard: &'g Guard,
    ) -> Result<(), Node<'g>> {
        match self.table.compare_exchange(id, old.as_u64(), new.as_u64()) {
            Ok(_) => Ok(()),
            Err(addr) => Err(Node::from_u64(addr, guard)),
        }
    }
}
