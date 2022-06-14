use std::sync::atomic::{AtomicU64, Ordering};

const NODE_TABLE_SIZE: usize = 2 << 16;

pub struct NodeTable {
    table: Vec<AtomicU64>,
    index: AtomicU64,
}

impl NodeTable {
    pub fn new() -> Self {
        let mut table = Vec::with_capacity(NODE_TABLE_SIZE);
        for _ in 0..NODE_TABLE_SIZE {
            table.push(AtomicU64::new(0));
        }
        Self {
            table,
            index: AtomicU64::new(0),
        }
    }

    pub fn allocate(&self) -> u64 {
        self.index.fetch_add(1, Ordering::Relaxed)
    }

    pub fn free(&self, id: u64) {
        todo!();
    }

    pub fn load(&self, id: u64) -> u64 {
        self.table[id as usize].load(Ordering::Acquire)
    }

    pub fn compare_exchange(&self, id: u64, old: u64, new: u64) -> Result<u64, u64> {
        self.table[id as usize].compare_exchange(old, new, Ordering::AcqRel, Ordering::Acquire)
    }
}
