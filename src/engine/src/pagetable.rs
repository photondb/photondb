use std::sync::atomic::{AtomicU64, Ordering};

const TABLE_SIZE: usize = 2 << 16;

pub struct PageTable {
    map: Vec<AtomicU64>,
    next: AtomicU64,
}

impl PageTable {
    pub fn new() -> Self {
        let mut map = Vec::with_capacity(TABLE_SIZE);
        for _ in 0..TABLE_SIZE {
            map.push(AtomicU64::new(0));
        }
        Self {
            map,
            next: AtomicU64::new(0),
        }
    }

    pub fn get(&self, id: u64) -> u64 {
        self.map[id as usize].load(Ordering::Acquire)
    }

    pub fn cas(&self, id: u64, old: u64, new: u64) -> Option<u64> {
        match self.map[id as usize].compare_exchange(old, new, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => None,
            Err(actual) => Some(actual),
        }
    }

    pub fn install(&self, new: u64) -> u64 {
        let id = self.next.fetch_add(1, Ordering::AcqRel);
        self.map[id as usize].store(new, Ordering::Release);
        id
    }

    pub fn uninstall(&self, id: u64) {
        todo!()
    }
}
