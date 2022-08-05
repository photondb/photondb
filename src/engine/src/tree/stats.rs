use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug)]
pub struct Stats {
    pub cache_size: u64,
    pub num_data_splits: u64,
    pub num_data_consolidations: u64,
    pub num_index_splits: u64,
    pub num_index_consolidations: u64,
}

pub struct Counter(AtomicU64);

impl Counter {
    pub const fn new(value: u64) -> Self {
        Self(AtomicU64::new(value))
    }

    pub fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }

    pub fn inc(&self) -> u64 {
        self.0.fetch_add(1, Ordering::Relaxed)
    }
}

impl Default for Counter {
    fn default() -> Self {
        Self::new(0)
    }
}

#[derive(Default)]
pub struct AtomicStats {
    pub num_data_splits: Counter,
    pub num_data_consolidations: Counter,
    pub num_index_splits: Counter,
    pub num_index_consolidations: Counter,
}
