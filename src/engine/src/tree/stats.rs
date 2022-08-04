use std::sync::atomic::{AtomicU64, Ordering};

pub struct Counter(AtomicU64);

impl Counter {
    pub fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }

    pub fn inc(&self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

impl Default for Counter {
    fn default() -> Self {
        Self(AtomicU64::new(0))
    }
}

#[derive(Debug)]
pub struct Stats {
    pub cache_size: u64,
    pub num_data_splits: u64,
    pub num_data_consolidations: u64,
    pub num_index_splits: u64,
    pub num_index_consolidations: u64,
}

#[derive(Default)]
pub struct AtomicStats {
    pub num_data_splits: Counter,
    pub num_data_consolidations: Counter,
    pub num_index_splits: Counter,
    pub num_index_consolidations: Counter,
}

impl AtomicStats {
    pub fn snapshot(&self) -> Stats {
        Stats {
            cache_size: 0,
            num_data_splits: self.num_data_splits.get(),
            num_data_consolidations: self.num_data_consolidations.get(),
            num_index_splits: self.num_index_splits.get(),
            num_index_consolidations: self.num_index_consolidations.get(),
        }
    }
}
