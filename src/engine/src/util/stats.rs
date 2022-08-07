use std::sync::atomic::{AtomicU64, Ordering};

pub struct RelaxedCounter(AtomicU64);

impl Default for RelaxedCounter {
    fn default() -> Self {
        Self::new(0)
    }
}

impl RelaxedCounter {
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
