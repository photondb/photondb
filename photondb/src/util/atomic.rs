use std::sync::atomic::{AtomicU64, Ordering};

/// An atomic counter with relaxed memory ordering.
#[derive(Debug)]
pub(crate) struct Counter(AtomicU64);

impl Counter {
    pub(crate) const fn new(value: u64) -> Self {
        Self(AtomicU64::new(value))
    }

    pub(crate) fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }

    pub(crate) fn inc(&self) -> u64 {
        self.add(1)
    }

    pub(crate) fn add(&self, n: u64) -> u64 {
        self.0.fetch_add(n, Ordering::Relaxed)
    }
}

impl Default for Counter {
    fn default() -> Self {
        Self::new(0)
    }
}
