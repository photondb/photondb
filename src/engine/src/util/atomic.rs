use std::sync::atomic::{AtomicU64, Ordering};

pub struct Counter(AtomicU64);

impl Default for Counter {
    fn default() -> Self {
        Self::new(0)
    }
}

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

pub struct Sequencer(AtomicU64);

impl Default for Sequencer {
    fn default() -> Self {
        Self::new(0)
    }
}

impl Sequencer {
    pub const fn new(value: u64) -> Self {
        Self(AtomicU64::new(value))
    }

    pub fn get(&self) -> u64 {
        self.0.load(Ordering::Acquire)
    }

    pub fn inc(&self) -> u64 {
        self.0.fetch_add(1, Ordering::Release)
    }
}
