use std::sync::atomic::{AtomicU64, Ordering};

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

#[derive(Debug)]
pub(crate) struct Sequencer(AtomicU64);

impl Sequencer {
    pub(crate) const fn new(value: u64) -> Self {
        Self(AtomicU64::new(value))
    }

    pub(crate) fn get(&self) -> u64 {
        self.0.load(Ordering::Acquire)
    }

    pub(crate) fn inc(&self) -> u64 {
        self.0.fetch_add(1, Ordering::Release)
    }
}

impl Default for Sequencer {
    fn default() -> Self {
        Self::new(0)
    }
}
