pub use crossbeam_epoch::Guard;

pub struct Ghost {
    guard: Guard,
}

impl Ghost {
    pub fn pin() -> Self {
        let guard = crossbeam_epoch::pin();
        Self { guard }
    }

    pub fn guard(&self) -> &Guard {
        &self.guard
    }
}
