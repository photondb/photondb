use crate::util::Counter;

#[derive(Default, Debug)]
pub struct Stats {
    pub cache_size: u64,
    pub succeeded: OpStats,
    pub conflicted: OpStats,
}

#[derive(Default, Debug)]
pub struct OpStats {
    pub num_gets: u64,
    pub num_inserts: u64,
    pub num_data_splits: u64,
    pub num_data_consolidates: u64,
    pub num_index_splits: u64,
    pub num_index_consolidates: u64,
}

#[derive(Default)]
pub struct AtomicStats {
    pub succeeded: AtomicOpStats,
    pub conflicted: AtomicOpStats,
}

#[derive(Default)]
pub struct AtomicOpStats {
    pub num_gets: Counter,
    pub num_inserts: Counter,
    pub num_data_splits: Counter,
    pub num_data_consolidates: Counter,
    pub num_index_splits: Counter,
    pub num_index_consolidates: Counter,
}

impl AtomicOpStats {
    pub fn snapshot(&self) -> OpStats {
        OpStats {
            num_gets: self.num_gets.get(),
            num_inserts: self.num_inserts.get(),
            num_data_splits: self.num_data_splits.get(),
            num_data_consolidates: self.num_data_consolidates.get(),
            num_index_splits: self.num_index_splits.get(),
            num_index_consolidates: self.num_index_consolidates.get(),
        }
    }
}
