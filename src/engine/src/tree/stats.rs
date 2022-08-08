use crate::util::RelaxedCounter;

#[derive(Debug)]
pub struct Stats {
    pub cache_size: u64,
    pub num_data_splits: u64,
    pub num_data_splits_failed: u64,
    pub num_data_consolidations: u64,
    pub num_data_consolidations_failed: u64,
    pub num_index_splits: u64,
    pub num_index_splits_failed: u64,
    pub num_index_consolidations: u64,
    pub num_index_consolidations_failed: u64,
}

#[derive(Default)]
pub struct AtomicStats {
    pub num_data_splits: RelaxedCounter,
    pub num_data_splits_failed: RelaxedCounter,
    pub num_data_consolidations: RelaxedCounter,
    pub num_data_consolidations_failed: RelaxedCounter,
    pub num_index_splits: RelaxedCounter,
    pub num_index_splits_failed: RelaxedCounter,
    pub num_index_consolidations: RelaxedCounter,
    pub num_index_consolidations_failed: RelaxedCounter,
}
