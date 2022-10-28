/// Options to configure the behavior of reads.
#[non_exhaustive]
#[derive(Clone)]
pub struct ReadOptions {
    /// The maximum visible LSN for this read.
    ///
    /// Keys with larger LSNs will be ignored in this read.
    ///
    /// Default: `u64::MAX`
    pub max_lsn: u64,

    /// Whether to fill pages load from store to the page cache.
    ///
    /// Default: true
    pub fill_cache: bool,
}

impl Default for ReadOptions {
    fn default() -> Self {
        Self {
            max_lsn: u64::MAX,
            fill_cache: true,
        }
    }
}

/// Options to configure the behavior of writes.
#[non_exhaustive]
#[derive(Clone)]
pub struct WriteOptions {}
