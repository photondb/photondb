/// Options to configure a table.
#[non_exhaustive]
#[derive(Clone)]
pub struct Options {
    /// Approximate size of user data packed per page before it is split.
    ///
    /// Note that the size specified here corresponds to uncompressed data.
    ///
    /// Default: 8KB
    pub page_size: usize,

    /// Approximate number of delta pages kept per page chain before it is
    /// consolidated.
    ///
    /// Default: 4
    pub page_chain_length: usize,

    /// The capacity of [`WriteBuffer`]. It should be power of two.
    ///
    /// Default: 128MB
    pub write_buffer_capacity: u32,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            page_size: 8 << 10,
            page_chain_length: 4,
            write_buffer_capacity: 128 << 20,
        }
    }
}

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
