#[non_exhaustive]
#[derive(Clone)]
pub struct Options {
    pub page_size: usize,
    pub page_chain_length: usize,

    /// The capacity of [`WriteBuffer`]. It should be power of two.
    ///
    /// Default: 128MB
    pub write_buffer_capacity: u32,
}

impl Options {
    pub fn new() -> Self {
        Self {
            page_size: 8 << 10,
            page_chain_length: 4,
            write_buffer_capacity: 128 << 20,
        }
    }
}

impl Default for Options {
    fn default() -> Self {
        Self::new()
    }
}
