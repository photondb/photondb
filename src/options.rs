#[non_exhaustive]
pub struct Options {
    pub page_size: usize,
    pub page_chain_length: usize,
}

impl Options {
    pub fn new() -> Self {
        Self {
            page_size: 8 * 1024,
            page_chain_length: 4,
        }
    }
}

impl Default for Options {
    fn default() -> Self {
        Self::new()
    }
}
