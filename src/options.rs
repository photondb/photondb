#[non_exhaustive]
pub struct Options {
    pub page_chain_length: usize,
}

impl Options {
    pub fn new() -> Self {
        Self {
            page_chain_length: 8,
        }
    }
}

impl Default for Options {
    fn default() -> Self {
        Self::new()
    }
}
