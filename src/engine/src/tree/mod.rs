mod table;
pub use table::Table;

mod error;
use error::{Error, Result};

mod node;
mod tree;
use tree::Tree;

mod page;
mod pagestore;
mod pagetable;

struct Ghost {
    guard: crossbeam_epoch::Guard,
}

impl Ghost {
    pub fn pin() -> Self {
        let guard = crossbeam_epoch::pin();
        Self { guard }
    }
}

pub struct Options {
    pub data_node_size: usize,
    pub data_delta_length: usize,
    pub index_node_size: usize,
    pub index_delta_length: usize,
}
