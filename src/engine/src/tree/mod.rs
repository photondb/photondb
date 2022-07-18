mod table;
pub use table::Table;

mod error;
pub use error::{Error, Result};

mod ghost;
use ghost::{Ghost, Guard};

mod node;
mod page;
mod pagealloc;
mod pagestore;
mod pagetable;
mod tree;

#[derive(Clone, Debug)]
pub struct Options {
    pub cache_size: usize,
    pub data_node_size: usize,
    pub data_delta_length: u8,
    pub index_node_size: usize,
    pub index_delta_length: u8,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            cache_size: usize::MAX,
            data_node_size: 8 * 1024,
            data_delta_length: 8,
            index_node_size: 4 * 1024,
            index_delta_length: 4,
        }
    }
}
