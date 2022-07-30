mod table;
pub use table::Table;

mod error;
pub use error::{Error, Result};

mod ghost;
use ghost::{Ghost, Guard};

mod btree;
use btree::BTree;

mod node;
mod page;
mod pagecache;
mod pagestore;
mod pagetable;

#[derive(Clone, Debug)]
pub struct Options {
    pub cache_size: usize,
    pub data_node_size: usize,
    pub data_delta_length: u8,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            cache_size: usize::MAX,
            data_node_size: 8 * 1024,
            data_delta_length: 8,
        }
    }
}

impl Options {
    fn node_size(&self, is_index: bool) -> usize {
        if is_index {
            self.data_node_size / 2
        } else {
            self.data_node_size
        }
    }
}
