//mod table;
//pub use table::Table;

//mod error;
//pub use error::{Error, Result};

//mod ghost;
//use ghost::{Ghost, Guard};

//mod btree;
//use btree::{BTree, Stats};

//mod node;
mod page;
//mod pagecache;
//mod pagestore;
//mod pagetable;

#[derive(Clone, Debug)]
pub struct Options {
    pub cache_size: usize,
    pub data_node_size: usize,
    pub data_delta_length: usize,
    pub index_node_size: usize,
    pub index_delta_length: usize,
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
