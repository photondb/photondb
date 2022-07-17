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

pub struct Options {
    pub data_node_size: usize,
    pub data_delta_length: usize,
    pub index_node_size: usize,
    pub index_delta_length: usize,
}
