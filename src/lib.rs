//! A storage engine for modern hardware.

// TODO: enable these warnings once the codebase is clean.
// #![warn(missing_docs, unreachable_pub)]

#![feature(
    io_error_more,
    type_alias_impl_trait,
    hash_drain_filter,
    pointer_is_aligned
)]

mod store;
pub use store::Store;

mod table;
pub use table::Table;

mod error;
pub use error::{Error, Result};

mod options;

pub mod env;

mod page;
mod page_store;
mod tree;
mod util;
