//! A storage engine for modern hardware.

// TODO: enable these warnings once the codebase is clean.
// #![warn(missing_docs, unreachable_pub)]

#![feature(
    io_error_more,
    type_alias_impl_trait,
    hash_drain_filter,
    pointer_is_aligned
)]

pub mod env;

mod error;
pub use error::{Error, Result};

mod store;
pub use store::{Options as StoreOptions, Store};

mod table;
pub use table::{Options as TableOptions, Table};

mod page;
mod page_store;
mod util;
