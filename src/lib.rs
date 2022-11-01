//! A storage engine for modern hardware.

// TODO: enable these warnings once the codebase is clean.
// #![warn(missing_docs, unreachable_pub)]

#![feature(
    io_error_more,
    type_alias_impl_trait,
    hash_drain_filter,
    pointer_is_aligned
)]

mod error;
pub use error::{Error, Result};

pub mod env;

pub mod raw;
pub use raw::{StoreOptions, TableOptions};

mod photon;
pub use photon::{Store, Table};

mod page;
mod page_store;
mod tree;
mod util;

#[cfg(test)]
mod tests {
    use super::*;

    #[photonio::test]
    async fn crud() {
        let path = std::env::temp_dir();
        let table = Table::open(path, TableOptions::default()).await.unwrap();
        let key = &[1];
        let lsn = 2;
        let value = &[3];
        table.put(key, lsn, value).await.unwrap();
        table
            .get(key, lsn, |v| {
                assert_eq!(v, Some(value.as_slice()));
            })
            .await
            .unwrap();
        table.close().await;
    }
}
