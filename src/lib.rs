//! A storage engine for modern hardware.

// TODO: enable these warnings once the codebase is clean.
// #![warn(missing_docs, unreachable_pub)]

#![feature(
    io_error_more,
    type_alias_impl_trait,
    hash_drain_filter,
    pointer_is_aligned
)]

mod table;
pub use table::{RawTable, Table};

mod error;
pub use error::{Error, Result};

mod options;
pub use options::Options;

pub mod env;

mod page;
mod page_store;
mod tree;
mod util;

#[cfg(test)]
mod tests {
    use super::{env::Env, *};

    #[photonio::test]
    async fn table() {
        let env = env::Photon;
        env.spawn_background(async move {
            let env = env::Photon;
            let _ = env.open_sequential_writer("/tmp/test.txt").await.unwrap();
        })
        .await;
    }
}
