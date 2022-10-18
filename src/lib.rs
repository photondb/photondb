//! A storage engine for modern hardware.

#![warn(unreachable_pub, trivial_casts, trivial_numeric_casts)]
#![feature(io_error_more, type_alias_impl_trait)]

mod db;
pub use db::Db;

mod error;
pub use error::{Error, Result};

mod options;
pub use options::Options;

pub mod env;

mod page;
mod page_store;
mod table;
mod tree;
mod util;

#[cfg(test)]
mod tests {
    use super::*;

    #[photonio::test]
    async fn open() {
        let opts = Options::new();
        let _ = Db::open("/tmp", opts).await.unwrap();
    }
}
