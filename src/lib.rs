//! A storage engine for modern hardware.

#![warn(unreachable_pub)]
#![feature(io_error_more, type_alias_impl_trait)]

mod db;
pub use db::{Db, Options};

pub mod env;

mod error;
pub use error::{Error, Result};

mod page;
mod page_table;
mod tree;

#[cfg(test)]
mod tests {
    use super::*;

    #[photonio::test]
    async fn open() {
        let env = env::Photon;
        let options = Options::default();
        let _ = Db::open(env, "/tmp", options).await.unwrap();
    }
}
