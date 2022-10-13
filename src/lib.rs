//! A storage engine for modern hardware.

#![warn(unreachable_pub)]
#![feature(io_error_more, type_alias_impl_trait)]

pub mod env;
pub use env::Env;

mod error;
pub use error::{Error, Result};

mod db;
pub use db::Db;
