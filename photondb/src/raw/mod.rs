//! Raw PhotonDB APIs that can can run with different environments.

mod table;
pub use table::{Guard, Pages, Table};
