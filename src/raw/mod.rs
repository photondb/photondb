//! Raw PhotonDB APIs that can can run with different environments.

mod store;
pub use store::Store;

mod table;
pub use table::{Guard, Pages, Table};
