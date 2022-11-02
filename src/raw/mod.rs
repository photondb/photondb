//! Raw PhotonDB APIs that can can run with different environments.

pub mod store;
pub use store::Store;

pub mod table;
pub use table::Table;
