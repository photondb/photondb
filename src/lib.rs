//! A high-performance storage engine for modern hardware and platforms.
//!
//! PhotonDB is designed from scratch to leverage the power of modern multi-core
//! chips, storage devices, operating systems, and programming languages.
//!
//! Features:
//!
//! - Latch-free data structures, scale to many cores.
//! - Log-structured persistent stores, optimized for flash storage.
//! - Asynchronous APIs and efficient file IO, powered by io_uring on Linux.
//!
//! This crate provides three sets of APIs:
//!
//! - [`Raw`]: a set of low-level APIs that can run with different environments.
//! - [`Std`]: a set of synchronous APIs based on the raw APIs that doesn't
//!   require a runtime to run.
//! - [`Photon`]: a set of asynchronous APIs based on the raw APIs that must run
//!   with the [PhotonIO] runtime.
//!
//! The [`Photon`] APIs are the default APIs that are re-exported to the
//! top-level module.
//!
//! [`Raw`]: crate::raw
//! [`Std`]: crate::std
//! [`Photon`]: crate::photon
//! [PhotonIO]: https://crates.io/crates/photonio

#![warn(missing_docs, unreachable_pub)]
#![feature(
    io_error_more,
    type_alias_impl_trait,
    hash_drain_filter,
    pointer_is_aligned
)]

pub mod env;
pub mod raw;
pub mod std;

pub mod photon;
pub use photon::Table;

mod error;
pub use error::{Error, Result};

mod tree;
pub use tree::{Options as TableOptions, PageIter, ReadOptions, Stats, WriteOptions};

mod page_store;
pub use page_store::Options as PageStoreOptions;

mod page;
mod util;

#[cfg(test)]
mod tests {
    use rand::random;
    use tempdir::TempDir;

    use super::*;

    const OPTIONS: TableOptions = TableOptions {
        page_size: 64,
        page_chain_length: 2,
        page_store: PageStoreOptions {
            write_buffer_capacity: 1 << 20,
        },
    };

    async fn must_put(table: &Table, i: u64, lsn: u64) {
        let buf = i.to_be_bytes();
        table.put(&buf, lsn, &buf).await.unwrap()
    }

    async fn must_get(table: &Table, i: u64, lsn: u64, expect: Option<u64>) {
        let buf = i.to_be_bytes();
        let value = table.get(&buf, lsn).await.unwrap();
        assert_eq!(value, expect.map(|v| v.to_be_bytes().to_vec()));
    }

    #[photonio::test]
    async fn crud() {
        let path = TempDir::new("crud").unwrap();
        let table = Table::open(&path, OPTIONS).await.unwrap();
        const N: u64 = 1 << 10;
        for i in 0..N {
            must_put(&table, i, i).await;
            must_get(&table, i, i, Some(i)).await;
        }
        for i in 0..N {
            must_get(&table, i, i, Some(i)).await;
        }

        let guard = table.pin();
        let mut pages = guard.pages();
        let mut i = 0u64;
        while let Some(page) = pages.next().await.unwrap() {
            for (k, v) in page {
                assert_eq!(k, &i.to_be_bytes());
                assert_eq!(v, &i.to_be_bytes());
                i += 1;
            }
        }

        table.close().await.unwrap();
    }

    #[photonio::test]
    async fn random_crud() {
        let path = TempDir::new("random_crud").unwrap();
        let table = Table::open(&path, OPTIONS).await.unwrap();
        const N: u64 = 1 << 12;
        for _ in 0..N {
            let i = random();
            must_put(&table, i, i).await;
            must_get(&table, i, i, Some(i)).await;
        }
        table.close().await.unwrap();
    }

    #[photonio::test]
    async fn concurrent_crud() {
        let path = TempDir::new("concurrent_crud").unwrap();
        let table = Table::open(&path, OPTIONS).await.unwrap();
        let mut tasks = Vec::new();
        for _ in 0..4 {
            let table = table.clone();
            let handle = photonio::task::spawn(async move {
                const N: u64 = 1 << 10;
                for _ in 0..N {
                    let i = random();
                    must_put(&table, i, i).await;
                    must_get(&table, i, i, Some(i)).await;
                }
            });
            tasks.push(handle);
        }
        for task in tasks {
            task.await.unwrap();
        }
        table.close().await.unwrap();
    }
}
