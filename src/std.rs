//! Synchronous PhotonDB APIs based on the raw APIs with the [`Std`]
//! environment.
//!
//! The [`Std`] environment use synchronous I/O from [`std`], so all the futures
//! it returns will block until completion. As a result, we can provide
//! synchronous APIs by manually polling the futures returned by the raw APIs.
//! The overhead introduced by the async abstraction in the raw APIs should be
//! negligible.
//!
//! [`Std`]: crate::env::Std

use std::{
    future::Future,
    ops::Deref,
    path::Path,
    pin::Pin,
    task::{Context, Poll},
};

use futures::task::noop_waker_ref;

use crate::{env::Std, raw, Result, TableOptions};

/// A persistent key-value store that manages multiple tables.
///
/// This is the same as [`raw::Store`] with the [`Std`] environment.
#[doc(hidden)]
#[derive(Clone, Debug)]
pub struct Store(raw::Store<Std>);

/// A reference to a latch-free, log-structured table that stores sorted
/// key-value entries.
///
/// This is the same as [`raw::Table`] with the [`Std`] environment.
#[derive(Clone, Debug)]
pub struct Table(raw::Table<Std>);

impl Table {
    /// Opens a table in the path with the given options.
    ///
    /// This is a synchronous version of [`raw::Table::open`] with the [`Std`]
    /// environment.
    pub fn open<P: AsRef<Path>>(path: P, options: TableOptions) -> Result<Self> {
        let table = poll(raw::Table::open(Std, path, options))?;
        Ok(Self(table))
    }

    /// Closes the table if this is the only reference to it.
    ///
    /// This is a synchronous version of [`raw::Table::close`].
    pub fn close(self) -> Result<(), Self> {
        poll(self.0.close()).map_err(Self)
    }

    /// Gets the value corresponding to the key.
    ///
    /// This is a synchronous version of [`raw::Table::get`].
    pub fn get(&self, key: &[u8], lsn: u64) -> Result<Option<Vec<u8>>> {
        poll(self.0.get(key, lsn))
    }

    /// Puts a key-value entry to the table.
    ///
    /// This is a synchronous version of [`raw::Table::put`].
    pub fn put(&self, key: &[u8], lsn: u64, value: &[u8]) -> Result<()> {
        poll(self.0.put(key, lsn, value))
    }

    /// Deletes the entry corresponding to the key from the table.
    ///
    /// This is a synchronous version of [`raw::Table::delete`].
    pub fn delete(&self, key: &[u8], lsn: u64) -> Result<()> {
        poll(self.0.delete(key, lsn))
    }
}

impl Deref for Table {
    type Target = raw::Table<Std>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// A handle that holds some resources of a table for user operations.
pub type Guard<'a> = raw::Guard<'a, Std>;

/// An iterator over pages in a table.
pub type Pages<'a, 't> = raw::Pages<'a, 't, Std>;

fn poll<F: Future>(mut future: F) -> F::Output {
    let cx = &mut Context::from_waker(noop_waker_ref());
    // Safety: the future will block until completion, so it will never be moved.
    let fut = unsafe { Pin::new_unchecked(&mut future) };
    match fut.poll(cx) {
        Poll::Ready(output) => output,
        Poll::Pending => unreachable!(),
    }
}
