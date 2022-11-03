//! Asynchronous PhotonDB APIs based on the raw APIs with the [`Photon`]
//! environment.
//!
//! [`Photon`]: crate::env::Photon

use std::{ops::Deref, path::Path};

use crate::{env::Photon, raw, Options, Result};

/// A persistent key-value store that manages multiple tables.
///
/// This is the same as [`raw::Store`] with the [`Photon`] environment.
#[doc(hidden)]
#[derive(Clone, Debug)]
pub struct Store(raw::Store<Photon>);

/// A latch-free, log-structured table with sorted key-value entries.
///
/// This is the same as [`raw::Table`] with the [`Photon`] environment.
#[derive(Clone, Debug)]
pub struct Table(raw::Table<Photon>);

impl Table {
    /// Opens a table in the path with the given options.
    ///
    /// This is the same as [`raw::Table::open`] with the [`Photon`]
    /// environment.
    pub async fn open<P: AsRef<Path>>(path: P, options: Options) -> Result<Self> {
        let table = raw::Table::open(Photon, path, options).await?;
        Ok(Self(table))
    }

    /// Closes the table if this is the only reference to it.
    ///
    /// This is the same as [`raw::Table::close`] with the [`Photon`]
    /// environment.
    pub async fn close(self) -> Result<(), Self> {
        self.0.close().await.map_err(Self)
    }
}

impl Deref for Table {
    type Target = raw::Table<Photon>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
