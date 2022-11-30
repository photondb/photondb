use std::{path::Path, sync::Arc};

use crate::{
    env::Env,
    page::{Key, Value},
    page_store::{PageStore, StoreStats},
    tree::*,
    Result,
};

/// A reference to a latch-free, log-structured table that stores sorted
/// key-value entries.
///
/// The reference is thread-safe and cheap to clone.
#[derive(Clone, Debug)]
pub struct Table<E: Env> {
    tree: Arc<Tree>,
    store: Arc<PageStore<E>>,
}

impl<E: Env> Table<E> {
    /// Opens a table in the path with the given options.
    pub async fn open<P: AsRef<Path>>(env: E, path: P, options: Options) -> Result<Self> {
        let tree = Arc::new(Tree::new(options.clone()));
        let store = PageStore::open(env, path, options.page_store, tree.clone()).await?;
        let txn = tree.begin(store.guard());
        txn.init().await?;
        Ok(Self {
            tree,
            store: Arc::new(store),
        })
    }

    /// Closes the table if this is the only reference to it.
    ///
    /// If this is not the only reference, returns [`Result::Err`] with this
    /// reference.
    pub async fn close(self) -> Result<(), Self> {
        match Arc::try_unwrap(self.store) {
            Ok(store) => {
                store.close().await;
                Ok(())
            }
            Err(store) => Err(Self {
                tree: self.tree,
                store,
            }),
        }
    }

    /// Begins a tree transaction.
    fn begin(&self) -> TreeTxn<'_, E> {
        self.tree.begin(self.store.guard())
    }

    /// Returns a [`Guard`] that pins the table for user operations.
    pub fn pin(&self) -> Guard<'_, E> {
        Guard::new(self)
    }

    /// Gets the value corresponding to the key.
    pub async fn get(&self, key: &[u8], lsn: u64) -> Result<Option<Vec<u8>>> {
        let key = Key::new(key, lsn);
        let txn = self.begin();
        let value = txn.get(key).await?;
        Ok(value.map(|v| v.to_vec()))
    }

    /// Puts a key-value entry to the table.
    pub async fn put(&self, key: &[u8], lsn: u64, value: &[u8]) -> Result<()> {
        let key = Key::new(key, lsn);
        let value = Value::Put(value);
        let txn = self.begin();
        txn.write(key, value).await?;
        Ok(())
    }

    /// Deletes the entry corresponding to the key from the table.
    pub async fn delete(&self, key: &[u8], lsn: u64) -> Result<()> {
        let key = Key::new(key, lsn);
        let value = Value::Delete;
        let txn = self.begin();
        txn.write(key, value).await?;
        Ok(())
    }

    /// Returns the statistics of the table.
    pub fn stats(&self) -> (Stats, StoreStats) {
        let store_stats = self.store.stats();
        let tree_stats = self.tree.stats();
        (tree_stats, store_stats)
    }

    /// Returns the minimal LSN that the table can safely read with.
    ///
    /// The table guarantees that entries visible to the returned LSN are
    /// retained for reads.
    pub fn safe_lsn(&self) -> u64 {
        self.tree.safe_lsn()
    }

    /// Updates the minimal LSN that the table can safely read with.
    ///
    /// The safe LSN must be increasing, so updating it with a smaller value has
    /// no effect. When the safe LSN is advanced, the table will gradually drop
    /// entries that are not visible to the LSN anymore.
    pub fn set_safe_lsn(&self, lsn: u64) {
        self.tree.set_safe_lsn(lsn);
    }
}

/// A handle that holds some resources of a table for user operations.
pub struct Guard<'a, E: Env> {
    table: &'a Table<E>,
    txn: TreeTxn<'a, E>,
}

impl<'a, E: Env> Guard<'a, E> {
    fn new(table: &'a Table<E>) -> Self {
        Self {
            table,
            txn: table.begin(),
        }
    }

    /// Re-pins the table so that the current pinned resources can be released.
    pub fn repin(&mut self) {
        self.txn = self.table.begin();
    }

    /// Gets the value corresponding to the key.
    //
    /// On success, if the value is found, returns [`Option::Some`] with the
    /// value; if the value is not found, returns [`Option::None`].
    pub async fn get(&self, key: &[u8], lsn: u64) -> Result<Option<&[u8]>> {
        let key = Key::new(key, lsn);
        Ok(self.txn.get(key).await?)
    }

    /// Returns an iterator over pages in the table.
    pub fn pages(&self) -> Pages<'_, 'a, E> {
        Pages::new(&self.txn)
    }
}

/// An iterator over pages in a table.
pub struct Pages<'a, 't: 'a, E: Env> {
    iter: TreeIter<'a, 't, E>,
}

impl<'a, 't: 'a, E: Env> Pages<'a, 't, E> {
    fn new(txn: &'a TreeTxn<'t, E>) -> Self {
        Self {
            iter: TreeIter::new(txn, ReadOptions::default()),
        }
    }

    /// Returns the next page in the table.
    pub async fn next(&mut self) -> Result<Option<PageIter<'_>>> {
        Ok(self.iter.next_page().await?)
    }
}
