use std::{path::Path, sync::Arc};

use crate::{
    env::Env,
    page::{Key, Value},
    page_store::PageStore,
    tree::*,
    Result,
};

/// A latch-free, log-structured table with sorted key-value entries.
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

    /// Gets the value corresponding to the key and applies a function to it.
    //
    /// On success, if the value is found, applies the function to
    /// [`Option::Some`] with the value; if the value is not found, applies the
    /// function to [`Option::None`]. Then returns [`Result::Ok`] with the
    /// output of the function.
    /// On failure, returns [`Result::Err`] without applying the function.
    pub async fn get<F, R>(&self, key: &[u8], lsn: u64, f: F) -> Result<R>
    where
        F: FnOnce(Option<&[u8]>) -> R,
    {
        let key = Key::new(key, lsn);
        let txn = self.begin();
        let value = txn.get(key).await?;
        Ok(f(value))
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
    pub fn stats(&self) -> Stats {
        self.tree.stats()
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
