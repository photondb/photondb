use super::Tree;
use crate::{env::Env, page::*, page_store::*};

// The id of the root page is always 0.
const ROOT_ID: u64 = 0;
// An invalid id for pages other than the root.
const NULL_ID: u64 = 0;

pub(super) struct Txn<'a, E> {
    tree: &'a Tree<E>,
    guard: Guard,
}

impl<'a, E: Env> Txn<'a, E> {
    /// Creates a new transaction on the tree.
    pub(super) fn new(tree: &'a Tree<E>) -> Self {
        Self {
            tree,
            guard: tree.store.guard(),
        }
    }

    /// Gets the value corresponding to the key from the tree.
    pub(super) async fn get(&self, key: Key<'_>) -> Result<Option<&[u8]>> {
        let (view, _) = self.find_leaf(&key).await?;
        self.find_value(&key, &view).await
    }

    /// Writes the key-value pair to the tree.
    pub(super) async fn write(&self, key: Key<'_>, value: Value<'_>) -> Result<()> {
        let (mut view, parent) = self.find_leaf(&key).await?;
        // Build a delta page with the given key-value pair.
        let iter = ItemIter::new((key, value));
        let builder = SortedPageBuilder::new(PageTier::Leaf, PageKind::Data).with_iter(iter);
        let mut txn = self.guard.begin();
        let (new_addr, mut new_page) = txn.alloc_page(builder.size())?;
        builder.build(&mut new_page);
        // Update the corresponding leaf page with the delta.
        loop {
            new_page.set_epoch(view.page.epoch());
            new_page.set_chain_len(view.page.chain_len().saturating_add(1));
            new_page.set_chain_next(view.addr);
            match txn.update_page(view.id, view.addr, new_addr) {
                Ok(_) => break,
                Err(addr) => {
                    // The page has been updated by other transactions.
                    // We keep retrying as long as the page epoch remains the same.
                    let page = self.guard.read_page(addr).await?;
                    if page.epoch() == view.page.epoch() {
                        view.page = page;
                        continue;
                    }
                    return Err(Error::Again);
                }
            }
        }

        // Try to consolidate the page if the chain is too long.
        if new_page.chain_len() as usize > self.tree.options.page_chain_length {
            view.page = new_page.into();
            let _ = self.consolidate_page(view, parent).await;
        }
        Ok(())
    }
}

impl<'a, E> Txn<'a, E> {
    /// Finds the leaf page that may contain the key.
    ///
    /// Returns the leaf page and its parent.
    async fn find_leaf(&self, key: &Key<'_>) -> Result<(PageView<'_>, Option<PageView<'_>>)> {
        // The index, range, and parent of the current page, starting from the root.
        let mut index = Index::new(ROOT_ID);
        let mut range = Range::default();
        let mut parent = None;
        loop {
            // Read the current page from the store.
            let addr = self.guard.page_addr(index.id);
            let page = self.guard.read_page(addr).await?;
            let view = PageView {
                id: index.id,
                addr,
                page,
                range,
            };
            // If the page epoch has changed, the page may not contain the data we expect
            // anymore. Try to reconcile pending conflicts and restart the operation.
            if view.page.epoch() != index.epoch {
                let _ = self.reconcile_page(view, parent).await;
                return Err(Error::Again);
            }
            if view.page.tier().is_leaf() {
                return Ok((view, parent));
            }
            // Find the child page that may contain the key and update the current page.
            let (child_index, child_range) = self.find_child(key, &view).await?;
            index = child_index;
            range.start = child_range.start;
            // If the child has no range end, use the current one instead.
            if let Some(end) = child_range.end {
                range.end = Some(end);
            }
            parent = Some(view);
        }
    }

    /// Walks through the page chain and applies the function to each page.
    ///
    /// This function returns when it reaches the end of the chain or the
    /// applied function returns true.
    async fn walk_page<'g, F>(&'g self, view: &PageView<'g>, mut f: F) -> Result<()>
    where
        F: FnMut(PageRef<'g>) -> bool,
    {
        let mut page = view.page;
        loop {
            if f(page) || page.chain_next() == 0 {
                return Ok(());
            }
            page = self.guard.read_page(page.chain_next()).await?;
        }
    }

    /// Creates an iterator over the key-value pairs in the page.
    async fn iter_page<'g>(&'g self, view: &PageView<'g>) -> Result<MergingDataPageIter<'g>> {
        // let mut builder = MergingIterBuilder::with_capacity(view.page.chain_len() as
        // usize);
        let mut range_end = None;
        self.walk_page(view, |page| {
            match page.kind() {
                PageKind::Data => {
                    // builder.add(DataPageIter::from(page));
                }
                PageKind::Split => {
                    if range_end.is_none() {
                        let split_page = SplitPageRef::from(page);
                        range_end = Some(split_page.get(0).unwrap().0);
                    }
                }
            }
            false
        })
        .await?;
        todo!()
    }

    /// Finds the value corresponding to the key from the page.
    async fn find_value<'g>(
        &'g self,
        key: &Key<'_>,
        view: &PageView<'g>,
    ) -> Result<Option<&'g [u8]>> {
        let mut value = None;
        self.walk_page(view, |page| {
            debug_assert!(page.tier().is_leaf());
            // We only care about data pages here.
            if page.kind().is_data() {
                let page = LeafDataPageRef::from(page);
                if let Some((_, v)) = page.find(key) {
                    if let Value::Put(v) = v {
                        value = Some(v);
                    }
                    return true;
                }
            }
            false
        })
        .await?;
        Ok(value)
    }

    /// Finds the child page that may contain the key from the page.
    ///
    /// Returns the index and range of the child page.
    async fn find_child<'g>(
        &'g self,
        key: &Key<'_>,
        view: &PageView<'g>,
    ) -> Result<(Index, Range<'g>)> {
        let mut child_index = Index::default();
        let mut child_range = Range::default();
        self.walk_page(view, |page| {
            debug_assert!(page.tier().is_inner());
            // We only care about data pages here.
            if page.kind().is_data() {
                let mut iter = InnerDataPageIter::from(page);
                iter.seek_back(key);
                let (start, index) = iter.next().expect("child must exist");
                child_index = index;
                child_range.start = start;
                if let Some((end, _)) = iter.next() {
                    child_range.end = Some(end);
                }
                return true;
            }
            false
        })
        .await?;
        Ok((child_index, child_range))
    }

    // Splits the page into two halfs.
    async fn split_page(&self, mut view: PageView<'_>, parent: Option<PageView<'_>>) -> Result<()> {
        // We can only split base data pages.
        if !view.page.kind().is_data() || view.page.chain_next() != 0 {
            return Err(Error::InvalidArgument);
        }

        let page = DataPageRef::from(view.page);
        if let Some((split_key, right_iter)) = page.split() {
            let mut txn = self.guard.begin();
            // Build and insert the right page.
            let right_id = {
                let builder = SortedPageBuilder::new(view.page.tier(), view.page.kind())
                    .with_iter(right_iter);
                let (new_addr, mut new_page) = txn.alloc_page(builder.size())?;
                builder.build(&mut new_page);
                txn.insert_page(new_addr)
            };
            // Build a delta page with the right index.
            let iter = ItemIter::new((split_key, Index::new(right_id)));
            let builder = SortedPageBuilder::new(view.page.tier(), PageKind::Split).with_iter(iter);
            let (new_addr, mut new_page) = txn.alloc_page(builder.size())?;
            builder.build(&mut new_page);
            // Update the left page with the delta.
            // The page epoch must be updated to indicate the change of the page range.
            new_page.set_epoch(view.page.epoch() + 1);
            new_page.set_chain_len(view.page.chain_len().saturating_add(1));
            new_page.set_chain_next(view.addr);
            txn.update_page(view.id, view.addr, new_addr)
                .map(|_| {
                    self.tree.stats.success.split_page.inc();
                    view.page = new_page.into();
                })
                .map_err(|_| {
                    self.tree.stats.restart.split_page.inc();
                    Error::Again
                })?;
        }

        // Try to reconcile the page after a split.
        let _ = self.reconcile_page(view, parent).await;
        Ok(())
    }

    /// Reconciles any conflicts on the page.
    async fn reconcile_page(&self, view: PageView<'_>, parent: Option<PageView<'_>>) -> Result<()> {
        match view.page.kind() {
            PageKind::Data => {}
            PageKind::Split => {
                if let Some(parent) = parent {
                    self.reconcile_split_page(view, parent).await?;
                } else {
                    self.reconcile_split_root(view).await?;
                }
            }
        }
        Ok(())
    }

    // Reconciles a pending split on the page.
    async fn reconcile_split_page(&self, view: PageView<'_>, parent: PageView<'_>) -> Result<()> {
        debug_assert!(view.page.kind().is_split());
        let left_key = view.range.start;
        let left_index = Index::with_epoch(view.id, view.page.epoch());
        let split_page = SplitPageRef::from(view.page);
        let (split_key, split_index) = split_page.get(0).unwrap();
        // Build a delta page with the child on the left and the new split page on
        // the right.
        let delta = if let Some(right_key) = view.range.end {
            vec![
                (left_key, left_index),
                (split_key, split_index),
                (right_key, Index::new(NULL_ID)),
            ]
        } else {
            vec![(left_key, left_index), (split_key, split_index)]
        };
        let builder = SortedPageBuilder::new(PageTier::Inner, PageKind::Data)
            .with_iter(SliceIter::new(&delta));
        let mut txn = self.guard.begin();
        let (new_addr, mut new_page) = txn.alloc_page(builder.size())?;
        builder.build(&mut new_page);
        // Update the parent page with the delta.
        new_page.set_epoch(parent.page.epoch());
        new_page.set_chain_len(parent.page.chain_len().saturating_add(1));
        new_page.set_chain_next(parent.addr);
        txn.update_page(parent.id, parent.addr, new_addr)
            .map_err(|_| Error::Again)
    }

    // Reconciles a pending split on the root page.
    async fn reconcile_split_root(&self, view: PageView<'_>) -> Result<()> {
        debug_assert_eq!(view.id, ROOT_ID);
        debug_assert!(view.page.kind().is_split());
        let mut txn = self.guard.begin();
        // Move the root to another place.
        let left_id = txn.insert_page(view.addr);
        let left_key = Key::default();
        let left_index = Index::with_epoch(left_id, view.page.epoch());
        let split_page = SplitPageRef::from(view.page);
        let (split_key, split_index) = split_page.get(0).unwrap();
        // Build a new root with the original root on the left and the new split page on
        // the right.
        let delta = [(left_key, left_index), (split_key, split_index)];
        let builder = SortedPageBuilder::new(PageTier::Inner, PageKind::Data)
            .with_iter(SliceIter::new(&delta));
        let (new_addr, mut new_page) = txn.alloc_page(builder.size())?;
        builder.build(&mut new_page);
        // Update the original root with the new root.
        txn.update_page(view.id, view.addr, new_addr)
            .map_err(|_| Error::Again)
    }

    /// Consolidates delta pages on the page chain.
    async fn consolidate_page(
        &self,
        view: PageView<'_>,
        parent: Option<PageView<'_>>,
    ) -> Result<()> {
        let (iter, last_page) = self.iter_page_for_consolidation(&view).await;
        let builder = SortedPageBuilder::new(view.page.tier(), view.page.kind()).with_iter(iter);
        let mut txn = self.guard.begin();
        let (new_addr, mut new_page) = txn.alloc_page(builder.size())?;
        builder.build(&mut new_page);
        new_page.set_epoch(view.page.epoch());
        new_page.set_chain_len(last_page.chain_len());
        new_page.set_chain_next(last_page.chain_next());
        txn.replace_page(view.id, view.addr, new_addr)
            .map(|_| {
                self.tree.stats.success.consolidate_page.inc();
            })
            .map_err(|_| {
                self.tree.stats.restart.consolidate_page.inc();
                Error::Again
            })?;

        // Try to split the page if it is too large.
        if new_page.size() >= self.tree.options.page_size {
            let _ = self.split_page(view, parent);
        }
        Ok(())
    }

    async fn iter_page_for_consolidation(
        &self,
        view: &PageView<'_>,
    ) -> (DataPageIter<'_>, PageRef<'_>) {
        todo!()
    }

    async fn dealloc_page(&self, start: u64, until: u64) {
        todo!()
    }
}

struct PageView<'a> {
    id: u64,
    addr: u64,
    page: PageRef<'a>,
    range: Range<'a>,
}

type DataPageRef<'a> = SortedPageRef<'a, &'a [u8]>;
type DataPageIter<'a> = SortedPageIter<'a, &'a [u8]>;
type SplitPageRef<'a> = SortedPageRef<'a, Index>;
type LeafDataPageRef<'a> = SortedPageRef<'a, Value<'a>>;
type InnerDataPageRef<'a> = SortedPageRef<'a, Index>;
type InnerDataPageIter<'a> = SortedPageIter<'a, Index>;

struct MergingDataPageIter<'a> {
    iter: MergingIter<DataPageIter<'a>>,
    range_end: Option<Key<'a>>,
}
