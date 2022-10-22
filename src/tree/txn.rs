use super::{iter::*, Tree};
use crate::{
    env::Env,
    page::*,
    page_store::*,
    util::codec::{DecodeFrom, EncodeTo},
};

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

    /// Gets the value corresponding to the key.
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
                Ok(_) => {
                    view.page = new_page.into();
                    break;
                }
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

        // Try to consolidate the page if it is too long.
        if self.should_consolidate_page(view.page) {
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
        let mut index = Index::new(MIN_ID, 0);
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
            let (child_index, child_range) = self
                .find_child(key, &view)
                .await?
                .expect("child page must exist");
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
        F: FnMut(u64, PageRef<'g>) -> bool,
    {
        let mut addr = view.addr;
        let mut page = view.page;
        loop {
            if f(addr, page) || page.chain_next() == 0 {
                return Ok(());
            }
            addr = page.chain_next();
            page = self.guard.read_page(addr).await?;
        }
    }

    #[allow(dead_code)]
    /// Creates an iterator over the key-value pairs in the page.
    async fn iter_page<'g>(&'g self, view: &PageView<'g>) -> Result<()> {
        // usize);
        let mut range_limit = None;
        self.walk_page(view, |_, page| {
            match page.kind() {
                PageKind::Data => {}
                PageKind::Split => {
                    // The split key we first encountered must be the smallest.
                    #[cfg(debug_assertions)]
                    if let Some(range_limit) = range_limit {
                        let (split_key, _) = split_delta_from_page(page);
                        assert!(range_limit < split_key);
                    }
                    if range_limit.is_none() {
                        let (split_key, _) = split_delta_from_page(page);
                        range_limit = Some(split_key);
                    }
                }
            }
            false
        })
        .await?;
        Ok(())
    }

    /// Finds the value corresponding to the key from the page.
    async fn find_value<'g>(
        &'g self,
        key: &Key<'_>,
        view: &PageView<'g>,
    ) -> Result<Option<&'g [u8]>> {
        let mut value = None;
        self.walk_page(view, |_, page| {
            debug_assert!(page.tier().is_leaf());
            // We only care about data pages here.
            if page.kind().is_data() {
                let page = LeafDataPageRef::from(page);
                let index = match page.rank(key) {
                    Ok(i) => i,
                    Err(i) => i,
                };
                if let Some((k, v)) = page.get(index) {
                    if k.raw == key.raw {
                        debug_assert!(k.lsn <= key.lsn);
                        if let Value::Put(v) = v {
                            value = Some(v);
                        }
                        return true;
                    }
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
    ) -> Result<Option<(Index, Range<'g>)>> {
        let mut child = None;
        self.walk_page(view, |_, page| {
            debug_assert!(page.tier().is_inner());
            // We only care about data pages here.
            if page.kind().is_data() {
                let page = InnerDataPageRef::from(page);
                // Finds the two items that enclose the key.
                let (left, right) = match page.rank(key) {
                    // The `i` item is equal to the key, so the range is [i, i + 1).
                    Ok(i) => (page.get(i), i.checked_add(1).and_then(|i| page.get(i))),
                    // The `i` item is greater than the key, so the range is [i - 1, i).
                    Err(i) => (i.checked_sub(1).and_then(|i| page.get(i)), page.get(i)),
                };
                if let Some((start, index)) = left {
                    if index.id != NAN_ID {
                        let range = Range {
                            start,
                            end: right.map(|(end, _)| end),
                        };
                        child = Some((index, range));
                        return true;
                    }
                }
            }
            false
        })
        .await?;
        Ok(child)
    }

    // Splits the page into two halfs.
    async fn split_page(&self, view: PageView<'_>, parent: Option<PageView<'_>>) -> Result<()> {
        match view.page.tier() {
            PageTier::Leaf => self.split_page_impl::<Value>(view, parent).await,
            PageTier::Inner => self.split_page_impl::<Index>(view, parent).await,
        }
    }

    async fn split_page_impl<V>(
        &self,
        mut view: PageView<'_>,
        parent: Option<PageView<'_>>,
    ) -> Result<()>
    where
        V: EncodeTo + DecodeFrom,
    {
        // We can only split base data pages.
        if !view.page.kind().is_data() || view.page.chain_next() != 0 {
            return Err(Error::InvalidArgument);
        }

        let page = SortedPageRef::<V>::from(view.page);
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
            let iter = ItemIter::new((split_key, Index::new(right_id, 0)));
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
                } else if view.id == MIN_ID {
                    self.reconcile_split_root(view).await?;
                } else {
                    return Err(Error::InvalidArgument);
                }
            }
        }
        Ok(())
    }

    // Reconciles a pending split on the page.
    async fn reconcile_split_page(
        &self,
        view: PageView<'_>,
        mut parent: PageView<'_>,
    ) -> Result<()> {
        let left_key = view.range.start;
        let left_index = Index::new(view.id, view.page.epoch());
        let (split_key, split_index) = split_delta_from_page(view.page);
        // Build a delta page with the child on the left and the new split page on
        // the right.
        let delta = if let Some(range_end) = view.range.end {
            debug_assert!(split_key < range_end);
            vec![
                (left_key, left_index),
                (split_key, split_index),
                // This is a placeholder to indicate the range end of the right page.
                (range_end, Index::new(NAN_ID, 0)),
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
            .map(|_| {
                parent.page = new_page.into();
            })
            .map_err(|_| Error::Again)?;

        // Try to consolidate the parent page if it is too long.
        if self.should_consolidate_page(parent.page) {
            let _ = self.consolidate_page(parent, None).await;
        }
        Ok(())
    }

    // Reconciles a pending split on the root page.
    async fn reconcile_split_root(&self, view: PageView<'_>) -> Result<()> {
        debug_assert_eq!(view.id, MIN_ID);
        // Move the root to another place.
        let mut txn = self.guard.begin();
        let left_id = txn.insert_page(view.addr);
        let left_key = Key::default();
        let left_index = Index::new(left_id, view.page.epoch());
        let (split_key, split_index) = split_delta_from_page(view.page);
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
        match view.page.tier() {
            PageTier::Leaf => {
                self.consolidate_page_impl(view, parent, MergingLeafPageIter::new)
                    .await
            }
            PageTier::Inner => {
                self.consolidate_page_impl(view, parent, MergingInnerPageIter::new)
                    .await
            }
        }
    }

    async fn consolidate_page_impl<'g, F, I, V>(
        &'g self,
        mut view: PageView<'g>,
        parent: Option<PageView<'g>>,
        f: F,
    ) -> Result<()>
    where
        F: Fn(MergingPageIter<'g, V>) -> I,
        I: RewindableIterator<Item = (Key<'g>, V)>,
        V: EncodeTo + DecodeFrom,
    {
        let cons = self.build_consolidation(&view).await?;
        let iter = f(cons.iter);
        let builder = SortedPageBuilder::new(view.page.tier(), view.page.kind()).with_iter(iter);
        let mut txn = self.guard.begin();
        // Build a new page from some delta pages.
        let (new_addr, mut new_page) = txn.alloc_page(builder.size())?;
        builder.build(&mut new_page);
        new_page.set_epoch(view.page.epoch());
        new_page.set_chain_len(cons.last_page.chain_len());
        new_page.set_chain_next(cons.last_page.chain_next());
        // Deallocate the consolidated delta pages.
        txn.dealloc_pages(&cons.page_addrs);
        txn.update_page(view.id, view.addr, new_addr)
            .map(|_| {
                self.tree.stats.success.consolidate_page.inc();
                view.page = new_page.into();
            })
            .map_err(|_| {
                self.tree.stats.restart.consolidate_page.inc();
                Error::Again
            })?;

        // Try to split the page if it is too large.
        if self.should_split_page(view.page) {
            let _ = self.split_page(view, parent);
        }
        Ok(())
    }

    async fn build_consolidation<'g, V>(
        &'g self,
        view: &PageView<'g>,
    ) -> Result<Consolidation<'g, V>> {
        let chain_len = view.page.chain_len() as usize;
        let mut builder = MergingIterBuilder::with_capacity(chain_len);
        let mut last_page = view.page;
        let mut page_size = 0;
        let mut page_addrs = Vec::with_capacity(chain_len);
        let mut range_limit = None;
        self.walk_page(view, |addr, page| {
            match page.kind() {
                PageKind::Data => {
                    // TODO: do some benchmarks to evaluate this.
                    if builder.len() >= 2 && page_size < page.size() / 2 && range_limit.is_none() {
                        return true;
                    }
                    builder.add(SortedPageIter::from(page));
                    page_size += page.size();
                    page_addrs.push(addr);
                }
                PageKind::Split => {
                    if range_limit.is_none() {
                        let (split_key, _) = split_delta_from_page(page);
                        range_limit = Some(split_key);
                    }
                }
            }
            last_page = page;
            false
        })
        .await?;
        let iter = MergingPageIter::new(builder.build(), range_limit);
        Ok(Consolidation {
            iter,
            last_page,
            page_addrs,
        })
    }

    // Returns true if the page should be split.
    fn should_split_page(&self, page: PageRef<'_>) -> bool {
        let mut max_size = self.tree.options.page_size;
        if page.tier().is_inner() {
            // Adjust the page size for inner pages.
            // TODO: do some benchmarks to evaluate this.
            max_size /= 2;
        }
        page.size() > max_size
    }

    // Returns true if the page should be consolidated.
    fn should_consolidate_page(&self, page: PageRef<'_>) -> bool {
        let mut max_chain_len = self.tree.options.page_chain_length;
        if page.tier().is_inner() {
            // Adjust the chain length for inner pages.
            // TODO: do some benchmarks to evaluate this.
            max_chain_len /= 2;
        }
        page.chain_len() as usize > max_chain_len.max(1)
    }
}

struct PageView<'a> {
    id: u64,
    addr: u64,
    page: PageRef<'a>,
    range: Range<'a>,
}

struct Consolidation<'a, V> {
    iter: MergingPageIter<'a, V>,
    last_page: PageRef<'a>,
    page_addrs: Vec<u64>,
}

fn split_delta_from_page(page: PageRef<'_>) -> (Key<'_>, Index) {
    debug_assert!(page.kind().is_split());
    SplitPageRef::from(page)
        .get(0)
        .expect("split page delta must exist")
}
