use std::{
    fmt,
    future::Future,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use log::trace;

use crate::{env::Env, page::*, page_store::*};

mod page;
pub use page::PageIter;
use page::*;

mod stats;
use stats::AtomicStats;
pub use stats::Stats;

mod options;
pub use options::{Options, ReadOptions, WriteOptions};

pub(crate) struct Tree {
    options: Options,
    stats: AtomicStats,
    safe_lsn: AtomicU64,
}

impl Tree {
    pub(crate) fn new(options: Options) -> Self {
        Self {
            options,
            stats: AtomicStats::default(),
            safe_lsn: AtomicU64::new(0),
        }
    }

    pub(crate) fn begin<'a, E: Env>(&'a self, guard: Guard<'a, E>) -> TreeTxn<'a, E> {
        TreeTxn::new(self, guard)
    }

    pub(crate) fn stats(&self) -> Stats {
        self.stats.snapshot()
    }

    pub(crate) fn safe_lsn(&self) -> u64 {
        self.safe_lsn.load(Ordering::Acquire)
    }

    pub(crate) fn set_safe_lsn(&self, lsn: u64) {
        loop {
            let safe_lsn = self.safe_lsn.load(Ordering::Acquire);
            // Make sure that the safe LSN is increasing.
            if safe_lsn >= lsn {
                return;
            }
            if self
                .safe_lsn
                .compare_exchange(safe_lsn, lsn, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return;
            }
        }
    }
}

impl fmt::Debug for Tree {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Tree")
            .field("options", &self.options)
            .field("safe_lsn", &self.safe_lsn())
            .finish()
    }
}

impl<E: Env> RewritePage<E> for Arc<Tree> {
    type Rewrite<'a> = impl Future<Output = Result<(), Error>> + Send + 'a
        where
            Self: 'a;

    fn rewrite<'a>(&'a self, id: u64, guard: Guard<'a, E>) -> Self::Rewrite<'a> {
        async move {
            let txn = self.begin(guard);
            txn.rewrite_page(id).await
        }
    }
}

pub(crate) struct TreeTxn<'a, E: Env> {
    tree: &'a Tree,
    guard: Guard<'a, E>,
}

impl<'a, E: Env> TreeTxn<'a, E> {
    fn new(tree: &'a Tree, guard: Guard<'a, E>) -> Self {
        Self { tree, guard }
    }

    /// Initializes the tree if it is not initialized yet.
    pub(crate) async fn init(&self) -> Result<()> {
        let addr = self.guard.page_addr(ROOT_ID);
        if addr != 0 {
            return Ok(());
        }

        // Insert an empty data page as the root.
        let iter: ItemIter<(Key, Value)> = None.into();
        let builder = SortedPageBuilder::new(PageTier::Leaf, PageKind::Data).with_iter(iter);
        let mut txn = self.guard.begin();
        let (new_addr, mut new_page) = txn.alloc_page(builder.size())?;
        builder.build(&mut new_page);
        let root_id = txn.insert_page(new_addr);
        assert_eq!(root_id, ROOT_ID);
        txn.commit();

        Ok(())
    }

    /// Gets the value corresponding to the key.
    pub(crate) async fn get(&self, key: Key<'_>) -> Result<Option<&[u8]>> {
        let (view, _) = self.find_leaf(key.raw).await?;
        self.find_value(&key, &view).await
    }

    /// Writes the key-value pair to the tree.
    pub(crate) async fn write(&self, key: Key<'_>, value: Value<'_>) -> Result<()> {
        loop {
            match self.try_write(key, value).await {
                Ok(_) => {
                    self.tree.stats.success.write.inc();
                    return Ok(());
                }
                Err(Error::Again) => {
                    self.tree.stats.conflict.write.inc();
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
    }

    async fn try_write(&self, key: Key<'_>, value: Value<'_>) -> Result<()> {
        let (mut view, parent) = self.find_leaf(key.raw).await?;

        // Try to split the page before every write to avoid starving the split
        // operation due to contentions.
        if self.should_split_page(view.page)
            && self.split_page(view.clone(), parent.clone()).await.is_ok()
        {
            return Err(Error::Again);
        }

        // Build a delta page with the given key-value pair.
        let delta = (key, value);
        let builder = SortedPageBuilder::new(PageTier::Leaf, PageKind::Data).with_item(delta);
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
                    view.addr = new_addr;
                    view.page = new_page.into();
                    break;
                }
                Err(None) => return Err(Error::Again),
                Err(Some((_txn, addr))) => {
                    // The page has been updated by other transactions.
                    // We can keep retrying as long as the page epoch remains
                    // the same. However, this doesn't work for the root
                    // because we split the root without updating its epoch.
                    if view.id != ROOT_ID {
                        let page = self.guard.read_page(addr).await?;
                        if page.epoch() == view.page.epoch() {
                            txn = _txn;
                            view.addr = addr;
                            view.page = page;
                            continue;
                        }
                    }
                    return Err(Error::Again);
                }
            }
        }

        // Try to consolidate the page if it is too long.
        if self.should_consolidate_page(view.page) {
            let _ = self.consolidate_and_restructure_page(view, parent).await;
        }
        Ok(())
    }

    /// Returns a view to the page.
    async fn page_view<'g>(&'g self, id: u64, range: Option<Range<'g>>) -> Result<PageView<'g>> {
        let addr = self.guard.page_addr(id);
        let page = self.guard.read_page(addr).await?;
        Ok(PageView {
            id,
            addr,
            page,
            range,
        })
    }

    /// Finds the leaf page that may contain the key.
    ///
    /// Returns the leaf page and its parent.
    async fn find_leaf(&self, key: &[u8]) -> Result<(PageView<'_>, Option<PageView<'_>>)> {
        loop {
            match self.try_find_leaf(key).await {
                Ok((view, parent)) => {
                    self.tree.stats.success.read.inc();
                    return Ok((view, parent));
                }
                Err(Error::Again) => {
                    self.tree.stats.conflict.read.inc();
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
    }

    async fn try_find_leaf(&self, key: &[u8]) -> Result<(PageView<'_>, Option<PageView<'_>>)> {
        // The index, range, and parent of the current page, starting from the root.
        let mut index = ROOT_INDEX;
        let mut range = ROOT_RANGE;
        let mut parent = None;
        loop {
            let view = self.page_view(index.id, Some(range)).await?;
            // If the page epoch has changed, the page may not contain the data we expect
            // anymore. Try to reconcile pending conflicts and restart the operation.
            if view.page.epoch() != index.epoch {
                let _ = self.reconcile_page(view, parent).await;
                return Err(Error::Again);
            }
            if view.page.tier().is_leaf() {
                return Ok((view, parent));
            }
            // Find the child page that may contain the key.
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
    async fn walk_page<'g, F>(&'g self, mut page: PageRef<'g>, mut f: F) -> Result<()>
    where
        F: FnMut(PageRef<'g>) -> bool,
    {
        loop {
            if f(page) || page.chain_next() == 0 {
                return Ok(());
            }
            page = self.guard.read_page(page.chain_next()).await?;
        }
    }

    /// Creates an iterator over the key-value pairs in the page.
    #[allow(dead_code)]
    async fn iter_page<'g, K, V>(&'g self, view: &PageView<'g>) -> Result<MergingPageIter<'g, K, V>>
    where
        K: SortedPageKey,
        V: SortedPageValue,
    {
        let mut builder = MergingIterBuilder::with_capacity(view.page.chain_len() as usize);
        let mut range_limit = None;
        self.walk_page(view.page, |page| {
            match page.kind() {
                PageKind::Data => {
                    builder.add(SortedPageIter::from(page));
                }
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
        Ok(MergingPageIter::new(builder.build(), range_limit))
    }

    /// Finds the value corresponding to the key from the page.
    async fn find_value<'g>(
        &'g self,
        key: &Key<'_>,
        view: &PageView<'g>,
    ) -> Result<Option<&'g [u8]>> {
        let mut value = None;
        self.walk_page(view.page, |page| {
            debug_assert!(page.tier().is_leaf());
            // We only care about data pages here.
            if page.kind().is_data() {
                let page = ValuePageRef::from(page);
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
        key: &[u8],
        view: &PageView<'g>,
    ) -> Result<Option<(Index, Range<'g>)>> {
        let mut child = None;
        self.walk_page(view.page, |page| {
            debug_assert!(page.tier().is_inner());
            // We only care about data pages here.
            if page.kind().is_data() {
                let page = IndexPageRef::from(page);
                // Find the two items that enclose the key.
                let (left, right) = match page.rank(&key) {
                    // The `i` item is equal to the key, so the range is [i, i + 1).
                    Ok(i) => (page.get(i), i.checked_add(1).and_then(|i| page.get(i))),
                    // The `i` item is greater than the key, so the range is [i - 1, i).
                    Err(i) => (i.checked_sub(1).and_then(|i| page.get(i)), page.get(i)),
                };
                if let Some((start, index)) = left {
                    if index != NULL_INDEX {
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

    // Splits the page into two halves.
    async fn split_page(&self, view: PageView<'_>, parent: Option<PageView<'_>>) -> Result<()> {
        // We can only split base data pages.
        if !view.page.kind().is_data() || view.page.chain_next() != 0 {
            return Err(Error::InvalidArgument);
        }
        match view.page.tier() {
            PageTier::Leaf => self.split_page_impl::<Key, Value>(view, parent).await,
            PageTier::Inner => self.split_page_impl::<&[u8], Index>(view, parent).await,
        }
    }

    async fn split_page_impl<K, V>(
        &self,
        mut view: PageView<'_>,
        parent: Option<PageView<'_>>,
    ) -> Result<()>
    where
        K: SortedPageKey,
        V: SortedPageValue,
    {
        if view.id == ROOT_ID {
            return self.split_root_impl::<K, V>(view).await;
        }

        let page = SortedPageRef::<K, V>::from(view.page);
        let Some((split_key, _, right_iter)) = page.into_split_iter() else {
            return Ok(());
        };

        let mut txn = self.guard.begin();
        // Build and insert the right page.
        let right_id = {
            let builder =
                SortedPageBuilder::new(view.page.tier(), PageKind::Data).with_iter(right_iter);
            let (new_addr, mut new_page) = txn.alloc_page(builder.size())?;
            builder.build(&mut new_page);
            txn.insert_page(new_addr)
        };
        // Build a delta page with the right index.
        let delta = (split_key.as_raw(), Index::new(right_id, 0));
        let builder = SortedPageBuilder::new(view.page.tier(), PageKind::Split).with_item(delta);
        let (new_addr, mut new_page) = txn.alloc_page(builder.size())?;
        builder.build(&mut new_page);
        // Update the left page with the delta.
        // The page epoch must be updated to indicate the change of the page range.
        new_page.set_epoch(view.page.epoch() + 1);
        new_page.set_chain_len(view.page.chain_len().saturating_add(1));
        new_page.set_chain_next(view.addr);
        txn.update_page(view.id, view.addr, new_addr)
            .map(|_| {
                trace!("split page {:?} with delta {:?}", view, delta);
                self.tree.stats.success.split_page.inc();
                view.addr = new_addr;
                view.page = new_page.into();
            })
            .map_err(|_| {
                self.tree.stats.conflict.split_page.inc();
                Error::Again
            })?;

        // Try to reconcile the page after a split.
        let _ = self.reconcile_page(view, parent).await;
        Ok(())
    }

    async fn split_root_impl<K, V>(&self, view: PageView<'_>) -> Result<()>
    where
        K: SortedPageKey,
        V: SortedPageValue,
    {
        assert_eq!(view.id, ROOT_ID);
        assert_eq!(view.page.epoch(), 0);
        assert_eq!(view.page.chain_len(), 1);

        let page = SortedPageRef::<K, V>::from(view.page);
        let Some((split_key, left_iter, right_iter)) = page.into_split_iter() else {
            return Ok(());
        };

        let mut txn = self.guard.begin();
        // Build and insert the left page.
        let left_id = {
            let builder =
                SortedPageBuilder::new(view.page.tier(), PageKind::Data).with_iter(left_iter);
            let (new_addr, mut new_page) = txn.alloc_page(builder.size())?;
            builder.build(&mut new_page);
            txn.insert_page(new_addr)
        };
        // Build and insert the right page.
        let right_id = {
            let builder =
                SortedPageBuilder::new(view.page.tier(), PageKind::Data).with_iter(right_iter);
            let (new_addr, mut new_page) = txn.alloc_page(builder.size())?;
            builder.build(&mut new_page);
            txn.insert_page(new_addr)
        };
        // Build a delta page with the right index.
        let delta = [
            ([].as_slice(), Index::new(left_id, 0)),
            (split_key.as_raw(), Index::new(right_id, 0)),
        ];
        let builder = SortedPageBuilder::new(PageTier::Inner, PageKind::Data).with_slice(&delta);
        let (new_addr, mut new_page) = txn.alloc_page(builder.size())?;
        builder.build(&mut new_page);
        // Replace and deallocate the original root.
        txn.replace_page(view.id, view.addr, new_addr, &[view.addr])
            .map(|_| {
                trace!("split root {:?} with delta {:?}", view, delta);
                self.tree.stats.success.split_page.inc();
            })
            .map_err(|_| {
                self.tree.stats.conflict.split_page.inc();
                Error::Again
            })
    }

    /// Reconciles any conflicts on the page.
    async fn reconcile_page(&self, view: PageView<'_>, parent: Option<PageView<'_>>) -> Result<()> {
        let result = match view.page.kind() {
            PageKind::Data => Ok(()),
            PageKind::Split => {
                if let Some(parent) = parent {
                    self.reconcile_split_page(view, parent).await
                } else {
                    Err(Error::InvalidArgument)
                }
            }
        };
        match result {
            Ok(_) => {
                self.tree.stats.success.reconcile_page.inc();
                Ok(())
            }
            Err(e) => {
                if let Error::Again = e {
                    self.tree.stats.conflict.reconcile_page.inc();
                }
                Err(e)
            }
        }
    }

    // Reconciles a pending split on the page.
    async fn reconcile_split_page(
        &self,
        view: PageView<'_>,
        mut parent: PageView<'_>,
    ) -> Result<()> {
        let Some(range) = view.range else {
            return Err(Error::InvalidArgument);
        };
        let left_key = range.start;
        let left_index = Index::new(view.id, view.page.epoch());
        let (split_key, split_index) = split_delta_from_page(view.page);
        // Build a delta page with the child on the left and the new split page on
        // the right.
        let delta = if let Some(range_end) = range.end {
            assert!(split_key < range_end);
            vec![
                (left_key, left_index),
                (split_key, split_index),
                // This is a placeholder to indicate the range end of the right page.
                (range_end, NULL_INDEX),
            ]
        } else {
            vec![(left_key, left_index), (split_key, split_index)]
        };
        let builder = SortedPageBuilder::new(PageTier::Inner, PageKind::Data).with_slice(&delta);
        let mut txn = self.guard.begin();
        let (new_addr, mut new_page) = txn.alloc_page(builder.size())?;
        builder.build(&mut new_page);
        // Update the parent page with the delta.
        new_page.set_epoch(parent.page.epoch());
        new_page.set_chain_len(parent.page.chain_len().saturating_add(1));
        new_page.set_chain_next(parent.addr);
        txn.update_page(parent.id, parent.addr, new_addr)
            .map(|_| {
                trace!("reconciled split page {:?} with delta {:?}", view, delta);
                parent.addr = new_addr;
                parent.page = new_page.into();
            })
            .map_err(|_| Error::Again)?;

        // Try to consolidate the parent page if it is too long.
        if self.should_consolidate_page(parent.page) {
            let _ = self
                .consolidate_page(parent, ConsolidationKind::Partial)
                .await;
        }
        Ok(())
    }

    /// Rewrites the page to reclaim its space.
    async fn rewrite_page(&self, id: u64) -> Result<()> {
        loop {
            match self.try_rewrite_page(id).await {
                Ok(()) => return Ok(()),
                Err(Error::Again) => continue,
                Err(err) => return Err(err),
            }
        }
    }

    async fn try_rewrite_page(&self, id: u64) -> Result<()> {
        let view = self.page_view(id, None).await?;
        self.consolidate_page(view, ConsolidationKind::Full).await?;
        Ok(())
    }

    /// Consolidates delta pages on the page chain.
    ///
    /// If `kind` is [`ConsolidationKind::Full`], consolidates all delta pages
    /// on the chain.
    /// If `kind` is [`ConsolidationKind::Partial`], the implementation may
    /// choose to consolidate only a subset of delta pages on the chain.
    async fn consolidate_page<'g>(
        &'g self,
        view: PageView<'g>,
        kind: ConsolidationKind,
    ) -> Result<PageView<'g>> {
        match view.page.tier() {
            PageTier::Leaf => {
                let safe_lsn = self.tree.safe_lsn();
                self.consolidate_page_impl(view, kind, |iter| {
                    MergingLeafPageIter::new(iter, safe_lsn)
                })
                .await
            }
            PageTier::Inner => {
                self.consolidate_page_impl(view, kind, MergingInnerPageIter::new)
                    .await
            }
        }
    }

    async fn consolidate_page_impl<'g, F, I, K, V>(
        &'g self,
        mut view: PageView<'g>,
        kind: ConsolidationKind,
        f: F,
    ) -> Result<PageView<'g>>
    where
        F: Fn(MergingPageIter<'g, K, V>) -> I,
        I: RewindableIterator<Item = (K, V)>,
        K: SortedPageKey,
        V: SortedPageValue,
    {
        // Collect information for this consolidation.
        let info = self.collect_consolidation_info(&view, kind).await?;
        let iter = f(info.iter);
        let builder = SortedPageBuilder::new(view.page.tier(), PageKind::Data).with_iter(iter);
        let mut txn = self.guard.begin();
        let (new_addr, mut new_page) = txn.alloc_page(builder.size())?;
        builder.build(&mut new_page);
        new_page.set_epoch(view.page.epoch());
        new_page.set_chain_len(info.last_page.chain_len());
        new_page.set_chain_next(info.last_page.chain_next());
        // Update the page and deallocate the consolidated delta pages.
        txn.replace_page(view.id, view.addr, new_addr, &info.page_addrs)
            .map(|_| {
                trace!("consolidated page {:?}", view);
                self.tree.stats.success.consolidate_page.inc();
                view.addr = new_addr;
                view.page = new_page.into();
                view
            })
            .map_err(|_| {
                self.tree.stats.conflict.consolidate_page.inc();
                Error::Again
            })
    }

    /// Collects some information to consolidate a page.
    async fn collect_consolidation_info<'g, K, V>(
        &'g self,
        view: &PageView<'g>,
        kind: ConsolidationKind,
    ) -> Result<ConsolidationInfo<'g, K, V>>
    where
        K: SortedPageKey,
        V: SortedPageValue,
    {
        let chain_len = view.page.chain_len() as usize;
        let mut builder = MergingIterBuilder::with_capacity(chain_len);
        let mut page_size = 0;
        let mut last_page = view.page;
        let mut next_addr = view.addr;
        let mut page_addrs = Vec::with_capacity(chain_len);
        let mut range_limit = None;
        self.walk_page(view.page, |page| {
            match page.kind() {
                PageKind::Data => {
                    // Inner pages can not do partial consolidations because of the placeholders.
                    // This is fine since inner pages doesn't consolidate as often as leaf pages.
                    if kind == ConsolidationKind::Partial
                        && page.tier().is_leaf()
                        && builder.len() >= 2
                        && page_size < page.size() / 2
                        && range_limit.is_none()
                        && !self.should_consolidate_page(page)
                    {
                        return true;
                    }
                    builder.add(SortedPageIter::from(page));
                    page_size += page.size();
                }
                PageKind::Split => {
                    if range_limit.is_none() {
                        let (split_key, _) = split_delta_from_page(page);
                        range_limit = Some(split_key);
                    }
                }
            }
            page_addrs.push(next_addr);
            last_page = page;
            next_addr = page.chain_next();
            false
        })
        .await?;
        let iter = MergingPageIter::new(builder.build(), range_limit);
        Ok(ConsolidationInfo {
            iter,
            last_page,
            page_addrs,
        })
    }

    /// Consolidates and restructures a page.
    async fn consolidate_and_restructure_page<'g>(
        &'g self,
        mut view: PageView<'g>,
        parent: Option<PageView<'g>>,
    ) -> Result<()> {
        view = self
            .consolidate_page(view, ConsolidationKind::Partial)
            .await?;
        // Try to split the page if it is too large.
        if self.should_split_page(view.page) {
            let _ = self.split_page(view, parent).await;
        }
        Ok(())
    }

    // Returns true if the page should be split.
    fn should_split_page(&self, page: PageRef<'_>) -> bool {
        let mut max_size = self.tree.options.page_size;
        if page.tier().is_inner() {
            // Adjust the page size for inner pages.
            max_size /= 2;
        }
        page.size() > max_size && page.chain_next() == 0
    }

    // Returns true if the page should be consolidated.
    fn should_consolidate_page(&self, page: PageRef<'_>) -> bool {
        let mut max_chain_len = self.tree.options.page_chain_length;
        if page.tier().is_inner() {
            // Adjust the chain length for inner pages.
            max_chain_len /= 2;
        }
        page.chain_len() as usize > max_chain_len.max(1)
    }
}

/// An iterator over leaf pages in a tree.
pub(crate) struct TreeIter<'a, 't: 'a, E: Env> {
    txn: &'a TreeTxn<'t, E>,
    options: ReadOptions,
    inner_iter: Option<MergingInnerPageIter<'a>>,
    inner_next: Option<&'a [u8]>,
}

impl<'a, 't: 'a, E: Env> TreeIter<'a, 't, E> {
    pub(crate) fn new(txn: &'a TreeTxn<'t, E>, options: ReadOptions) -> Self {
        Self {
            txn,
            options,
            inner_iter: None,
            inner_next: None,
        }
    }

    async fn seek(&mut self, target: &[u8]) -> Result<PageIter<'_>> {
        let (view, parent) = self.txn.find_leaf(target).await?;
        let iter = self.txn.iter_page(&view).await?;
        let mut leaf_iter = PageIter::new(iter, self.options.max_lsn);
        leaf_iter.seek(target);
        if let Some(parent) = parent {
            let iter = self.txn.iter_page(&parent).await?;
            let mut iter = MergingInnerPageIter::new(iter);
            if iter.seek(target) {
                iter.next();
            }
            self.inner_iter = Some(iter);
            self.inner_next = parent.range.unwrap().end;
        } else {
            self.inner_iter = None;
            self.inner_next = None;
        }
        Ok(leaf_iter)
    }

    pub(crate) async fn next_page(&mut self) -> Result<Option<PageIter<'_>>> {
        let Some(inner_iter) = self.inner_iter.as_mut() else {
            return Ok(None);
        };
        let mut inner_next = self.inner_next;
        if let Some((start, index)) = inner_iter.next() {
            let view = self.txn.page_view(index.id, None).await?;
            if view.page.epoch() == index.epoch {
                let iter = self.txn.iter_page(&view).await?;
                return Ok(Some(PageIter::new(iter, self.options.max_lsn)));
            } else {
                // The page epoch has changed, we need to restart from this.
                inner_next = Some(start);
            }
        }
        if let Some(next) = inner_next {
            let iter = self.seek(next).await?;
            Ok(Some(iter))
        } else {
            self.inner_iter = None;
            Ok(None)
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum ConsolidationKind {
    /// Consolidates all delta pages on the chain.
    Full,
    /// Consolidates two or more delta pages on the chain.
    Partial,
}

struct ConsolidationInfo<'a, K, V>
where
    K: SortedPageKey,
    V: SortedPageValue,
{
    iter: MergingPageIter<'a, K, V>,
    last_page: PageRef<'a>,
    page_addrs: Vec<u64>,
}

fn split_delta_from_page(page: PageRef<'_>) -> (&[u8], Index) {
    debug_assert!(page.kind().is_split());
    IndexPageRef::from(page)
        .get(0)
        .expect("split page delta must exist")
}
