use super::Tree;
use crate::{page::*, page_store::*};

const ROOT_ID: u64 = 0;
const NULL_ID: u64 = 0;

pub(super) struct Txn<'a, E> {
    tree: &'a Tree<E>,
    guard: Guard,
}

impl<'a, E> Txn<'a, E> {
    pub(super) fn new(tree: &'a Tree<E>, guard: Guard) -> Self {
        Self { tree, guard }
    }

    /// Gets the value corresponding to the key.
    pub(super) async fn get(&self, key: Key<'_>) -> Result<Option<&[u8]>> {
        let (view, _) = self.find_leaf(&key).await?;
        self.find_value(&key, &view).await
    }

    /// Writes the key-value pair to the tree.
    pub(super) async fn write(&self, key: Key<'_>, value: Value<'_>) -> Result<()> {
        let (mut view, parent) = self.find_leaf(&key).await?;

        let mut txn = self.guard.begin();
        // Build a delta page with the given key-value pair.
        let iter = ItemIter::new((key, value));
        let builder = SortedPageBuilder::new(PageTier::Leaf, PageKind::Data).with_iter(iter);
        let (new_addr, mut new_page) = txn.alloc_page(builder.size())?;
        builder.build(&mut new_page);
        // Update the corresponding leaf page with the delta page.
        loop {
            new_page.set_epoch(view.page.epoch());
            new_page.set_chain_len(view.page.chain_len().saturating_add(1));
            new_page.set_chain_next(view.addr);
            match txn.update_page(view.id, view.addr, new_addr) {
                Ok(_) => break,
                Err(addr) => {
                    // The page has been updated by other transactions.
                    // We can keep retrying as long as the page epoch remains the same.
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
        if new_page.chain_len() as usize >= self.tree.options.page_chain_length {
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
        let mut index = Index::new(ROOT_ID);
        let mut range = Range::default();
        let mut parent = None;
        loop {
            let addr = self.guard.page_addr(index.id);
            let page = self.guard.read_page(addr).await?;
            let view = PageView {
                id: index.id,
                addr,
                page,
                range,
            };
            // If the page epoch has changed, the page may not contain the expect data
            // anymore. Try to reconcile pending conflicts and restart the operation.
            if view.page.epoch() != index.epoch {
                let _ = self.reconcile_page(view, parent).await;
                return Err(Error::Again);
            }
            if view.page.tier().is_leaf() {
                return Ok((view, parent));
            }
            let (child_index, child_range) = self.find_child(key, &view).await?;
            index = child_index;
            range.start = child_range.start;
            // If the child has no range end, use the parent's range end instead.
            if let Some(end) = child_range.end {
                range.end = Some(end);
            }
            parent = Some(view);
        }
    }

    /// Walks through the page chain and applies a function on each page.
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

    /// Finds the value corresponding to the key in the given page.
    async fn find_value<'g>(
        &'g self,
        key: &Key<'_>,
        view: &PageView<'g>,
    ) -> Result<Option<&'g [u8]>> {
        let mut value = None;
        self.walk_page(view, |page| {
            debug_assert!(page.tier().is_leaf());
            if page.kind().is_data() {
                let page = LeafPageRef::from(page);
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

    /// Finds the child page that may contain the key in the given page.
    ///
    /// Returns the index and the range of the child page.
    async fn find_child<'g>(
        &'g self,
        key: &Key<'_>,
        view: &PageView<'g>,
    ) -> Result<(Index, Range<'g>)> {
        let mut child_index = Index::default();
        let mut child_range = Range::default();
        self.walk_page(view, |page| {
            debug_assert!(page.tier().is_inner());
            if page.kind().is_data() {
                let mut iter = InnerPageIter::from(page);
                iter.seek_back(key);
                let (start, index) = iter.next().expect("child must exist");
                child_index = index;
                child_range.start = start;
                if let Some((end, _)) = iter.next() {
                    child_range.end = Some(end);
                }
            }
            false
        })
        .await?;
        Ok((child_index, child_range))
    }

    async fn split_page(&self, view: PageView<'_>, parent: Option<PageView<'_>>) -> Result<()> {
        // We can only split on base data pages.
        if !view.page.kind().is_data() || view.page.chain_next() != 0 {
            return Err(Error::InvalidArgument);
        }

        let data_page = DataPageRef::from(view.page);
        if let Some((split_key, right_iter)) = data_page.split() {
            let mut txn = self.guard.begin();
            // Build and insert the right page.
            let right_id = {
                let builder = SortedPageBuilder::new(view.page.tier(), view.page.kind())
                    .with_iter(right_iter);
                let (new_addr, mut new_page) = txn.alloc_page(builder.size())?;
                builder.build(&mut new_page);
                txn.insert_page(new_addr)
            };
            // Build and update the left page.
            {
                let iter = ItemIter::new((split_key, Index::new(right_id)));
                let builder =
                    SortedPageBuilder::new(view.page.tier(), PageKind::Split).with_iter(iter);
                let (new_addr, mut new_page) = txn.alloc_page(builder.size())?;
                new_page.set_epoch(view.page.epoch() + 1);
                new_page.set_chain_len(view.page.chain_len().saturating_add(1));
                new_page.set_chain_next(view.addr);
                txn.update_page(view.id, view.addr, new_addr)
                    .map(|_| {
                        self.tree.stats.success.split_page.inc();
                    })
                    .map_err(|_| {
                        self.tree.stats.restart.split_page.inc();
                        Error::Again
                    })?;
            }
        }

        Ok(())
    }

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

    async fn reconcile_split_page(&self, view: PageView<'_>, parent: PageView<'_>) -> Result<()> {
        debug_assert!(view.page.kind().is_split());
        let page = InnerPageRef::from(view.page);
        let (split_key, split_index) = page.get(0).unwrap();

        let left_key = view.range.start;
        let left_index = Index::with_epoch(view.id, view.page.epoch());
        let iter = ArrayIter::new([(left_key, left_index), (split_key, split_index)]);
        let builder = SortedPageBuilder::new(PageTier::Inner, PageKind::Data).with_iter(iter);

        let mut txn = self.guard.begin();
        let (new_addr, mut new_page) = txn.alloc_page(builder.size())?;
        builder.build(&mut new_page);
        new_page.set_epoch(parent.page.epoch());
        new_page.set_chain_len(parent.page.chain_len().saturating_add(1));
        new_page.set_chain_next(parent.addr);
        txn.update_page(parent.id, parent.addr, new_addr)
            .map_err(|_| Error::Again)
    }

    async fn reconcile_split_root(&self, view: PageView<'_>) -> Result<()> {
        debug_assert!(view.page.kind().is_split());
        let split_page = SplitPageRef::from(view.page);
        let (split_key, split_index) = split_page.get(0).unwrap();

        let mut txn = self.guard.begin();
        // Move the original root page to another place.
        let left_id = txn.insert_page(view.addr);
        let left_key = Key::default();
        let left_index = Index::with_epoch(left_id, view.page.epoch());
        // Build a new root with the original root page in the left and the split page
        // in the right.
        let iter = ArrayIter::new([(left_key, left_index), (split_key, split_index)]);
        let builder = SortedPageBuilder::new(PageTier::Inner, PageKind::Data).with_iter(iter);
        let (new_addr, mut new_page) = txn.alloc_page(builder.size())?;
        builder.build(&mut new_page);
        txn.update_page(view.id, view.addr, new_addr)
            .map_err(|_| Error::Again)
    }

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
type LeafPageRef<'a> = SortedPageRef<'a, Value<'a>>;
type InnerPageRef<'a> = SortedPageRef<'a, Index>;
type InnerPageIter<'a> = SortedPageIter<'a, Index>;
type SplitPageRef<'a> = SortedPageRef<'a, Index>;

struct MergingDataPageIter<'a> {
    iter: MergingIter<DataPageIter<'a>>,
    range_end: Option<Key<'a>>,
}
