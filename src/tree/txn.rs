use super::Tree;
use crate::{
    page::{
        ArrayIter, Index, ItemIter, Key, MergingIter, PageBuf, PageKind, PageRef, PageTier, Range,
        SortedPageBuilder, SortedPageIter, SortedPageRef, Value,
    },
    page_store::{Error, Guard, PageTxn, Result},
};

const ROOT_ID: u64 = 0;

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

    /// Writes the given key-value pair to the tree.
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
            // If the page epoch has changed, the page may not contain the expect data anymore.
            // Try to reconcile any pending conflicts and retry the operation.
            if view.page.epoch() != index.epoch {
                let _ = self.reconcile_page(view, parent).await;
                return Err(Error::Again);
            }
            if view.page.tier().is_leaf() {
                return Ok((view, parent));
            }
            let (child_index, child_range) = self.find_child(key, &view).await?;
            index = child_index;
            range = child_range;
            parent = Some(view);
        }
    }

    /// Walks through the page chain and applies a function on each page.
    ///
    /// This function walks through pages on the chain one by one and applies the given function on
    /// each page. If the applied function returns some result on a page, the walk stops and returns
    /// that result. Otherwise, it walks until the end of the chain and returns none.
    async fn walk_page<F, R>(&self, view: &PageView<'_>, mut f: F) -> Result<Option<R>>
    where
        F: FnMut(PageRef<'_>) -> Option<R>,
    {
        let mut page = view.page;
        loop {
            if let Some(result) = f(page) {
                return Ok(Some(result));
            }
            if page.chain_next() == 0 {
                break;
            }
            page = self.guard.read_page(page.chain_next()).await?;
        }
        Ok(None)
    }

    /// Finds the value corresponding to the key in the given page.
    async fn find_value(&self, key: &Key<'_>, view: &PageView<'_>) -> Result<Option<&[u8]>> {
        self.walk_page(view, |page| {
            debug_assert!(page.tier().is_leaf());
            if page.kind() == PageKind::Data {
                todo!()
            }
            None
        })
        .await
    }

    /// Finds the child page that may contain the key in the given page.
    ///
    /// Returns the index and the range of the child page.
    async fn find_child(&self, key: &Key<'_>, view: &PageView<'_>) -> Result<(Index, Range)> {
        let child = self
            .walk_page(view, |page| {
                debug_assert!(page.tier().is_inner());
                if page.kind() == PageKind::Data {
                    todo!()
                }
                None
            })
            .await?;
        Ok(child.expect("child page must exist"))
    }

    async fn split_page(&self, view: PageView<'_>, parent: Option<PageView<'_>>) -> Result<()> {
        // We can only split on base data pages.
        if !view.page.kind().is_data() || view.page.chain_next() != 0 {
            return Err(Error::InvalidArgument);
        }

        let page = DataPageRef::from(view.page);
        if let Some((split_key, right_iter)) = page.split() {
            let mut txn = self.guard.begin();
            // Build and insert the right page.
            let right_id = {
                let builder =
                    SortedPageBuilder::new(page.tier(), page.kind()).with_iter(right_iter);
                let (new_addr, mut new_page) = txn.alloc_page(builder.size())?;
                builder.build(&mut new_page);
                txn.insert_page(new_addr)
            };
            // Build and update the left page.
            {
                let iter = ItemIter::new((split_key, Index::new(right_id)));
                let builder = SortedPageBuilder::new(page.tier(), PageKind::Split).with_iter(iter);
                let (new_addr, mut new_page) = txn.alloc_page(builder.size())?;
                new_page.set_epoch(page.epoch() + 1);
                new_page.set_chain_len(page.chain_len().saturating_add(1));
                new_page.set_chain_next(view.addr);
                txn.update_page(view.id, view.addr, new_addr)
                    .map_err(|_| Error::Again)?;
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
        let left_key = Key::new(view.range.start, 0);
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
        let page = InnerPageRef::from(view.page);
        let (split_key, split_index) = page.get(0).unwrap();

        let mut txn = self.guard.begin();
        // Move the original root page to another place.
        let left_id = txn.insert_page(view.addr);
        let left_key = Key::default();
        let left_index = Index::with_epoch(left_id, view.page.epoch());
        // Build a new root with the original root in the left and the new split page in the right.
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
        let (iter, last_page) = self.delta_page_iter(&view).await;
        let builder = SortedPageBuilder::new(view.page.tier(), view.page.kind()).with_iter(iter);
        let mut txn = self.guard.begin();
        let (new_addr, mut new_page) = txn.alloc_page(builder.size())?;
        builder.build(&mut new_page);
        new_page.set_epoch(view.page.epoch());
        new_page.set_chain_len(last_page.chain_len());
        new_page.set_chain_next(last_page.chain_next());
        txn.replace_page(view.id, view.addr, new_addr)
            .map_err(|_| Error::Again)?;

        // Try to split the page if it is too large.
        if new_page.size() >= self.tree.options.page_size {
            let _ = self.split_page(view, parent);
        }
        Ok(())
    }

    async fn delta_page_iter(&self, view: &PageView<'_>) -> (DataPageIter<'_>, PageRef<'_>) {
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
type DataPageIter<'a> = MergingIter<SortedPageIter<'a, &'a [u8]>>;
type InnerPageRef<'a> = SortedPageRef<'a, Index>;
