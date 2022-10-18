use super::stats::AtomicTreeStats;
use crate::{
    page::{
        Index, ItemIter, Key, PageBuf, PageKind, PageRef, PageTier, Range, SortedPageBuilder, Value,
    },
    page_store::{Error, Guard, PageTxn, Result},
    Options,
};

pub(super) struct TreeTxn<'a> {
    options: &'a Options,
    stats: &'a AtomicTreeStats,
    guard: Guard,
}

impl<'a> TreeTxn<'a> {
    pub(super) fn new(options: &'a Options, stats: &'a AtomicTreeStats, guard: Guard) -> Self {
        Self {
            options,
            stats,
            guard,
        }
    }

    pub(super) async fn get(&self, key: Key<'_>) -> Result<Option<&[u8]>> {
        let (view, _) = self.find_leaf(key).await?;
        self.find_value(key, &view).await
    }

    pub(super) async fn write(&self, key: Key<'_>, value: Value<'_>) -> Result<()> {
        // Builds a delta page with the given key-value pair.
        let iter = ItemIter::new((key, value));
        let builder = SortedPageBuilder::new(PageTier::Leaf, PageKind::Data).with_iter(iter);
        let mut txn = self.guard.begin();
        let (new_addr, mut new_page) = txn.alloc_page(0)?;
        builder.build(&mut new_page);

        // Installs the delta page to the corresponding leaf page.
        let (mut view, _) = self.find_leaf(key).await?;
        loop {
            new_page.set_epoch(view.page.epoch());
            new_page.set_chain_len(view.page.chain_len());
            new_page.set_chain_next(view.addr.into());
            match txn.update_page(view.id, view.addr, new_addr) {
                Ok(_) => break,
                Err(addr) => {
                    let page = self.guard.read_page(addr).await?;
                    if page.epoch() == view.page.epoch() {
                        view.page = page;
                        continue;
                    }
                    return Err(Error::Again);
                }
            }
        }
        txn.commit();

        if new_page.chain_len() as usize >= self.options.page_chain_length {
            view.page = new_page.into();
            // It doesn't matter whether this consolidation succeeds or not.
            let _ = self.consolidate_page(view).await;
        }
        Ok(())
    }

    async fn find_leaf(&self, key: Key<'_>) -> Result<(PageView<'_>, Option<PageView<'_>>)> {
        let mut index = Index::new(0);
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
            // Do not continue if the page epoch has changed.
            if view.page.epoch() != index.epoch {
                self.reconcile_page(view, parent).await;
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

    async fn find_value(&self, key: Key<'_>, view: &PageView<'_>) -> Result<Option<&[u8]>> {
        self.walk_page(view, |page| {
            debug_assert!(page.tier().is_leaf());
            if page.kind() == PageKind::Data {
                todo!()
            }
            None
        })
        .await
    }

    async fn find_child(&self, key: Key<'_>, view: &PageView<'_>) -> Result<(Index, Range)> {
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

    async fn split_page(&self, view: PageView<'_>) -> Result<()> {
        todo!()
    }

    async fn reconcile_page(&self, view: PageView<'_>, parent: Option<PageView<'_>>) {
        let mut txn = self.guard.begin();
        todo!()
    }

    async fn consolidate_page(&self, view: PageView<'_>) -> Result<()> {
        let (iter, last_page) = self.delta_page_iter(&view).await;
        // let builder = DataPageBuilder::new(view.page.tier()).with_iter(iter);

        let mut txn = self.guard.begin();
        let (new_addr, mut new_page) = txn.alloc_page(0)?;
        // builder.build(&mut new_page);
        new_page.set_epoch(view.page.epoch());
        new_page.set_chain_len(last_page.chain_len());
        new_page.set_chain_next(last_page.chain_next());
        txn.replace_page(view.id, view.addr, new_addr)
            .map_err(|_| Error::Again)?;
        txn.commit();

        if new_page.chain_next() == 0 && new_page.size() >= self.options.page_size {
            let _ = self.split_page(view);
        }
        Ok(())
    }

    async fn delta_page_iter(&self, view: &PageView<'_>) -> ((), PageRef<'_>) {
        todo!()
    }
}

struct PageView<'a> {
    id: u64,
    addr: u64,
    page: PageRef<'a>,
    range: Range<'a>,
}
