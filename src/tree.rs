use std::path::Path;

use crate::{
    data::{Entry, Key, Range},
    env::Env,
    page::{PageBuf, PageEpoch, PageKind, PageRef, PageTier, SortedPageBuilder},
    page_store::{Error, Guard, PageStore, PageTxn, Result},
    util::Counter,
    Options,
};

pub(crate) struct Tree<E> {
    options: Options,
    stats: AtomicTreeStats,
    store: PageStore<E>,
}

impl<E: Env> Tree<E> {
    pub(crate) async fn open<P: AsRef<Path>>(env: E, path: P, options: Options) -> Result<Self> {
        let stats = AtomicTreeStats::default();
        let store = PageStore::open(env, path).await?;
        Ok(Self {
            options,
            stats,
            store,
        })
    }

    fn begin(&self) -> TreeTxn {
        let guard = self.store.guard();
        TreeTxn::new(&self.options, &self.stats, guard)
    }

    pub(crate) async fn get<F, R>(&self, key: Key<'_>, f: F) -> Result<R>
    where
        F: FnOnce(Option<&[u8]>) -> R,
    {
        loop {
            let txn = self.begin();
            match txn.get(&key).await {
                Ok(value) => return Ok(f(value)),
                Err(Error::Again) => continue,
                Err(e) => return Err(e),
            }
        }
    }

    pub(crate) async fn write(&self, entry: Entry<'_>) -> Result<()> {
        loop {
            let txn = self.begin();
            match txn.write(&entry).await {
                Ok(_) => return Ok(()),
                Err(Error::Again) => continue,
                Err(e) => return Err(e),
            }
        }
    }

    pub(crate) fn stats(&self) -> TreeStats {
        self.stats.snapshot()
    }
}

struct TreeTxn<'a> {
    options: &'a Options,
    stats: &'a AtomicTreeStats,
    guard: Guard,
}

impl<'a> TreeTxn<'a> {
    fn new(options: &'a Options, stats: &'a AtomicTreeStats, guard: Guard) -> Self {
        Self {
            options,
            stats,
            guard,
        }
    }

    async fn get(&self, key: &Key<'_>) -> Result<Option<&[u8]>> {
        let (view, _) = self.find_leaf(key).await?;
        self.find_value(key, &view).await
    }

    async fn write(&self, entry: &Entry<'_>) -> Result<()> {
        let (mut view, _) = self.find_leaf(&entry.key).await?;
        // let builder = SortedPageBuilder::new(PageTier::Leaf, PageKind::Data);

        let mut txn = self.guard.begin();
        let (new_addr, mut new_page) = txn.alloc_page(0)?;
        // builder.build(&mut new_page);
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

    async fn find_leaf(&self, key: &Key<'_>) -> Result<(PageView<'_>, Option<PageView<'_>>)> {
        let mut index = PageIndex::new(0);
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

    async fn find_child(&self, key: &Key<'_>, view: &PageView<'_>) -> Result<(PageIndex, Range)> {
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
        new_page.set_epoch(view.page.epoch().next());
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

pub(crate) struct TxnStats {
    get: u64,
    write: u64,
    split_page: u64,
    consolidate_page: u64,
}

pub(crate) struct TreeStats {
    success: TxnStats,
    failure: TxnStats,
}

#[derive(Default)]
pub(crate) struct AtomicTxnStats {
    get: Counter,
    write: Counter,
    page_split: Counter,
    page_consolidate: Counter,
}

impl AtomicTxnStats {
    fn snapshot(&self) -> TxnStats {
        TxnStats {
            get: self.get.get(),
            write: self.write.get(),
            split_page: self.page_split.get(),
            consolidate_page: self.page_consolidate.get(),
        }
    }
}

#[derive(Default)]
pub(crate) struct AtomicTreeStats {
    success: AtomicTxnStats,
    failure: AtomicTxnStats,
}

impl AtomicTreeStats {
    fn snapshot(&self) -> TreeStats {
        TreeStats {
            success: self.success.snapshot(),
            failure: self.failure.snapshot(),
        }
    }
}

struct PageView<'a> {
    id: u64,
    addr: u64,
    page: PageRef<'a>,
    range: Range<'a>,
}

#[derive(Copy, Clone, Debug)]
struct PageIndex {
    id: u64,
    epoch: PageEpoch,
}

impl PageIndex {
    fn new(id: u64) -> Self {
        Self {
            id,
            epoch: PageEpoch::default(),
        }
    }
}
