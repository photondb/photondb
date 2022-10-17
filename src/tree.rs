use std::sync::atomic::{AtomicU64, Ordering};

use crate::{
    data::{Entry, Key, Range},
    env::Env,
    page::{PageBuf, PageEpoch, PageKind, PageRef, PageTier, SortedPageBuilder},
    page_store::{Error, PageStore, PageTxn, Result},
    Options,
};

pub(crate) struct Tree<E> {
    opts: Options,
    root: AtomicId,
    store: PageStore<E>,
}

impl<E: Env> Tree<E> {
    pub(crate) async fn open(env: E, opts: Options) -> Result<Self> {
        let store = PageStore::open(env).await?;
        let txn = store.begin();
        let root = txn.alloc_id();
        txn.commit();
        Ok(Self {
            opts,
            root: AtomicId::new(root),
            store,
        })
    }

    fn begin(&self) -> TreeTxn {
        let txn = self.store.begin();
        TreeTxn {
            txn,
            opts: &self.opts,
            root: &self.root,
        }
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
}

struct TreeTxn<'a> {
    txn: PageTxn,
    opts: &'a Options,
    root: &'a AtomicId,
}

impl<'a> TreeTxn<'a> {
    async fn get(&self, key: &Key<'_>) -> Result<Option<&[u8]>> {
        let (view, _) = self.find_leaf(key).await?;
        self.lookup_value(key, &view).await
    }

    async fn write(&self, entry: &Entry<'_>) -> Result<()> {
        let (mut view, _) = self.find_leaf(&entry.key).await?;
        // let builder = SortedPageBuilder::new(PageTier::Leaf, PageKind::Data);
        let (new_addr, mut new_page) = self.txn.alloc_page(0)?;
        // builder.build(&mut new_page);
        loop {
            new_page.set_epoch(view.page.epoch());
            new_page.set_chain_len(view.page.chain_len());
            new_page.set_chain_next(view.addr.into());
            match self.txn.update_page(view.id, view.addr, new_addr) {
                Ok(_) => {
                    if new_page.chain_len() as usize >= self.opts.page_chain_length {
                        view.page = new_page.into();
                        // It doesn't matter whether this consolidation succeeds or not.
                        let _ = self.consolidate_page(view).await;
                        return Ok(());
                    }
                }
                Err(Error::UpdatePage(addr)) => {
                    let page = self.txn.read_page(addr).await?;
                    if page.epoch() == view.page.epoch() {
                        view.page = page;
                        continue;
                    }
                    return Err(Error::Again);
                }
                Err(err) => return Err(err),
            }
        }
    }

    async fn find_leaf(&self, key: &Key<'_>) -> Result<(PageView<'_>, Option<PageView<'_>>)> {
        let root = self.root.get();
        let mut index = PageIndex::new(root);
        let mut range = Range::default();
        let mut parent = None;
        loop {
            let addr = self.txn.page_addr(index.id);
            let page = self.txn.read_page(addr).await?;
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
            let (child_index, child_range) = self.lookup_child(key, &view).await?;
            index = child_index;
            range = child_range;
            parent = Some(view);
        }
    }

    async fn walk_page<F>(&self, view: &PageView<'_>) -> Result<()>
    where
        F: FnMut(PageRef<'_>),
    {
        Ok(())
    }

    async fn lookup_value(&self, key: &Key<'_>, view: &PageView<'_>) -> Result<Option<&[u8]>> {
        todo!()
    }

    async fn lookup_child(&self, key: &Key<'_>, view: &PageView<'_>) -> Result<(PageIndex, Range)> {
        todo!()
    }

    async fn reconcile_page(&self, view: PageView<'_>, parent: Option<PageView<'_>>) {
        todo!()
    }

    async fn consolidate_page(&self, view: PageView<'_>) -> Result<()> {
        let (iter, last_page) = self.delta_page_iter(&view).await;
        // let builder = DataPageBuilder::new(view.page.tier()).with_iter(iter);
        let (new_addr, mut new_page) = self.txn.alloc_page(0)?;
        // builder.build(&mut new_page);
        new_page.set_epoch(view.page.epoch().next());
        new_page.set_chain_len(last_page.chain_len());
        new_page.set_chain_next(last_page.chain_next());

        // self.txn.update_page(view.id, view.addr, addr)?;

        todo!()
    }

    async fn delta_page_iter(&self, view: &PageView<'_>) -> ((), PageRef<'_>) {
        todo!()
    }
}

struct AtomicId(AtomicU64);

impl AtomicId {
    fn new(id: u64) -> Self {
        Self(AtomicU64::new(id))
    }

    fn get(&self) -> u64 {
        self.0.load(Ordering::Acquire)
    }

    fn set(&self, id: u64) {
        self.0.store(id, Ordering::Release)
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
