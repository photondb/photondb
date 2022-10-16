use crate::{
    page::{DataPageBuilder, Entry, Key, PageBuf, PageEpoch, PageRef, PageTier, Range},
    page_store::{AtomicPageId, PageAddr, PageId, PageStore, PageTxn},
    Error, Options, Result,
};

pub(crate) struct Tree {
    opts: Options,
    root: AtomicPageId,
    store: PageStore,
}

impl Tree {
    pub(crate) async fn open(opts: Options) -> Result<Self> {
        let store = PageStore::open().await?;
        let txn = store.begin();
        let root = txn.alloc_id();
        txn.commit();
        Ok(Self {
            opts,
            root: root.into(),
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
                Ok(()) => return Ok(()),
                Err(Error::Again) => continue,
                Err(e) => return Err(e),
            }
        }
    }
}

struct TreeTxn<'a> {
    txn: PageTxn,
    opts: &'a Options,
    root: &'a AtomicPageId,
}

impl<'a> TreeTxn<'a> {
    async fn get(&self, key: &Key<'_>) -> Result<Option<&[u8]>> {
        let (view, _) = self.find_leaf(key).await?;
        self.lookup_value(key, &view).await
    }

    async fn write(&self, entry: &Entry<'_>) -> Result<()> {
        let builder = DataPageBuilder::new(PageTier::Leaf).with_item(&entry);
        let (addr, mut page) = self.txn.alloc_page(builder.size())?;
        builder.build(&mut page);
        let (mut view, _) = self.find_leaf(&entry.key).await?;
        loop {
            page.set_epoch(view.page.epoch());
            page.set_chain_len(view.page.chain_len());
            page.set_chain_next(view.addr.into());
            match self.txn.update_page(view.id, view.addr, addr) {
                Ok(_) => {
                    if page.chain_len() as usize >= self.opts.page_chain_length {
                        view.page = page.into();
                        // It doesn't matter whether this consolidation succeeds or not.
                        let _ = self.consolidate_page(view).await;
                        return Ok(());
                    }
                }
                Err(new_addr) => {
                    if new_addr < addr {
                        let new_page = self.txn.read_page(new_addr).await?;
                        if new_page.epoch() == view.page.epoch() {
                            view.page = new_page;
                            continue;
                        }
                    }
                    return Err(Error::Again);
                }
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
            self.lookup_index(key, &view).await?;
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

    async fn lookup_index(&self, key: &Key<'_>, view: &PageView<'_>) -> Result<()> {
        todo!()
    }

    async fn reconcile_page(&self, view: PageView<'_>, parent: Option<PageView<'_>>) {
        todo!()
    }

    async fn consolidate_page(&self, view: PageView<'_>) -> Result<()> {
        let iter = self.consolidate_page_iter(&view).await;
        let builder = DataPageBuilder::new(view.page.tier()).with_iter(iter);
        let (addr, mut page) = self.txn.alloc_page(builder.size())?;
        // builder.build(&mut page);
        page.set_epoch(view.page.epoch().next());
        // page.set_chain_len(last_page.chain_len());
        // page.set_chain_next(last_page.next());

        // self.txn.update_page(view.id, view.addr, addr)?;

        todo!()
    }

    async fn consolidate_page_iter(&self, view: &PageView<'_>) {
        todo!()
    }
}

struct PageView<'a> {
    id: PageId,
    addr: PageAddr,
    page: PageRef<'a>,
    range: Range<'a>,
}

#[derive(Copy, Clone, Debug)]
struct PageIndex {
    id: PageId,
    epoch: PageEpoch,
}

impl PageIndex {
    fn new(id: PageId) -> Self {
        Self {
            id,
            epoch: PageEpoch::default(),
        }
    }
}
