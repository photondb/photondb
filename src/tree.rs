use crate::{
    page::{DataPageBuilder, Entry, Index, Key, PageBuf, PageRef, PageTier, Range},
    page_table::PageTable,
    Error, Options, Result,
};

#[derive(Copy, Clone)]
struct PageAddr(u64);

impl From<u64> for PageAddr {
    fn from(addr: u64) -> Self {
        Self(addr)
    }
}

impl From<PageAddr> for u64 {
    fn from(addr: PageAddr) -> Self {
        addr.0
    }
}

struct PageStore;

impl PageStore {
    fn begin(&self) -> Txn {
        todo!()
    }
}

struct Txn;

impl Txn {
    fn read_page(&self, addr: PageAddr) -> Result<PageRef<'_>> {
        todo!()
    }

    fn alloc_page(&self, size: usize) -> Result<(PageAddr, PageBuf<'_>)> {
        todo!()
    }
}

pub(crate) struct Tree {
    table: PageTable,
    store: PageStore,
}

impl Tree {
    pub(crate) async fn open(options: Options) -> Result<Self> {
        todo!()
    }

    pub(crate) async fn get<F, R>(&self, key: Key<'_>, f: F) -> Result<R>
    where
        F: FnOnce(Option<&[u8]>) -> R,
    {
        let txn = self.store.begin();
        loop {
            match self.try_get(&txn, &key).await {
                Ok(value) => return Ok(f(value)),
                Err(Error::Again) => continue,
                Err(e) => return Err(e),
            }
        }
    }

    async fn try_get<'t>(&self, txn: &'t Txn, key: &Key<'_>) -> Result<Option<&[u8]>> {
        let (view, _) = self.find_leaf(txn, key).await?;
        self.lookup_value(txn, key, &view).await
    }

    pub(crate) async fn write(&self, entry: Entry<'_>) -> Result<()> {
        let txn = self.store.begin();
        let builder = DataPageBuilder::new(PageTier::Leaf).with_item(&entry);
        let (addr, mut page) = txn.alloc_page(builder.size())?;
        builder.build(&mut page);
        loop {
            match self.try_write(&txn, &entry.key, addr, &mut page).await {
                Ok(()) => return Ok(()),
                Err(Error::Again) => continue,
                Err(e) => return Err(e),
            }
        }
    }

    async fn try_write<'t>(
        &self,
        txn: &'t Txn,
        key: &Key<'_>,
        addr: PageAddr,
        page: &mut PageBuf<'t>,
    ) -> Result<()> {
        let (view, _) = self.find_leaf(txn, key).await?;
        loop {
            page.set_epoch(view.page.epoch());
            page.set_chain_len(view.page.chain_len());
            page.set_chain_next(view.addr);
        }
        todo!()
    }
}

impl Tree {
    async fn find_leaf<'t>(
        &self,
        txn: &'t Txn,
        key: &Key<'_>,
    ) -> Result<(PageView<'t>, Option<PageView<'t>>)> {
        let mut index = Index::default();
        let mut range = Range::default();
        let mut parent = None;

        loop {
            let addr = self.table.get(index.id);
            let page = txn.read_page(addr.into())?;
            let view = PageView {
                id: index.id,
                addr,
                page,
                range,
            };

            // Do not continue if the page epoch has changed.
            if view.page.epoch() != index.epoch {
                self.reconcile_page(txn, view, parent).await;
                return Err(Error::Again);
            }

            if view.page.tier().is_leaf() {
                return Ok((view, parent));
            }

            parent = Some(view);
        }
    }

    async fn walk_page<'t, F>(&self, txn: &'t Txn, view: &PageView<'t>) -> Result<()>
    where
        F: FnMut(PageRef<'t>),
    {
        Ok(())
    }

    async fn lookup_value<'t>(
        &self,
        txn: &'t Txn,
        key: &Key<'_>,
        view: &PageView<'t>,
    ) -> Result<Option<&[u8]>> {
        todo!()
    }

    async fn lookup_index<'t>(
        &self,
        txn: &'t Txn,
        key: &Key<'_>,
        view: &PageView<'t>,
    ) -> Result<()> {
        todo!()
    }

    async fn reconcile_page<'t>(
        &self,
        txn: &'t Txn,
        view: PageView<'t>,
        parent: Option<PageView<'t>>,
    ) {
        todo!()
    }

    async fn consolidate_page<'t>(&self, txn: &'t Txn, view: &PageView<'t>) -> Result<()> {
        let iter = self.consolidate_page_iter(txn, view).await;
        let builder = DataPageBuilder::new(view.page.tier()).with_iter(iter);
        let (addr, mut page) = txn.alloc_page(builder.size())?;
        // builder.build(&mut page);
        page.set_epoch(view.page.epoch().next());
        // page.set_chain_len(last_page.chain_len());
        // page.set_chain_next(last_page.next());

        todo!()
    }

    async fn consolidate_page_iter<'t>(&self, txn: &'t Txn, view: &PageView<'t>) {
        todo!()
    }
}

struct PageView<'a> {
    id: u64,
    addr: u64,
    page: PageRef<'a>,
    range: Range<'a>,
}
