use crate::{
    page::{DataPageBuilder, Entry, Index, Key, PageBuf, PageRef, PageTier, Range},
    page_table::PageTable,
    Error, Options, Result,
};

struct PageStore;

impl PageStore {
    fn begin(&self) -> Txn {
        todo!()
    }
}

struct Txn;

impl Txn {
    fn borrow_mut(&self) -> TxnMut<'_> {
        TxnMut { txn: self }
    }

    fn read_page(&self, addr: u64) -> PageRef<'_> {
        todo!()
    }
}

struct TxnMut<'a> {
    txn: &'a Txn,
}

impl<'a> TxnMut<'a> {
    fn alloc_page(&mut self, size: usize) -> PageBuf<'a> {
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
                Err(Error::Conflicted) => continue,
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
        let mut txn_mut = txn.borrow_mut();
        let builder = DataPageBuilder::new(PageTier::Leaf).with_item(&entry);
        let mut page = txn_mut.alloc_page(builder.size());
        builder.build(&mut page);
        loop {
            match self.try_write(&txn, &entry.key, &mut page).await {
                Ok(()) => return Ok(()),
                Err(Error::Conflicted) => continue,
                Err(e) => return Err(e),
            }
        }
    }

    async fn try_write<'t>(
        &self,
        txn: &'t Txn,
        key: &Key<'_>,
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
            let page = txn.read_page(addr);
            let view = PageView {
                id: index.id,
                addr,
                page,
                range,
            };

            // Do not continue if the page epoch has changed.
            if view.page.epoch() != index.epoch {
                self.reconcile_page(txn, view, parent).await;
                return Err(Error::Conflicted);
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

    async fn consolidate_page<'t>(&self, txn: &mut TxnMut<'t>, view: &PageView<'t>) {
        let iter = self.consolidate_page_iter(txn, view).await;
        let builder = DataPageBuilder::new(view.page.tier()).with_iter(iter);
        let mut page = txn.alloc_page(builder.size());
        // builder.build(&mut page);
        page.set_epoch(view.page.epoch().next());
        // page.set_chain_len(last_page.chain_len());
        // page.set_chain_next(last_page.next());

        todo!()
    }

    async fn consolidate_page_iter<'t>(&self, txn: &mut TxnMut<'t>, view: &PageView<'t>) {
        todo!()
    }
}

struct PageView<'a> {
    id: u64,
    addr: u64,
    page: PageRef<'a>,
    range: Range<'a>,
}
