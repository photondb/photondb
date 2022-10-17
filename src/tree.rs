use std::path::Path;

use crate::{
    data::{Entry, Key, Range},
    env::Env,
    page::{PageBuf, PageEpoch, PageKind, PageRef, PageTier, SortedPageBuilder},
    page_store::{Error, Guard, PageStore, PageTxn, Result},
    Options,
};

pub(crate) struct Tree<E> {
    options: Options,
    store: PageStore<E>,
}

impl<E: Env> Tree<E> {
    pub(crate) async fn open<P: AsRef<Path>>(env: E, path: P, options: Options) -> Result<Self> {
        let store = PageStore::open(env, path).await?;
        Ok(Self { options, store })
    }

    fn begin(&self) -> TreeTxn {
        let guard = self.store.guard();
        TreeTxn::new(&self.options, guard)
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
    options: &'a Options,
    guard: Guard,
}

impl<'a> TreeTxn<'a> {
    fn new(options: &'a Options, guard: Guard) -> Self {
        Self { options, guard }
    }

    async fn get(&self, key: &Key<'_>) -> Result<Option<&[u8]>> {
        let mut txn = self.guard.begin();
        let (view, _) = self.find_leaf(&mut txn, key).await?;
        self.find_value(&mut txn, key, &view).await
    }

    async fn write(&self, entry: &Entry<'_>) -> Result<()> {
        let mut txn = self.guard.begin();
        let (mut view, _) = self.find_leaf(&mut txn, &entry.key).await?;
        // let builder = SortedPageBuilder::new(PageTier::Leaf, PageKind::Data);
        let (new_addr, mut new_page) = txn.alloc_page(0)?;
        // builder.build(&mut new_page);
        loop {
            new_page.set_epoch(view.page.epoch());
            new_page.set_chain_len(view.page.chain_len());
            new_page.set_chain_next(view.addr.into());
            match txn.update_page(view.id, view.addr, new_addr) {
                Ok(_) => {
                    if new_page.chain_len() as usize >= self.options.page_chain_length {
                        view.page = new_page.into();
                        // It doesn't matter whether this consolidation succeeds or not.
                        let _ = self.consolidate_page(&mut txn, view).await;
                        return Ok(());
                    }
                }
                Err(Error::UpdatePage(addr)) => {
                    let page = self.guard.read_page(addr).await?;
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

    async fn find_leaf<'s: 't, 't>(
        &'s self,
        txn: &mut PageTxn<'t>,
        key: &Key<'_>,
    ) -> Result<(PageView<'t>, Option<PageView<'t>>)> {
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
                self.reconcile_page(txn, view, parent).await;
                return Err(Error::Again);
            }
            if view.page.tier().is_leaf() {
                return Ok((view, parent));
            }
            let (child_index, child_range) = self.find_child(txn, key, &view).await?;
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

    async fn find_value<'t>(
        &self,
        txn: &mut PageTxn<'t>,
        key: &Key<'_>,
        view: &PageView<'t>,
    ) -> Result<Option<&[u8]>> {
        todo!()
    }

    async fn find_child<'t>(
        &self,
        txn: &mut PageTxn<'t>,
        key: &Key<'_>,
        view: &PageView<'t>,
    ) -> Result<(PageIndex, Range)> {
        todo!()
    }

    async fn reconcile_page<'t>(
        &self,
        txn: &mut PageTxn<'t>,
        view: PageView<'t>,
        parent: Option<PageView<'t>>,
    ) {
        todo!()
    }

    async fn consolidate_page<'t>(&self, txn: &mut PageTxn<'t>, view: PageView<'t>) -> Result<()> {
        let (iter, last_page) = self.delta_page_iter(&view).await;
        // let builder = DataPageBuilder::new(view.page.tier()).with_iter(iter);
        let (new_addr, mut new_page) = txn.alloc_page(0)?;
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
