use super::{
    page::*,
    pagecache::{PageAddr, PageCache, PageView},
    pagestore::PageStore,
    pagetable::PageTable,
    Error, Ghost, Options, Result,
};

const ROOT_ID: u64 = 0;
const ROOT_INDEX: Index = Index::new(ROOT_ID, 0);

struct Node<'a>(u64, PageView<'a>);

pub struct BTree {
    opts: Options,
    table: PageTable,
    cache: PageCache,
    store: PageStore,
}

impl BTree {
    pub async fn open(opts: Options) -> Result<Self> {
        let table = PageTable::default();
        let cache = PageCache::with_limit(opts.cache_size);
        let store = PageStore::open(opts.clone()).await?;
        let tree = Self {
            opts,
            table,
            cache,
            store,
        };
        tree.init();
        Ok(tree)
    }

    pub async fn get<'g>(
        &self,
        key: &[u8],
        lsn: u64,
        ghost: &'g Ghost,
    ) -> Result<Option<&'g [u8]>> {
        let key = Key::new(key, lsn);
        loop {
            match self.try_get(key, ghost).await {
                Err(Error::Conflict) => continue,
                other => return other,
            }
        }
    }

    async fn try_get<'g>(&self, key: Key<'_>, ghost: &'g Ghost) -> Result<Option<&'g [u8]>> {
        let node = self.try_find_data_node(key.raw, ghost).await?;
        self.search_node(node.id, &node.view, key, ghost).await
    }

    pub async fn put<'g>(
        &self,
        key: &[u8],
        lsn: u64,
        value: &[u8],
        ghost: &'g Ghost,
    ) -> Result<()> {
        let key = Key::new(key, lsn);
        let value = Value::Put(value);
        self.update(key, value, ghost).await
    }

    pub async fn delete<'g>(&self, key: &[u8], lsn: u64, ghost: &'g Ghost) -> Result<()> {
        let key = Key::new(key, lsn);
        let value = Value::Delete;
        self.update(key, value, ghost).await
    }

    async fn update<'g>(&self, key: Key<'_>, value: Value<'_>, ghost: &'g Ghost) -> Result<()> {
        let mut iter = SingleIter::from((key, value));
        let mut page = DataPageBuilder::default()
            .build_from_iter(&mut iter, &self.cache)
            .unwrap();
        loop {
            match self.try_update(key.raw, &mut page, ghost).await {
                Ok(_) => return Ok(()),
                Err(Error::Conflict) => continue,
                Err(err) => {
                    unsafe {
                        self.cache.dealloc_page(page.into());
                    }
                    return Err(err);
                }
            }
        }
    }

    async fn try_update<'g>(
        &self,
        key: &[u8],
        delta: &mut PageBuf,
        ghost: &'g Ghost,
    ) -> Result<()> {
        let Node(id, mut view) = self.try_find_data_node(key, ghost).await?;
        loop {
            let old_addr = view.as_addr();
            delta.set_ver(view.ver());
            delta.set_len(view.len() + 1);
            delta.set_next(old_addr);
            match self.update_node(id, old_addr, delta.as_addr()) {
                None => {
                    if delta.len() >= self.opts.data_delta_length {
                        let _ = self.try_consolidate_node(&node, ghost).await;
                    }
                    return Ok(());
                }
                Some(addr) => {
                    let new_view = self.page_view(addr, ghost);
                    if new_view.ver() != view.ver() {
                        return Err(Error::Conflict);
                    }
                    view = new_view;
                }
            }
        }
    }
}

impl BTree {
    fn init(&self) {
        let ghost = Ghost::pin();
        let root_id = self.table.alloc(ghost.guard()).unwrap();
        assert_eq!(root_id, ROOT_ID);
        let data_id = self.table.alloc(ghost.guard()).unwrap();
        let data_page = DataPageBuilder::default().build(&self.cache).unwrap();
        self.update_node(data_id, PagePtr::null(), data_page.as_ptr())
            .unwrap();
        let data_index = Index::new(data_id, 0);
        let mut iter = SingleIter::from(([].as_slice(), data_index));
        let root_page = IndexPageBuilder::default()
            .build_from_iter(&mut iter, &self.cache)
            .unwrap();
        self.update_node(root_id, PagePtr::null(), root_page.as_ptr())
            .unwrap();
    }

    pub fn page_addr(&self, id: u64) -> PageAddr {
        self.table.get(id).into()
    }

    pub fn page_view(&self, addr: PageAddr) -> Option<PageView> {
        match addr {
            PageAddr::Mem(addr) => unsafe {
                PageRef::from_raw(addr as *const u8).map(PageView::Mem)
            },
            PageAddr::Disk(addr) => self
                .store
                .page_info(addr)
                .map(|info| PageView::Disk(info, addr)),
        }
    }

    pub async fn swapin_page<'a>(
        &self,
        id: u64,
        addr: u64,
        ghost: &'a Ghost,
    ) -> Result<PageRef<'a>> {
        let page = self.store.load_page(addr).await?;
        Ok(page.into())
    }

    pub async fn load_page_with_view<'a>(
        &self,
        id: u64,
        view: PageView<'a>,
        ghost: &'a Ghost,
    ) -> Result<PageRef<'a>> {
        match view {
            PageView::Mem(page) => Ok(page),
            PageView::Disk(_, addr) => self.swapin_page(id, addr, ghost).await,
        }
    }

    pub async fn load_page_with_addr<'a>(
        &self,
        id: u64,
        addr: PageAddr,
        ghost: &'a Ghost,
    ) -> Result<Option<PageRef<'a>>> {
        match addr {
            PageAddr::Mem(addr) => unsafe { Ok(PageRef::from_raw(addr as *const u8)) },
            PageAddr::Disk(addr) => self.swapin_page(id, addr, ghost).await.map(Some),
        }
    }

    fn node<'g>(&self, id: u64, ghost: &'g Ghost) -> Node<'g> {
        let addr = self.page_addr(id);
        let view = self.page_view(addr, ghost);
        Node { id, view }
    }

    fn update_node(&self, id: NodeId, old: PagePtr, new: PagePtr) -> Option<PagePtr> {
        self.table
            .cas(id, old.into(), new.into())
            .map(|now| now.into())
    }

    async fn try_find_data_node<'g>(&self, key: &[u8], ghost: &'g Ghost) -> Result<Node<'g>> {
        let mut cursor = ROOT_INDEX;
        let mut parent = None;
        loop {
            let node = self.node(cursor.id, ghost);
            if node.view.ver() != cursor.ver {
                self.try_reconcile_node(&node, parent.as_ref(), ghost)?;
                return Err(Error::Conflict);
            }
            match node.view.tag() {
                PageTag::Leaf(_) => return Ok(node),
                PageTag::Internal(_) => {
                    cursor = self.search_index_node(&node, key, ghost).await?;
                    parent = Some(node);
                }
            }
        }
    }

    async fn iter_node<'g>(&self, node: &Node<'g>, ghost: &'g Ghost) -> Result<DataNodeIter<'g>> {
        let mut page = self.load_page_with_view(node.id, &node.view, ghost).await?;
        let mut merger = MergingIterBuilder::default();
        loop {
            let addr = page.chain_next().into();
            match self.load_page_with_addr(node.id, addr, ghost).await? {
                Some(next) => page = next,
                None => break,
            }
        }
        Ok(merger.build())
    }

    async fn search_node<'g, T>(
        &self,
        id: u64,
        view: &PageView<'g>,
        target: T,
        ghost: &'g Ghost,
    ) -> Result<Option<&'g [u8]>> {
        let mut page = self.load_page_with_view(id, view, ghost).await?;
        loop {
            let next = page.next();
            if let Some(page) = self.load_page_with_addr(id, next, ghost).await? {
                page.search(key.raw, ghost).await
            } else {
                return Ok(None);
            }
        }
    }

    async fn try_reconcile_node<'g>(
        &self,
        node: &Node<'g>,
        parent: Option<&Node<'g>>,
        ghost: &'g Ghost,
    ) -> Result<()> {
        todo!()
    }

    async fn try_consolidate_node<'g>(&self, node: &Node<'g>, ghost: &'g Ghost) -> Result<()> {
        let mut iter = self.iter_node(node, ghost).await?;
        let page = SortedPageBuilder::default()
            .build_from_iter(&mut iter, &self.cache)
            .unwrap();
        if self
            .update_node(node.id, node.view.as_ptr(), page.as_ptr())
            .is_some()
        {
            return Err(Error::Conflict);
        }
        Ok(())
    }
}
