use super::{
    page::*,
    pagecache::{PageAddr, PageCache, PageView},
    pagestore::PageStore,
    pagetable::PageTable,
    Error, Ghost, Options, Result,
};

const ROOT_ID: u64 = 0;
const ROOT_INDEX: Index = Index::new(ROOT_ID, 0);

struct Node<'a> {
    id: u64,
    view: PageView<'a>,
}

type NodeIter<'a, K, V> = MergingIter<DataPageIter<'a, K, V>>;

pub struct BTree {
    opts: Options,
    table: PageTable,
    cache: PageCache,
    store: PageStore,
}

impl BTree {
    pub async fn open(opts: Options) -> Result<Self> {
        let table = PageTable::default();
        let cache = PageCache::default();
        let store = PageStore::open(opts.clone()).await?;
        let tree = Self {
            opts,
            table,
            cache,
            store,
        };
        tree.init()?;
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
        let node = self.try_find_node(key.raw, ghost).await?;
        self.lookup_data(&key, &node, ghost).await
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
        let mut iter = OptionIter::from((key, value));
        let page = DataPageBuilder::new(true).build_from_iter(&self.cache, &mut iter)?;
        loop {
            match self.try_update(key.raw, page, ghost).await {
                Ok(_) => return Ok(()),
                Err(Error::Conflict) => continue,
                Err(err) => {
                    unsafe {
                        self.cache.dealloc(page);
                    }
                    return Err(err);
                }
            }
        }
    }

    async fn try_update<'g>(&self, key: &[u8], mut delta: PagePtr, ghost: &'g Ghost) -> Result<()> {
        let mut node = self.try_find_node(key, ghost).await?;
        loop {
            delta.set_ver(node.view.ver());
            delta.set_len(node.view.len() + 1);
            delta.set_next(node.view.as_addr().into());
            match self.table.cas(node.id, delta.next(), delta.into()) {
                None => {
                    if delta.len() >= self.opts.data_delta_length {
                        let _ = self.try_consolidate_node::<Key, Value>(&node, ghost).await;
                    }
                    return Ok(());
                }
                Some(addr) => {
                    if let Some(view) = self.page_view(addr.into(), ghost) {
                        if view.ver() == node.view.ver() {
                            node.view = view;
                            continue;
                        }
                    }
                    return Err(Error::Conflict);
                }
            }
        }
    }
}

impl BTree {
    fn init(&self) -> Result<()> {
        let ghost = Ghost::pin();
        // Initializes the tree as root -> leaf.
        let root_id = self.table.alloc(ghost.guard()).unwrap();
        assert_eq!(root_id, ROOT_ID);
        let leaf_id = self.table.alloc(ghost.guard()).unwrap();
        let leaf_page = DataPageBuilder::new(true).build(&self.cache)?;
        let leaf_index = Index::new(leaf_id, 0);
        let mut root_iter = OptionIter::from(([].as_slice(), leaf_index));
        let root_page = DataPageBuilder::new(false).build_from_iter(&self.cache, &mut root_iter)?;
        self.table.set(leaf_id, leaf_page.into());
        self.table.set(root_id, root_page.into());
        Ok(())
    }

    fn node<'g>(&self, id: u64, ghost: &'g Ghost) -> Node<'g> {
        let addr = self.page_addr(id);
        // Our access pattern ensures that the address is valid.
        let view = self.page_view(addr, ghost).unwrap();
        Node { id, view }
    }

    fn page_addr(&self, id: u64) -> PageAddr {
        self.table.get(id).into()
    }

    fn page_view<'g>(&self, addr: PageAddr, _: &'g Ghost) -> Option<PageView<'g>> {
        match addr {
            PageAddr::Mem(addr) => {
                let page = unsafe { PageRef::new(addr as *const u8) };
                page.map(PageView::Mem)
            }
            PageAddr::Disk(addr) => self
                .store
                .page_info(addr)
                .map(|info| PageView::Disk(info, addr)),
        }
    }

    async fn swapin_page<'a>(&self, _id: u64, addr: u64, _: &'a Ghost) -> Result<PageRef<'a>> {
        // TODO: adds the page to the chain.
        let page = self.store.load_page(addr).await?;
        let page: PageRef<'a> = page.into();
        Ok(page)
    }

    async fn load_page_with_view<'a>(
        &self,
        id: u64,
        view: &PageView<'a>,
        ghost: &'a Ghost,
    ) -> Result<PageRef<'a>> {
        match *view {
            PageView::Mem(page) => Ok(page),
            PageView::Disk(_, addr) => self.swapin_page(id, addr, ghost).await,
        }
    }

    async fn load_page_with_addr<'a>(
        &self,
        id: u64,
        addr: PageAddr,
        ghost: &'a Ghost,
    ) -> Result<Option<PageRef<'a>>> {
        match addr {
            PageAddr::Mem(addr) => {
                let page = unsafe { PageRef::new(addr as *const u8) };
                Ok(page)
            }
            PageAddr::Disk(addr) => {
                let page = self.swapin_page(id, addr, ghost).await?;
                Ok(Some(page))
            }
        }
    }

    async fn walk_node<'g, F>(&self, node: &Node<'g>, ghost: &'g Ghost, mut f: F) -> Result<()>
    where
        F: FnMut(PageRef<'g>),
    {
        let mut page = self.load_page_with_view(node.id, &node.view, ghost).await?;
        loop {
            f(page);
            let next = page.next().into();
            match self.load_page_with_addr(node.id, next, ghost).await? {
                Some(next) => page = next,
                None => break,
            }
        }
        Ok(())
    }

    async fn iter_node<'g, K, V>(
        &self,
        node: &Node<'g>,
        ghost: &'g Ghost,
    ) -> Result<NodeIter<'g, K, V>>
    where
        K: Decodable + Ord,
        V: Decodable,
    {
        let mut merger = MergingIterBuilder::default();
        self.walk_node(node, ghost, |page| {
            let page = unsafe { TypedPageRef::cast(page) };
            if let TypedPageRef::Data(data) = page {
                merger.add(data.into_iter());
            }
        })
        .await?;
        Ok(merger.build())
    }

    async fn lookup_data<'g>(
        &self,
        key: &Key<'_>,
        node: &Node<'g>,
        ghost: &'g Ghost,
    ) -> Result<Option<&'g [u8]>> {
        self.walk_node(node, ghost, |_| {}).await?;
        Ok(None)
    }

    async fn lookup_index<'g>(
        &self,
        key: &[u8],
        node: &Node<'g>,
        ghost: &'g Ghost,
    ) -> Result<Option<Index>> {
        self.walk_node(node, ghost, |_| {}).await?;
        Ok(None)
    }

    async fn try_find_node<'g>(&self, key: &[u8], ghost: &'g Ghost) -> Result<Node<'g>> {
        let mut cursor = ROOT_INDEX;
        let mut parent = None;
        loop {
            let node = self.node(cursor.id, ghost);
            if node.view.ver() != cursor.ver {
                self.try_reconcile_node(&node, parent.as_ref(), ghost)?;
                return Err(Error::Conflict);
            }
            if node.view.is_leaf() {
                return Ok(node);
            }
            cursor = self.lookup_index(key, &node, ghost).await?.unwrap();
            parent = Some(node);
        }
    }

    fn try_reconcile_node<'g>(
        &self,
        node: &Node<'g>,
        parent: Option<&Node<'g>>,
        ghost: &'g Ghost,
    ) -> Result<()> {
        todo!()
    }

    async fn try_consolidate_node<'g, K, V>(&self, node: &Node<'g>, ghost: &'g Ghost) -> Result<()>
    where
        K: Encodable + Decodable + Ord,
        V: Encodable + Decodable,
    {
        let mut iter = self.iter_node::<K, V>(node, ghost).await?;
        let page =
            DataPageBuilder::new(node.view.is_leaf()).build_from_iter(&self.cache, &mut iter)?;
        if self
            .table
            .cas(node.id, node.view.as_addr().into(), page.into())
            .is_some()
        {
            return Err(Error::Conflict);
        }
        Ok(())
    }
}
