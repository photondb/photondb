use super::{
    node::*,
    page::*,
    pagecache::{PageAddr, PageCache, PageView},
    pagestore::PageStore,
    pagetable::PageTable,
    Error, Ghost, Options, Result,
};

const ROOT_ID: u64 = 0;
const ROOT_INDEX: Index = Index::with_id(ROOT_ID);

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
        tree.init()
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
                Err(Error::Again) => continue,
                other => return other,
            }
        }
    }

    async fn try_get<'g>(&self, key: Key<'_>, ghost: &'g Ghost) -> Result<Option<&'g [u8]>> {
        let node = self.try_find_node(key.raw, ghost).await?;
        self.lookup_value(&node, key).await
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
        let mut page = DataPageBuilder::default().build_from_iter(&self.cache, &mut iter)?;
        loop {
            match self.try_update(key.raw, page.as_ptr(), ghost).await {
                Ok(_) => return Ok(()),
                Err(Error::Again) => continue,
                Err(err) => {
                    unsafe {
                        self.cache.dealloc(page.as_ptr());
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
            delta.set_rank(node.view.rank() + 1);
            delta.set_next(node.view.as_addr().into());
            match self.table.cas(node.id, delta.next(), delta.into()) {
                Ok(_) => {
                    if delta.rank() >= self.opts.data_delta_length {
                        node.view = delta.into();
                        let _ = self.try_consolidate_data_node(&node, ghost).await;
                    }
                    return Ok(());
                }
                Err(addr) => {
                    if let Some(view) = self.page_view(addr.into()) {
                        if view.ver() == node.view.ver() {
                            node.view = view;
                            continue;
                        }
                    }
                    return Err(Error::Again);
                }
            }
        }
    }
}

impl BTree {
    fn init(self) -> Result<Self> {
        let ghost = Ghost::pin();
        // Initializes the tree as root -> leaf.
        let root_id = self.table.alloc(ghost.guard()).unwrap();
        let leaf_id = self.table.alloc(ghost.guard()).unwrap();
        let mut leaf_page = DataPageBuilder::default().build(&self.cache)?;
        self.table.set(leaf_id, leaf_page.as_ptr().into());
        let mut root_iter = OptionIter::from(([].as_slice(), Index::with_id(leaf_id)));
        let mut root_page =
            IndexPageBuilder::default().build_from_iter(&self.cache, &mut root_iter)?;
        self.table.set(root_id, root_page.as_ptr().into());
        Ok(self)
    }

    fn node<'g>(&self, id: u64, _: &'g Ghost) -> Node<'g> {
        let addr = self.page_addr(id);
        // Our access pattern ensures that the address must be valid.
        let view = self.page_view(addr).unwrap();
        Node::new(id, view)
    }

    fn page_addr(&self, id: u64) -> PageAddr {
        self.table.get(id).into()
    }

    fn page_view(&self, addr: PageAddr) -> Option<PageView> {
        match addr {
            PageAddr::Mem(addr) => {
                let page = unsafe { PagePtr::new(addr as *mut u8) };
                page.map(PageView::from)
            }
            PageAddr::Disk(addr) => self
                .store
                .page_info(addr)
                .map(|info| PageView::Disk(info, addr)),
        }
    }

    fn dealloc_chain<'g>(&self, mut addr: PageAddr, ghost: &'g Ghost) {
        let cache = self.cache.clone();
        ghost.guard().defer(move || unsafe {
            while let PageAddr::Mem(ptr) = addr {
                if let Some(page) = PagePtr::new(ptr as *mut u8) {
                    addr = page.next().into();
                    cache.dealloc(page);
                } else {
                    break;
                }
            }
        });
    }

    async fn load_page_with_addr(&self, _: u64, addr: PageAddr) -> Result<Option<PagePtr>> {
        match addr {
            PageAddr::Mem(addr) => {
                let page = unsafe { PagePtr::new(addr as *mut u8) };
                Ok(page)
            }
            PageAddr::Disk(_) => {
                // self.swapin_page(id, addr).await,
                todo!()
            }
        }
    }

    async fn load_page_with_view(&self, _: u64, view: &PageView) -> Result<PagePtr> {
        match *view {
            PageView::Mem(page) => Ok(page),
            PageView::Disk(_, _) => {
                // self.swapin_page(id, addr).await,
                todo!()
            }
        }
    }

    async fn walk_node<'g, F>(&self, node: &Node<'g>, mut f: F) -> Result<()>
    where
        F: FnMut(PagePtr) -> bool,
    {
        let mut page = self.load_page_with_view(node.id, &node.view).await?;
        loop {
            if f(page) {
                break;
            }
            let next = page.next().into();
            match self.load_page_with_addr(node.id, next).await? {
                Some(next) => page = next,
                None => break,
            }
        }
        Ok(())
    }

    async fn iter_node<'g, N>(&self, node: &Node<'g>) -> Result<N::NodeIter>
    where
        N: NodeKind,
    {
        let mut merger = MergingIterBuilder::default();
        self.walk_node(node, |page| {
            if page.kind() == PageKind::Delta {
                merger.add(N::PageIter::from(page));
            }
            false
        })
        .await?;
        Ok(merger.build().into())
    }

    async fn lookup_value<'g>(&self, node: &Node<'g>, key: Key<'_>) -> Result<Option<&'g [u8]>> {
        let mut value = None;
        self.walk_node(node, |page| {
            if page.kind() == PageKind::Delta {
                let page = DataPageRef::from(page);
                if let Some((_, v)) = page.find(key) {
                    value = v.into();
                    return true;
                }
            }
            false
        })
        .await?;
        Ok(value)
    }

    async fn lookup_index<'g>(&self, node: &Node<'g>, key: &[u8]) -> Result<Option<Index>> {
        let mut value = None;
        self.walk_node(node, |page| {
            if page.kind() == PageKind::Delta {
                let page = IndexPageRef::from(page);
                if let Some((_, v)) = page.find(key) {
                    value = v.into();
                    return true;
                }
            }
            false
        })
        .await?;
        Ok(value)
    }

    async fn try_find_node<'g>(&self, key: &[u8], ghost: &'g Ghost) -> Result<Node<'g>> {
        let mut cursor = ROOT_INDEX;
        let mut parent = None;
        loop {
            let node = self.node(cursor.id, ghost);
            if node.view.ver() != cursor.ver {
                self.try_reconcile_node(&node, parent.as_ref(), ghost)?;
                return Err(Error::Again);
            }
            if node.view.is_data() {
                return Ok(node);
            }
            cursor = self.lookup_index(&node, key).await?.unwrap();
            parent = Some(node);
        }
    }

    fn try_replace_node<'g>(&self, node: &Node<'g>, page: PagePtr, ghost: &'g Ghost) -> Result<()> {
        let addr = node.view.as_addr();
        match self.table.cas(node.id, addr.into(), page.into()) {
            Ok(_) => {
                self.dealloc_chain(addr, ghost);
                Ok(())
            }
            Err(_) => {
                unsafe { self.cache.dealloc(page) };
                Err(Error::Again)
            }
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

    fn try_install_split<'g>(
        &self,
        left_id: u64,
        left_ptr: PagePtr,
        split_key: &[u8],
        right_ptr: PagePtr,
        ghost: &'g Ghost,
    ) -> Result<()> {
        let right_id = self.table.alloc(ghost.guard()).ok_or(Error::Alloc)?;
        self.table.set(right_id, right_ptr.into());
        let split = || -> Result<()> {
            let mut split_iter = OptionIter::from((split_key, Index::with_id(right_id)));
            let mut split_page = IndexPageBuilder::new(PageKind::Split, left_ptr.is_data())
                .build_from_iter(&self.cache, &mut split_iter)
                .map_err(|err| {
                    unsafe { self.cache.dealloc(right_ptr) };
                    err
                })?;
            split_page.set_ver(left_ptr.ver() + 1);
            split_page.set_rank(left_ptr.rank());
            split_page.set_next(left_ptr.into());
            self.table
                .cas(left_id, left_ptr.into(), split_page.as_ptr().into())
                .map(|_| ())
                .map_err(|_| {
                    unsafe {
                        self.cache.dealloc(right_ptr);
                        self.cache.dealloc(split_page.as_ptr());
                    }
                    Error::Again
                })
        };
        split().map_err(|err| {
            self.table.dealloc(right_id, ghost.guard());
            err
        })
    }

    fn try_split_data_node<'g>(
        &self,
        id: u64,
        page: DataPageRef<'g>,
        ghost: &'g Ghost,
    ) -> Result<()> {
        if let Some((sep, mut iter)) = page.split() {
            let right_page = DataPageBuilder::default().build_from_iter(&self.cache, &mut iter)?;
            self.try_install_split(id, page.as_ptr(), sep.raw, right_page.as_ptr(), ghost)?;
        }
        Ok(())
    }

    async fn try_consolidate_data_node<'g>(&self, node: &Node<'g>, ghost: &'g Ghost) -> Result<()> {
        let mut iter = self.iter_node::<DataNode>(node).await?;
        let mut page = DataPageBuilder::default().build_from_iter(&self.cache, &mut iter)?;
        page.set_ver(node.view.ver());
        self.try_replace_node(node, page.as_ptr(), ghost)?;
        // let _ = self.try_split_data_node(node.id, page.as_ref(), ghost);
        Ok(())
    }

    async fn try_consolidate_index_node<'g>(
        &self,
        node: &Node<'g>,
        ghost: &'g Ghost,
    ) -> Result<()> {
        let mut iter = self.iter_node::<IndexNode>(node).await?;
        let mut page = IndexPageBuilder::default().build_from_iter(&self.cache, &mut iter)?;
        page.set_ver(node.view.ver());
        self.try_replace_node(node, page.as_ptr(), ghost)?;
        Ok(())
    }
}
