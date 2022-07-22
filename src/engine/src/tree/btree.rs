use super::{
    node::*, page::*, pagecache::PageCache, pagestore::PageStore, pagetable::PageTable, Error,
    Ghost, Options, Result,
};

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
        let store = PageStore::open().await?;
        let tree = Self {
            opts,
            table,
            cache,
            store,
        };
        tree.init();
        // TODO: recovers the page table from the page store.
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
        self.search_data_node(&node, key, ghost).await
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
        let mut node = self.try_find_data_node(key, ghost).await?;
        loop {
            let next = node.view.as_ptr();
            delta.set_ver(node.view.ver());
            delta.set_len(node.view.len() + 1);
            delta.set_next(next);
            match self.update_node(node.id, next, delta.as_ptr()) {
                None => {
                    if delta.len() >= self.opts.data_delta_length {
                        let _ = self.try_consolidate_data_node(&node, ghost).await;
                    }
                    return Ok(());
                }
                Some(ptr) => {
                    let view = self.page_view(ptr, ghost);
                    if view.ver() != node.view.ver() {
                        return Err(Error::Conflict);
                    }
                    node.view = view;
                }
            }
        }
    }
}

const ROOT_ID: u64 = 0;
const ROOT_INDEX: Index = Index {
    id: ROOT_ID,
    ver: 0,
};

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

    async fn recover(&self) -> Result<()> {
        Ok(())
    }

    fn page_ptr(&self, id: NodeId) -> PagePtr {
        self.table.get(id).into()
    }

    fn page_view<'g>(&self, ptr: PagePtr, _: &'g Ghost) -> PageView<'g> {
        match ptr {
            PagePtr::Mem(addr) => PageView::Mem(addr.into()),
            PagePtr::Disk(addr) => {
                let addr = addr.into();
                let info = self.store.page_info(addr).unwrap();
                PageView::Disk(addr, info)
            }
        }
    }

    fn node_pair<'g>(&self, id: NodeId, ghost: &'g Ghost) -> NodePair<'g> {
        let ptr = self.page_ptr(id);
        let view = self.page_view(ptr, ghost);
        NodePair { id, view }
    }

    fn update_node(&self, id: NodeId, old: PagePtr, new: PagePtr) -> Option<PagePtr> {
        self.table
            .cas(id, old.into(), new.into())
            .map(|now| now.into())
    }

    fn swapin_page<'g>(
        &self,
        id: NodeId,
        ptr: PagePtr,
        buf: PageBuf,
        ghost: &'g Ghost,
    ) -> Result<PageRef<'g>> {
        todo!()
    }

    async fn load_page_with_ptr<'g>(
        &self,
        id: NodeId,
        ptr: PagePtr,
        ghost: &'g Ghost,
    ) -> Result<PageRef<'g>> {
        match ptr {
            PagePtr::Mem(addr) => Ok(addr.into()),
            PagePtr::Disk(addr) => {
                let buf = self.store.load_page_with_addr(addr.into()).await?;
                self.swapin_page(id, ptr, buf, ghost)
            }
        }
    }

    async fn load_page_with_view<'g>(
        &self,
        id: NodeId,
        view: &PageView<'g>,
        ghost: &'g Ghost,
    ) -> Result<PageRef<'g>> {
        match *view {
            PageView::Mem(page) => Ok(page),
            PageView::Disk(addr, ref info) => {
                let ptr = PagePtr::Disk(addr.into());
                let buf = self.store.load_page_with_handle(&info.handle).await?;
                if buf.ver() != view.ver() {
                    return Err(Error::Conflict);
                }
                self.swapin_page(id, ptr, buf, ghost)
            }
        }
    }

    async fn try_find_data_node<'g>(&self, key: &[u8], ghost: &'g Ghost) -> Result<NodePair<'g>> {
        let mut cursor = ROOT_INDEX;
        let mut parent = None;
        loop {
            let node = self.node_pair(cursor.id, ghost);
            if node.view.ver() != cursor.ver {
                self.try_help_pending_smo(&node, parent, ghost)?;
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

    fn try_help_pending_smo<'g>(
        &self,
        node: &NodePair<'g>,
        parent: Option<NodePair<'g>>,
        ghost: &'g Ghost,
    ) -> Result<()> {
        todo!()
    }

    async fn iter_data_node<'g>(
        &self,
        node: &NodePair<'g>,
        ghost: &'g Ghost,
    ) -> Result<DataNodeIter<'g>> {
        let mut page = self.load_page_with_view(node.id, &node.view, ghost).await?;
        let mut merger = MergingIterBuilder::default();
        loop {
            let next = page.next();
            if !next.is_null() {
                page = self.load_page_with_ptr(node.id, next, ghost).await?;
            } else {
                return Ok(merger.build());
            }
        }
    }

    async fn search_data_node<'g>(
        &self,
        node: &NodePair<'g>,
        key: Key<'_>,
        ghost: &'g Ghost,
    ) -> Result<Option<&'g [u8]>> {
        let mut page = self.load_page_with_view(node.id, &node.view, ghost).await?;
        loop {
            let next = page.next();
            if !next.is_null() {
                page = self.load_page_with_ptr(node.id, next, ghost).await?;
            } else {
                return Ok(None);
            }
        }
    }

    async fn try_split_data_node<'g>(
        &self,
        id: NodeId,
        page: DataPageRef<'g>,
        ghost: &'g Ghost,
    ) -> Result<()> {
        // TODO
        Ok(())
    }

    async fn try_consolidate_data_node<'g>(
        &self,
        node: &NodePair<'g>,
        ghost: &'g Ghost,
    ) -> Result<()> {
        let mut iter = self.iter_data_node(node, ghost).await?;
        let page = DataPageBuilder::default()
            .build_from_iter(&mut iter, &self.cache)
            .unwrap();
        // TODO: builds data page
        if self
            .update_node(node.id, node.view.as_ptr(), page.as_ptr())
            .is_some()
        {
            return Err(Error::Conflict);
        }
        if page.size() >= self.opts.data_node_size {
            // self.try_split_data_node(node.id, page.as_ref(), ghost).await;
        }
        Ok(())
    }

    async fn iter_index_node<'g>(
        &self,
        node: &NodePair<'g>,
        ghost: &'g Ghost,
    ) -> Result<IndexNodeIter<'g>> {
        let mut page = self.load_page_with_view(node.id, &node.view, ghost).await?;
        let mut merger = MergingIterBuilder::default();
        loop {
            let next = page.next();
            if !next.is_null() {
                page = self.load_page_with_ptr(node.id, next, ghost).await?;
            } else {
                return Ok(merger.build());
            }
        }
    }

    async fn search_index_node<'g>(
        &self,
        node: &NodePair<'g>,
        target: &[u8],
        ghost: &'g Ghost,
    ) -> Result<Index> {
        let mut page = self.load_page_with_view(node.id, &node.view, ghost).await?;
        loop {
            let next = page.next();
            if !next.is_null() {
                page = self.load_page_with_ptr(node.id, next, ghost).await?;
            } else {
                todo!()
            }
        }
    }

    async fn try_split_index_node<'g>(
        &self,
        id: NodeId,
        page: IndexPageRef<'g>,
        ghost: &'g Ghost,
    ) -> Result<()> {
        // TODO
        Ok(())
    }

    async fn try_consolidate_index_node<'g>(
        &self,
        node: &NodePair<'g>,
        ghost: &'g Ghost,
    ) -> Result<()> {
        let mut iter = self.iter_index_node(node, ghost).await?;
        let page = IndexPageBuilder::default()
            .build_from_iter(&mut iter, &self.cache)
            .unwrap();
        if self
            .update_node(node.id, node.view.as_ptr(), page.as_ptr())
            .is_some()
        {
            return Err(Error::Conflict);
        }
        if page.size() >= self.opts.index_node_size {
            // self.try_split_index_node(node.id, page.as_ref(), ghost).await;
        }
        Ok(())
    }
}
