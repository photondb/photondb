use super::{
    node::{DataNodeIter, IndexNodeIter, NodeId, NodeIndex, NodePair, PageView},
    page::{
        DataPageBuf, DataPageLayout, DataPageRef, DataRecord, DataValue, IndexPageBuf,
        IndexPageLayout, IndexPageRef, MergeIterBuilder, PageBuf, PageKind, PagePtr, PageRef,
    },
    pagealloc::PageAlloc,
    pagestore::PageStore,
    pagetable::PageTable,
    Error, Ghost, Options, Result,
};

pub struct Tree {
    opts: Options,
    alloc: PageAlloc,
    table: PageTable,
    store: PageStore,
}

impl Tree {
    pub async fn open(opts: Options) -> Result<Self> {
        let alloc = PageAlloc::with_limit(opts.cache_size);
        let table = PageTable::default();
        let store = PageStore::open(opts.clone()).await?;
        let tree = Self {
            opts,
            alloc,
            table,
            store,
        };
        tree.recover().await?;
        Ok(tree)
    }

    async fn recover(&self) -> Result<()> {
        // TODO: recovers the page table from the page store.
        Ok(())
    }

    pub async fn get<'g>(
        &self,
        key: &[u8],
        lsn: u64,
        ghost: &'g Ghost,
    ) -> Result<Option<&'g [u8]>> {
        loop {
            match self.try_get(key, lsn, ghost).await {
                Err(Error::Conflict) => continue,
                other => return other,
            }
        }
    }

    async fn try_get<'g>(
        &self,
        key: &[u8],
        lsn: u64,
        ghost: &'g Ghost,
    ) -> Result<Option<&'g [u8]>> {
        let node = self.try_find_data_node(key, ghost).await?;
        self.search_data_node(&node, key, lsn, ghost).await
    }

    pub async fn put<'g>(
        &self,
        key: &[u8],
        lsn: u64,
        value: &[u8],
        ghost: &'g Ghost,
    ) -> Result<()> {
        let record = DataRecord::put(key, lsn, value);
        self.update(&record, ghost).await
    }

    pub async fn delete<'g>(&self, key: &[u8], lsn: u64, ghost: &'g Ghost) -> Result<()> {
        let record = DataRecord::delete(key, lsn);
        self.update(&record, ghost).await
    }

    async fn update<'g>(&self, record: &DataRecord<'_>, ghost: &'g Ghost) -> Result<()> {
        let mut layout = DataPageLayout::default();
        layout.add(&record);
        let mut page: DataPageBuf = self.alloc_page(layout.size());
        page.add(&record);
        loop {
            match self.try_update(record.key, &mut page, ghost).await {
                Ok(_) => return Ok(()),
                Err(Error::Conflict) => continue,
                Err(err) => {
                    self.dealloc_page(page);
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
            let next_ptr = node.view.as_ptr();
            delta.set_ver(node.view.ver());
            delta.set_len(node.view.len() + 1);
            delta.set_next(next_ptr);
            match self.update_node(node.id, next_ptr, delta.as_ptr()) {
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

impl Tree {
    fn page_ptr(&self, id: NodeId) -> PagePtr {
        self.table.get(id.into()).into()
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

    fn update_node<'g>(&self, id: NodeId, old: PagePtr, new: PagePtr) -> Option<PagePtr> {
        self.table
            .cas(id.into(), old.into(), new.into())
            .map(|now| now.into())
    }

    fn alloc_page<P: From<PageBuf>>(&self, size: usize) -> P {
        // TODO: swap out pages when allocation fails.
        self.alloc.alloc(size).unwrap().into()
    }

    fn dealloc_page<P: Into<PageBuf>>(&self, page: P) {
        self.alloc.dealloc(page.into());
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

    fn swapout_page<'g>(&self, id: NodeId, ptr: PagePtr, ghost: &'g Ghost) -> Result<PageRef<'g>> {
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
                let page = self.store.load_page_with_addr(addr.into()).await?;
                self.swapin_page(id, ptr, page, ghost)
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
                let page = self.store.load_page_with_handle(&info.handle).await?;
                if page.ver() != view.ver() {
                    return Err(Error::Conflict);
                }
                self.swapin_page(id, ptr, page, ghost)
            }
        }
    }

    async fn try_find_data_node<'g>(&self, key: &[u8], ghost: &'g Ghost) -> Result<NodePair<'g>> {
        let mut cursor = NodeIndex::root();
        let mut parent = None;
        loop {
            let node = self.node_pair(cursor.id, ghost);
            if node.view.ver() != cursor.ver {
                self.try_help_pending_smo(&node, parent.as_ref(), ghost)?;
                return Err(Error::Conflict);
            }
            if node.view.is_data() {
                return Ok(node);
            }
            cursor = self.search_index_node(&node, key, ghost).await?;
            parent = Some(node);
        }
    }

    fn try_help_pending_smo<'g>(
        &self,
        node: &NodePair<'g>,
        parent: Option<&NodePair<'g>>,
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
        let mut merger = MergeIterBuilder::default();
        loop {
            match page.kind() {
                PageKind::Data => {
                    let page = DataPageRef::from(page);
                    merger.add(page.iter());
                }
                _ => unreachable!(),
            }
            if let Some(next) = page.next() {
                page = self.load_page_with_ptr(node.id, next, ghost).await?;
            } else {
                return Ok(merger.build());
            }
        }
    }

    async fn search_data_node<'g>(
        &self,
        node: &NodePair<'g>,
        key: &[u8],
        lsn: u64,
        ghost: &'g Ghost,
    ) -> Result<Option<&'g [u8]>> {
        let mut page = self.load_page_with_view(node.id, &node.view, ghost).await?;
        loop {
            match page.kind() {
                PageKind::Data => {
                    let page = DataPageRef::from(page);
                    if let Some(value) = page.get(key, lsn) {
                        match value {
                            DataValue::Put(value) => return Ok(Some(value)),
                            DataValue::Delete => return Ok(None),
                        }
                    }
                }
                _ => unreachable!(),
            }
            if let Some(next) = page.next() {
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
        let iter = self.iter_data_node(node, ghost).await?;
        let mut layout = DataPageLayout::default();
        for record in iter {
            layout.add(&record);
        }
        let page: DataPageBuf = self.alloc_page(layout.size());
        // TODO: builds data page
        if self
            .update_node(node.id, node.view.as_ptr(), page.as_ptr())
            .is_some()
        {
            return Err(Error::Conflict);
        }
        if page.size() >= self.opts.data_node_size {
            let _ = self
                .try_split_data_node(node.id, page.as_ref(), ghost)
                .await;
        }
        Ok(())
    }

    async fn iter_index_node<'g>(
        &self,
        node: &NodePair<'g>,
        ghost: &'g Ghost,
    ) -> Result<IndexNodeIter<'g>> {
        let mut page = self.load_page_with_view(node.id, &node.view, ghost).await?;
        let mut merger = MergeIterBuilder::default();
        loop {
            match page.kind() {
                PageKind::Index => {
                    let page = IndexPageRef::from(page);
                    merger.add(page.iter());
                }
                _ => unreachable!(),
            }
            if let Some(next) = page.next() {
                page = self.load_page_with_ptr(node.id, next, ghost).await?;
            } else {
                return Ok(merger.build());
            }
        }
    }

    async fn search_index_node<'g>(
        &self,
        node: &NodePair<'g>,
        key: &[u8],
        ghost: &'g Ghost,
    ) -> Result<NodeIndex> {
        let mut page = self.load_page_with_view(node.id, &node.view, ghost).await?;
        loop {
            match page.kind() {
                PageKind::Data => {
                    let page = IndexPageRef::from(page);
                    if let Some(value) = page.get(key) {
                        todo!()
                    }
                }
                _ => unreachable!(),
            }
            if let Some(next) = page.next() {
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
        let iter = self.iter_index_node(node, ghost).await?;
        let mut layout = IndexPageLayout::default();
        for record in iter {
            layout.add(&record);
        }
        let page: IndexPageBuf = self.alloc_page(layout.size());
        // TODO: builds the index page
        if self
            .update_node(node.id, node.view.as_ptr(), page.as_ptr())
            .is_some()
        {
            return Err(Error::Conflict);
        }
        if page.size() >= self.opts.index_node_size {
            let _ = self
                .try_split_index_node(node.id, page.as_ref(), ghost)
                .await;
        }
        Ok(())
    }
}
