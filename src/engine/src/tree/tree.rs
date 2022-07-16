use super::{
    node::{NodeId, NodeIndex, NodePair, NodeView},
    page::{
        DeltaDataBuf, DeltaDataLayout, PageBuf, PageKind, PageLayout, PagePtr, PageRef, Record,
    },
    pagestore::PageStore,
    pagetable::PageTable,
    Error, Ghost, Options, Result,
};

pub struct Tree {
    table: PageTable,
    store: PageStore,
}

impl Tree {
    pub async fn open(opts: Options) -> Result<Self> {
        let table = PageTable::default();
        let store = PageStore::open(opts).await?;
        Ok(Self { table, store })
    }

    pub async fn get<'g>(&self, key: &[u8], ghost: &'g Ghost) -> Result<Option<&'g [u8]>> {
        loop {
            match self.try_get(key, ghost).await {
                Err(Error::Again) => continue,
                other => return other,
            }
        }
    }

    async fn try_get<'g>(&self, key: &[u8], ghost: &'g Ghost) -> Result<Option<&'g [u8]>> {
        let node = self.try_find_data_node(key, ghost).await?;
        self.try_find_data_in_node(key, &node, ghost).await
    }

    pub async fn put<'g>(
        &self,
        lsn: u64,
        key: &[u8],
        value: &[u8],
        ghost: &'g Ghost,
    ) -> Result<()> {
        let record = Record::put(lsn, key, value);
        self.update(&record, ghost).await
    }

    pub async fn delete<'g>(&self, lsn: u64, key: &[u8], ghost: &'g Ghost) -> Result<()> {
        let record = Record::delete(lsn, key);
        self.update(&record, ghost).await
    }

    async fn update<'g>(&self, record: &Record<'g>, ghost: &'g Ghost) -> Result<()> {
        let mut layout = DeltaDataLayout::default();
        layout.add(&record);
        let mut page: DeltaDataBuf = self.alloc_page(&layout);
        page.add(&record);
        loop {
            match self
                .try_update(record.key, page.as_ref().as_ptr(), ghost)
                .await
            {
                Ok(_) => {
                    std::mem::forget(page);
                    return Ok(());
                }
                Err(err) => {
                    self.dealloc_page(page.into());
                    return Err(err);
                }
                Err(Error::Again) => continue,
            }
        }
    }

    async fn try_update<'g>(&self, key: &[u8], delta: PagePtr, ghost: &'g Ghost) -> Result<()> {
        let mut node = self.try_find_data_node(key, ghost).await?;
        loop {
            match self.update_node(node.id, node.view.as_ptr(), delta) {
                None => return Ok(()),
                Some(now) => {
                    let view = self.node_view(now, ghost);
                    if view.ver() != node.ver() {
                        return Err(Error::Aborted);
                    }
                    node.view = view;
                }
            }
        }
    }
}

impl Tree {
    fn node_ptr(&self, id: NodeId) -> PagePtr {
        self.table.get(id.into()).into()
    }

    fn node_view<'g>(&self, ptr: PagePtr, _: &'g Ghost) -> NodeView<'g> {
        match ptr {
            PagePtr::Mem(addr) => NodeView::Mem(addr.into()),
            PagePtr::Disk(addr) => {
                let addr = addr.into();
                let info = self.store.page_info(addr).unwrap();
                NodeView::Disk(addr, info)
            }
        }
    }

    fn node_pair<'g>(&self, id: NodeId, ghost: &'g Ghost) -> NodePair<'g> {
        let ptr = self.node_ptr(id);
        let view = self.node_view(ptr, ghost);
        NodePair::new(id, view)
    }

    fn update_node<'g>(&self, id: NodeId, old: PagePtr, new: PagePtr) -> Option<PagePtr> {
        self.table
            .cas(id.into(), old.into(), new.into())
            .map(|now| now.into())
    }

    fn alloc_page<L: PageLayout, B: From<PageBuf>>(&self, layout: &L) -> B {
        todo!()
    }

    fn dealloc_page(&self, page: PageBuf) {
        todo!()
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
                self.swapin_page(id, ptr, buf.into(), ghost)
            }
        }
    }

    async fn load_page_with_node<'g>(
        &self,
        node: &NodePair<'g>,
        ghost: &'g Ghost,
    ) -> Result<PageRef<'g>> {
        match node.view {
            NodeView::Mem(page) => Ok(page),
            NodeView::Disk(addr, ref info) => {
                let ptr = PagePtr::Disk(addr.into());
                let buf = self.store.load_page_with_handle(&info.handle).await?;
                self.swapin_page(node.id, ptr, buf.into(), ghost)
            }
        }
    }

    async fn try_find_data_node<'g>(&self, key: &[u8], ghost: &'g Ghost) -> Result<NodePair<'g>> {
        let mut cursor = NodeIndex::root();
        let mut parent = None;
        loop {
            let node = self.node_pair(cursor.id, ghost);
            if node.ver() != cursor.ver {
                self.try_help_pending_smo(&node, parent.as_ref(), ghost)?;
                return Err(Error::Aborted);
            }
            if node.is_data() {
                return Ok(node);
            }
            cursor = self.try_find_index_in_node(key, &node, ghost).await?;
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

    async fn try_find_data_in_node<'g>(
        &self,
        key: &[u8],
        node: &NodePair<'g>,
        ghost: &'g Ghost,
    ) -> Result<Option<&'g [u8]>> {
        let mut page = self.load_page_with_node(node, ghost).await?;
        loop {
            if let Some(next) = page.next() {
                page = self.load_page_with_ptr(node.id, next, ghost).await?;
            } else {
                return Ok(None);
            }
        }
    }

    async fn try_find_index_in_node<'g>(
        &self,
        key: &[u8],
        node: &NodePair<'g>,
        ghost: &'g Ghost,
    ) -> Result<NodeIndex> {
        todo!()
    }

    fn try_consolidate_data_node<'g>(&self, node: &NodePair<'g>, ghost: &'g Ghost) -> Result<()> {
        todo!()
    }

    fn try_consolidate_index_node<'g>(&self, node: &NodePair<'g>, ghost: &'g Ghost) -> Result<()> {
        todo!()
    }
}
