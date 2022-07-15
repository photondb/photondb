use super::{
    page::{PageKind, PagePtr, PageRef},
    pagestore::{PageAddr, PageInfo, PageStore},
    pagetable::PageTable,
    Error, Ghost, Options, Result,
};

pub struct BTree {
    table: PageTable,
    store: PageStore,
}

macro_rules! retry {
    ($e:expr) => {
        loop {
            match $e {
                Err(Error::Aborted) => continue,
                other => break other,
            }
        }
    };
}

impl BTree {
    pub async fn open(opts: Options) -> Result<Self> {
        let table = PageTable::default();
        let store = PageStore::open(opts).await?;
        Ok(Self { table, store })
    }

    pub async fn get<'g>(&self, key: &[u8], ghost: &'g Ghost) -> Result<Option<&'g [u8]>> {
        retry!(self.try_get(key, ghost).await)
    }

    async fn try_get<'g>(&self, key: &[u8], ghost: &'g Ghost) -> Result<Option<&'g [u8]>> {
        let node = self.try_find_data_node(key, ghost).await?;
        self.try_find_data_in_node(key, &node, ghost).await
    }

    pub async fn put<'g>(&self, key: &[u8], value: &[u8], ghost: &'g Ghost) -> Result<()> {
        todo!()
    }

    pub async fn delete<'g>(&self, key: &[u8], ghost: &'g Ghost) -> Result<()> {
        todo!()
    }

    async fn try_update<'g>(&self, node: &NodePair<'g>, ghost: &'g Ghost) -> Result<()> {
        todo!()
    }
}

impl BTree {
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
        NodePair::new(id, view)
    }

    fn swapin_page<'g>(
        &self,
        id: NodeId,
        ptr: PagePtr,
        buf: Vec<u8>,
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

    async fn load_page_with_node<'g>(
        &self,
        node: &NodePair<'g>,
        ghost: &'g Ghost,
    ) -> Result<PageRef<'g>> {
        match node.view {
            PageView::Mem(page) => Ok(page),
            PageView::Disk(addr, ref info) => {
                let ptr = PagePtr::Disk(addr.into());
                let buf = self.store.load_page_with_handle(&info.handle).await?;
                self.swapin_page(node.id, ptr, buf, ghost)
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

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
struct NodeId(u64);

impl NodeId {
    pub const fn root() -> Self {
        Self(0)
    }
}

impl From<u64> for NodeId {
    fn from(id: u64) -> Self {
        Self(id)
    }
}

impl Into<u64> for NodeId {
    fn into(self) -> u64 {
        self.0
    }
}

struct NodePair<'g> {
    id: NodeId,
    view: PageView<'g>,
}

impl<'g> NodePair<'g> {
    fn new(id: NodeId, view: PageView<'g>) -> Self {
        Self { id, view }
    }

    fn ver(&self) -> u64 {
        self.view.ver()
    }

    fn is_data(&self) -> bool {
        self.view.kind().is_data()
    }
}

pub enum PageView<'a> {
    Mem(PageRef<'a>),
    Disk(PageAddr, PageInfo),
}

impl<'a> PageView<'a> {
    pub fn ver(&self) -> u64 {
        match self {
            Self::Mem(page) => page.ver(),
            Self::Disk(_, page) => page.ver,
        }
    }

    pub fn kind(&self) -> PageKind {
        match self {
            Self::Mem(page) => page.kind(),
            Self::Disk(_, page) => page.kind,
        }
    }
}

struct NodeIndex {
    id: NodeId,
    ver: u64,
}

impl NodeIndex {
    const fn root() -> Self {
        Self {
            id: NodeId::root(),
            ver: 0,
        }
    }
}
