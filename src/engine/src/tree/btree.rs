use crossbeam_epoch::Guard;

use super::{Error, Options, PageAddr, PageRef, PageStore, PageTable, PageView, Result};

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

    pub async fn get<'g>(&self, key: &[u8], guard: &'g Guard) -> Result<Option<&'g [u8]>> {
        retry!(self.try_get(key, guard).await)
    }

    async fn try_get<'g>(&self, key: &[u8], guard: &'g Guard) -> Result<Option<&'g [u8]>> {
        let node = self.try_find_data_node(key, guard).await?;
        self.try_find_data_in_node(key, &node, guard).await
    }

    pub async fn put<'g>(&self, key: &[u8], value: &[u8], guard: &'g Guard) -> Result<()> {
        todo!()
    }

    pub async fn delete<'g>(&self, key: &[u8], guard: &'g Guard) -> Result<()> {
        todo!()
    }

    async fn try_update<'g>(&self, node: &NodeView<'g>, guard: &'g Guard) -> Result<()> {
        todo!()
    }
}

impl BTree {
    fn page_addr(&self, id: NodeId) -> PageAddr {
        self.table.get(id.into()).into()
    }

    fn page_view<'g>(&self, id: NodeId, _: &'g Guard) -> PageView<'g> {
        let addr = self.page_addr(id);
        match addr {
            PageAddr::Mem(addr) => PageView::Mem(addr.into()),
            PageAddr::Disk(addr) => {
                let addr = addr.into();
                let info = self.store.page_info(addr).unwrap();
                PageView::Disk(addr, info)
            }
        }
    }

    fn node_view<'g>(&self, id: NodeId, guard: &'g Guard) -> NodeView<'g> {
        NodeView::new(id, self.page_view(id, guard))
    }

    async fn load_page_with_addr<'g>(
        &self,
        addr: PageAddr,
        guard: &'g Guard,
    ) -> Result<Option<PageRef<'g>>> {
        todo!()
    }

    async fn load_page_with_view<'g>(
        &self,
        view: PageView<'g>,
        guard: &'g Guard,
    ) -> Result<PageRef<'g>> {
        match view {
            PageView::Mem(page) => Ok(page),
            PageView::Disk(_, info) => {
                let page = self.store.load_page_with_ptr(info.ptr).await?;
                // Ok(page)
                todo!()
            }
        }
    }

    async fn try_find_data_node<'g>(&self, key: &[u8], guard: &'g Guard) -> Result<NodeView<'g>> {
        let mut cursor = NodeIndex::root();
        let mut parent = None;
        loop {
            let node = self.node_view(cursor.id, guard);
            if node.ver() != cursor.ver {
                self.try_help_pending_smo(&node, parent.as_ref(), guard)?;
                return Err(Error::Aborted);
            }
            if node.is_data() {
                return Ok(node);
            }
            cursor = self.try_find_index_in_node(key, &node, guard).await?;
            parent = Some(node);
        }
    }

    fn try_help_pending_smo<'g>(
        &self,
        node: &NodeView<'g>,
        parent: Option<&NodeView<'g>>,
        guard: &'g Guard,
    ) -> Result<()> {
        todo!()
    }

    async fn try_find_data_in_node<'g>(
        &self,
        key: &[u8],
        node: &NodeView<'g>,
        guard: &'g Guard,
    ) -> Result<Option<&'g [u8]>> {
        todo!()
    }

    async fn try_find_index_in_node<'g>(
        &self,
        key: &[u8],
        node: &NodeView<'g>,
        guard: &'g Guard,
    ) -> Result<NodeIndex> {
        todo!()
    }

    fn try_consolidate_data_node<'g>(&self, node: &NodeView<'g>, guard: &'g Guard) -> Result<()> {
        todo!()
    }

    fn try_consolidate_index_node<'g>(&self, node: &NodeView<'g>, guard: &'g Guard) -> Result<()> {
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

struct NodeView<'g> {
    id: NodeId,
    page: PageView<'g>,
}

impl<'g> NodeView<'g> {
    fn new(id: NodeId, page: PageView<'g>) -> Self {
        Self { id, page }
    }

    fn ver(&self) -> u64 {
        self.page.ver()
    }

    fn is_data(&self) -> bool {
        self.page.kind().is_data()
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
