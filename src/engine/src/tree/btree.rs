use super::{
    page::*,
    pagecache::PageCache,
    pagestore::{PageInfo, PageStore},
    pagetable::PageTable,
    Error, Ghost, Options, Result,
};

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum PageAddr {
    Mem(u64),
    Disk(u64),
}

const MEM_DISK_MASK: u64 = 1 << 63;

impl From<u64> for PageAddr {
    fn from(addr: u64) -> Self {
        if addr & MEM_DISK_MASK == 0 {
            Self::Mem(addr)
        } else {
            Self::Disk(addr & !MEM_DISK_MASK)
        }
    }
}

impl From<PageAddr> for u64 {
    fn from(addr: PageAddr) -> u64 {
        match addr {
            PageAddr::Mem(addr) => addr,
            PageAddr::Disk(addr) => addr | MEM_DISK_MASK,
        }
    }
}

enum PageView<'a> {
    Mem(PageRef<'a>),
    Disk(PageInfo, u64),
}

impl<'a> PageView<'a> {
    fn ver(&self) -> u64 {
        match self {
            Self::Mem(page) => page.ver(),
            Self::Disk(info, _) => info.ver,
        }
    }

    fn len(&self) -> u8 {
        match self {
            Self::Mem(page) => page.len(),
            Self::Disk(info, _) => info.len,
        }
    }

    fn is_data(&self) -> bool {
        match self {
            Self::Mem(page) => page.is_data(),
            Self::Disk(info, _) => info.is_data,
        }
    }

    fn as_addr(&self) -> PageAddr {
        match self {
            Self::Mem(page) => PageAddr::Mem(page.as_ptr() as u64),
            Self::Disk(_, addr) => PageAddr::Disk(*addr),
        }
    }
}

const ROOT_ID: u64 = 0;
const ROOT_INDEX: Index = Index::new(ROOT_ID, 0);

struct Node<'a> {
    id: u64,
    view: PageView<'a>,
}

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
        let node = self.try_find_leaf_node(key.raw, ghost).await?;
        self.lookup_data(&node, key, ghost).await
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
        let mut page = unsafe {
            SortedPageBuilder::default()
                .build_from_iter(&self.cache, &(key, value))
                .unwrap()
        };
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
        delta: mut PagePtr,
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
        let data_page = unsafe { SortedPageBuilder::default().build(&self.cache).unwrap() };
        self.table.set(data_id, new.into());
        let data_index = Index::new(data_id, 0);
        let root_page = unsafe {
            SortedPageBuilder::default()
                .build_from_iter(&self.cache, &(&[], data_index))
                .unwrap()
        };
        self.table.set(root_id, new.into());
    }

    fn node<'g>(&self, id: u64, ghost: &'g Ghost) -> Node<'g> {
        let addr = self.page_addr(id);
        // Our use cases ensure that we will not access invalid nodes, so it is safe to unwrap here.
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

    async fn swapin_page<'a>(&self, id: u64, addr: u64, _: &'a Ghost) -> Result<PageRef<'a>> {
        let page = self.store.load_page(addr).await?;
        Ok(page.into())
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

    async fn iter_node<'g>(&self, node: &Node<'g>, ghost: &'g Ghost) -> Result<DataNodeIter<'g>> {
        let mut page = self.load_page_with_view(node.id, &node.view, ghost).await?;
        let mut merger = MergingIterBuilder::default();
        loop {
            let addr = page.next().into();
            match self.load_page_with_addr(node.id, addr, ghost).await? {
                Some(next) => page = next,
                None => break,
            }
        }
        Ok(merger.build())
    }

    async fn lookup_data<'g, T>(
        &self,
        node: &Node<'g>,
        target: T,
        ghost: &'g Ghost,
    ) -> Result<Option<&'g [u8]>> {
        let mut page = self.load_page_with_view(node.id, &node.view, ghost).await?;
        loop {
            let addr = page.next().into();
            match self.load_page_with_addr(node.id, addr, ghost).await? {
                Some(next) => page = next,
                None => break,
            }
        }
        Ok(None)
    }

    fn try_reconcile_node<'g>(
        &self,
        node: &Node<'g>,
        parent: Option<&Node<'g>>,
        ghost: &'g Ghost,
    ) -> Result<()> {
        todo!()
    }

    async fn try_consolidate_node<'g, K, V>(
        &self,
        node: &Node<'g>,
        ghost: &'g Ghost,
    ) -> Result<()> {
        let iter = self.iter_node(node, ghost).await?;
        let page = unsafe {
            SortedPageBuilder::default()
                .build_from_iter(&self.cache, iter)
                .unwrap()
        };
        if self
            .update_node(node.id, node.view.as_addr(), page.as_ptr())
            .is_some()
        {
            return Err(Error::Conflict);
        }
        Ok(())
    }

    async fn try_find_data_node<'g>(&self, target: &[u8], ghost: &'g Ghost) -> Result<Node<'g>> {
        let mut cursor = ROOT_INDEX;
        let mut parent = None;
        loop {
            let node = self.node(cursor.id, ghost);
            if node.view.ver() != cursor.ver {
                self.try_reconcile_node(&node, parent.as_ref(), ghost)?;
                return Err(Error::Conflict);
            }
            if node.view.is_data() {
                return Ok(node);
            }
            cursor = self.lookup_index(&node, target, ghost).await?;
            parent = Some(node);
        }
    }
}
