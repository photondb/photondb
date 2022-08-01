use std::marker::PhantomData;

use super::{
    page::*,
    pagecache::{PageAddr, PageCache, PageView},
    pagestore::PageStore,
    pagetable::PageTable,
    Error, Ghost, Options, Result,
};

const ROOT_INDEX: Index = Index::with_id(PageTable::MIN);
const NULL_INDEX: Index = Index::with_id(PageTable::NAN);

pub struct Node<'a> {
    pub id: u64,
    pub view: PageView,
    _mark: PhantomData<&'a ()>,
}

impl Node<'_> {
    pub fn new(id: u64, view: PageView) -> Self {
        Self {
            id,
            view,
            _mark: PhantomData,
        }
    }
}

pub type DataNodeIter<'a> = MergingIter<DataPageIter<'a>>;
pub type IndexNodeIter<'a> = MergingIter<IndexPageIter<'a>>;

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

    pub async fn get<'k, 'g>(
        &self,
        key: &'k [u8],
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

    async fn try_get<'k, 'g>(&self, key: Key<'k>, ghost: &'g Ghost) -> Result<Option<&'g [u8]>> {
        let node = self.try_find_leaf(key.raw, ghost).await?;
        self.search_data_node(&node, key).await
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
        let page = DataPageBuilder::default().build_from_iter(&self.cache, &mut iter)?;
        loop {
            match self.try_update(key.raw, page, ghost).await {
                Ok(_) => return Ok(()),
                Err(Error::Again) => continue,
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
        let mut node = self.try_find_leaf(key, ghost).await?;
        loop {
            delta.set_ver(node.view.ver());
            delta.set_rank(node.view.rank() + 1);
            delta.set_next(node.view.as_addr().into());
            match self.table.cas(node.id, delta.next(), delta.into()) {
                Ok(_) => {
                    if delta.rank() as usize >= self.opts.data_delta_length {
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
        let leaf_page = DataPageBuilder::default().build(&self.cache)?;
        self.table.set(leaf_id, leaf_page.into());
        let mut root_iter = OptionIter::from(([].as_slice(), Index::with_id(leaf_id)));
        let root_page = IndexPageBuilder::default().build_from_iter(&self.cache, &mut root_iter)?;
        self.table.set(root_id, root_page.into());
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
            PageAddr::Mem(addr) => Ok(unsafe { PagePtr::new(addr as *mut u8) }),
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

    async fn walk_node<'n, 'g, F>(&self, node: &'n Node<'g>, mut f: F) -> Result<()>
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

    async fn iter_data_node<'n, 'g>(&self, node: &'n Node<'g>) -> Result<DataNodeIter<'g>> {
        let mut merger = MergingIterBuilder::default();
        self.walk_node(node, |page| {
            if page.kind() == PageKind::Data {
                merger.add(page.into());
            }
            false
        })
        .await?;
        Ok(merger.build().into())
    }

    async fn iter_index_node<'n, 'g>(&self, node: &'n Node<'g>) -> Result<IndexNodeIter<'g>> {
        let mut merger = MergingIterBuilder::default();
        self.walk_node(node, |page| {
            if page.kind() == PageKind::Index {
                merger.add(page.into());
            }
            false
        })
        .await?;
        Ok(merger.build().into())
    }

    async fn search_data_node<'k, 'n, 'g>(
        &self,
        node: &'n Node<'g>,
        target: Key<'k>,
    ) -> Result<Option<&'g [u8]>> {
        let mut value = None;
        self.walk_node(node, |page| {
            if page.kind() == PageKind::Data {
                let page = DataPageRef::from(page);
                if let Some((_, v)) = page.find(target) {
                    value = v.into();
                    return true;
                }
            }
            false
        })
        .await?;
        Ok(value)
    }

    async fn search_index_node<'k, 'n, 'g>(
        &self,
        node: &'n Node<'g>,
        target: &'k [u8],
    ) -> Result<Option<Index>> {
        let mut value = None;
        self.walk_node(node, |page| {
            if page.kind() == PageKind::Index {
                let page = IndexPageRef::from(page);
                if let Some((_, v)) = page.find(target) {
                    value = v.into();
                    return true;
                }
            }
            false
        })
        .await?;
        Ok(value)
    }

    async fn try_find_leaf<'g>(&self, target: &[u8], ghost: &'g Ghost) -> Result<Node<'g>> {
        let mut cursor = ROOT_INDEX;
        let mut parent = None;
        loop {
            let node = self.node(cursor.id, ghost);
            if node.view.ver() != cursor.ver {
                self.try_reconcile_node(&node, parent.as_ref(), ghost)
                    .await?;
                return Err(Error::Again);
            }
            if node.view.is_leaf() {
                return Ok(node);
            }
            cursor = self.search_index_node(&node, target).await?.unwrap();
            parent = Some(node);
        }
    }

    fn try_install_node<'g>(&self, new: impl Into<u64>, ghost: &'g Ghost) -> Result<u64> {
        let id = self.table.alloc(ghost.guard()).ok_or(Error::Alloc)?;
        self.table.set(id, new.into());
        Ok(id)
    }

    fn try_update_node<'g>(
        &self,
        id: u64,
        old: impl Into<u64>,
        new: PagePtr,
        _: &'g Ghost,
    ) -> Result<()> {
        self.table
            .cas(id, old.into(), new.into())
            .map(|_| ())
            .map_err(|_| unsafe {
                self.cache.dealloc(new);
                Error::Again
            })
    }

    fn try_replace_node<'g>(
        &self,
        id: u64,
        old: PageAddr,
        new: PagePtr,
        ghost: &'g Ghost,
    ) -> Result<()> {
        self.try_update_node(id, old, new, ghost).map(|_| {
            self.dealloc_chain(old, ghost);
        })
    }

    async fn try_reconcile_node<'n, 'g>(
        &self,
        node: &'n Node<'g>,
        parent: Option<&'n Node<'g>>,
        ghost: &'g Ghost,
    ) -> Result<()> {
        let page = self.load_page_with_view(node.id, &node.view).await?;
        if page.kind() == PageKind::Split {
            let split = SplitPageRef::from(page);
            if let Some(parent) = parent {
                self.try_reconcile_split_node(parent, split, ghost).await?;
            } else {
                self.try_reconcile_split_root(node, split, ghost)?;
            }
        }
        Ok(())
    }

    async fn try_reconcile_split_node<'n, 'g>(
        &self,
        node: &'n Node<'g>,
        split: SplitPageRef<'g>,
        ghost: &'g Ghost,
    ) -> Result<()> {
        let split_index = split.get();
        let mut index_iter = self.iter_index_node(node).await?;
        // index_iter.seek_back(index.0);
        let left_index = index_iter.next().unwrap().clone();

        let delta_page = if let Some((k, v)) = index_iter.next() {
            let delta_data = [left_index, split_index, (k, NULL_INDEX)];
            let mut delta_iter = SliceIter::from(&delta_data);
            IndexPageBuilder::default().build_from_iter(&self.cache, &mut delta_iter)?
        } else {
            let delta_data = [left_index, split_index];
            let mut delta_iter = SliceIter::from(&delta_data);
            IndexPageBuilder::default().build_from_iter(&self.cache, &mut delta_iter)?
        };

        self.try_update_node(node.id, node.view.as_addr(), delta_page, ghost)
    }

    fn try_reconcile_split_root<'n, 'g>(
        &self,
        node: &'n Node<'g>,
        split: SplitPageRef<'g>,
        ghost: &'g Ghost,
    ) -> Result<()> {
        assert_eq!(node.id, ROOT_INDEX.id);

        // Links the original root to a new place.
        let root_addr = node.view.as_addr();
        let left_id = self.try_install_node(root_addr, ghost)?;

        // Builds a new root with the original root in the left and the split node in the right.
        let left_index = Index::new(left_id, node.view.ver());
        let split_index = split.get();
        let root_data = [([].as_slice(), left_index), split_index];
        let mut root_iter = SliceIter::from(&root_data);
        let root_page = IndexPageBuilder::default().build_from_iter(&self.cache, &mut root_iter)?;

        // Replaces the original root with the new root.
        self.try_update_node(node.id, root_addr, root_page, ghost)
            .map_err(|err| {
                self.table.dealloc(left_id, ghost.guard());
                err
            })
    }

    fn try_install_split<'g>(
        &self,
        left_id: u64,
        left_page: PagePtr,
        split_key: &[u8],
        right_page: PagePtr,
        ghost: &'g Ghost,
    ) -> Result<()> {
        let right_id = self.try_install_node(right_page, ghost)?;
        let split = || -> Result<()> {
            let mut split_page = SplitPageBuilder::new(left_page.is_leaf()).build_with_index(
                &self.cache,
                split_key,
                Index::with_id(right_id),
            )?;
            split_page.set_ver(left_page.ver() + 1);
            split_page.set_rank(left_page.rank());
            split_page.set_next(left_page.into());
            self.try_update_node(left_id, left_page, split_page, ghost)
        };
        split().map_err(|err| {
            self.table.dealloc(right_id, ghost.guard());
            err
        })
    }

    fn try_split_data_node<'g>(&self, id: u64, page: PagePtr, ghost: &'g Ghost) -> Result<()> {
        let data_page = DataPageRef::from(page);
        if let Some((sep, mut iter)) = data_page.split() {
            let right_page = DataPageBuilder::default().build_from_iter(&self.cache, &mut iter)?;
            self.try_install_split(id, page, sep.raw, right_page, ghost)
                .map_err(|err| unsafe {
                    self.cache.dealloc(right_page);
                    err
                })?;
        }
        Ok(())
    }

    async fn try_consolidate_data_node<'g>(&self, node: &Node<'g>, ghost: &'g Ghost) -> Result<()> {
        let mut iter = self.iter_data_node(node).await?;
        let mut page = DataPageBuilder::default().build_from_iter(&self.cache, &mut iter)?;
        page.set_ver(node.view.ver());
        self.try_replace_node(node.id, node.view.as_addr(), page, ghost)?;
        // let _ = self.try_split_data_node(node.id, page.as_ref(), ghost);
        Ok(())
    }

    async fn try_consolidate_index_node<'g>(
        &self,
        node: &Node<'g>,
        ghost: &'g Ghost,
    ) -> Result<()> {
        let mut iter = self.iter_index_node(node).await?;
        let mut page = IndexPageBuilder::default().build_from_iter(&self.cache, &mut iter)?;
        page.set_ver(node.view.ver());
        self.try_replace_node(node.id, node.view.as_addr(), page, ghost)?;
        Ok(())
    }
}
