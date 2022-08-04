use log::trace;

use super::{
    node::*,
    page::*,
    pagecache::{PageAddr, PageCache, PageView},
    pagestore::PageStore,
    pagetable::PageTable,
    stats::{AtomicStats, Stats},
    Error, Ghost, Options, Result,
};

pub struct BTree {
    opts: Options,
    table: PageTable,
    cache: PageCache,
    store: PageStore,
    stats: AtomicStats,
}

impl BTree {
    pub fn open(opts: Options) -> Result<Self> {
        let table = PageTable::default();
        let cache = PageCache::default();
        let store = PageStore::open()?;
        let tree = Self {
            opts,
            table,
            cache,
            store,
            stats: AtomicStats::default(),
        };
        tree.init()
    }

    pub fn stats(&self) -> Stats {
        let mut stats = self.stats.snapshot();
        stats.cache_size = self.cache.size() as u64;
        stats
    }

    pub fn get<'a: 'g, 'g>(
        &'a self,
        key: &[u8],
        lsn: u64,
        ghost: &'g Ghost,
    ) -> Result<Option<&'g [u8]>> {
        let key = Key::new(key, lsn);
        loop {
            match self.try_get(&key, ghost) {
                Err(Error::Again) => continue,
                other => return other,
            }
        }
    }

    fn try_get<'a: 'g, 'g>(&'a self, key: &Key<'_>, ghost: &'g Ghost) -> Result<Option<&'g [u8]>> {
        let node = self.find_leaf(key.raw, ghost)?;
        self.lookup_value(key, node, ghost)
    }

    pub fn put<'g>(&self, key: &[u8], lsn: u64, value: &[u8], ghost: &'g Ghost) -> Result<()> {
        let key = Key::new(key, lsn);
        let value = Value::Put(value);
        self.update(key, value, ghost)
    }

    pub fn delete<'g>(&self, key: &[u8], lsn: u64, ghost: &'g Ghost) -> Result<()> {
        let key = Key::new(key, lsn);
        let value = Value::Delete;
        self.update(key, value, ghost)
    }

    fn update<'g>(&self, key: Key<'_>, value: Value<'_>, ghost: &'g Ghost) -> Result<()> {
        let mut iter = OptionIter::from((key, value));
        let page = DataPageBuilder::default().build_from_iter(&self.cache, &mut iter)?;
        loop {
            match self.try_update(key.raw, page, ghost) {
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

    fn try_update<'g>(&self, key: &[u8], mut page: PagePtr, ghost: &'g Ghost) -> Result<()> {
        let mut node = self.find_leaf(key, ghost)?;
        loop {
            page.set_ver(node.view.ver());
            page.set_rank(node.view.rank() + 1);
            page.set_next(node.view.as_addr().into());
            match self.table.cas(node.id, page.next(), page.into()) {
                Ok(_) => {
                    if page.rank() as usize >= self.opts.data_delta_length {
                        node.view = page.into();
                        let _ = self.consolidate_data_node(node, ghost);
                    }
                    return Ok(());
                }
                Err(addr) => {
                    if let Some(view) = self.page_view(addr.into(), ghost) {
                        // We can keep retrying as long as the page version doesn't change.
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

    fn node<'a: 'g, 'g>(&'a self, id: u64, ghost: &'g Ghost) -> Option<Node<'g>> {
        let addr = self.table.get(id).into();
        self.page_view(addr, ghost).map(|view| Node::new(id, view))
    }

    fn page_view<'a: 'g, 'g>(&'a self, addr: PageAddr, _: &'g Ghost) -> Option<PageView<'g>> {
        match addr {
            PageAddr::Mem(addr) => {
                let page = unsafe { PageRef::new(addr as *mut u8) };
                page.map(PageView::Mem)
            }
            PageAddr::Disk(addr) => self
                .store
                .page_info(addr)
                .map(|info| PageView::Disk(info, addr)),
        }
    }

    fn update_node<'a: 'g, 'g>(
        &'a self,
        id: u64,
        old: impl Into<u64>,
        new: PagePtr,
        _: &'g Ghost,
    ) -> Result<PageRef<'g>> {
        self.table
            .cas(id, old.into(), new.into())
            .map(|_| new.into())
            .map_err(|_| unsafe {
                self.cache.dealloc(new);
                Error::Again
            })
    }

    fn dealloc_node<'a: 'g, 'g>(&'a self, addr: PageAddr, until: PageAddr, ghost: &'g Ghost) {
        let cache = self.cache.clone();
        ghost.guard().defer(move || unsafe {
            let mut next = addr;
            while next != until {
                if let PageAddr::Mem(addr) = next {
                    if let Some(page) = PagePtr::new(addr as *mut u8) {
                        next = page.next().into();
                        cache.dealloc(page);
                        continue;
                    }
                }
                break;
            }
        });
    }

    fn install_node<'a: 'g, 'g>(&'a self, new: impl Into<u64>, ghost: &'g Ghost) -> Result<u64> {
        let id = self.table.alloc(ghost.guard()).ok_or(Error::Alloc)?;
        self.table.set(id, new.into());
        Ok(id)
    }

    fn load_page_with_view<'a: 'g, 'g>(
        &'a self,
        _: u64,
        view: PageView<'g>,
        _: &'g Ghost,
    ) -> Result<PageRef<'g>> {
        match view {
            PageView::Mem(page) => Ok(page),
            PageView::Disk(_, addr) => {
                let ptr = self.store.load_page(addr)?;
                Ok(ptr.into())
            }
        }
    }

    fn load_page_with_addr<'a: 'g, 'g>(
        &'a self,
        _: u64,
        addr: PageAddr,
        _: &'g Ghost,
    ) -> Result<Option<PageRef<'g>>> {
        match addr {
            PageAddr::Mem(addr) => Ok(unsafe { PageRef::new(addr as *mut u8) }),
            PageAddr::Disk(addr) => {
                let ptr = self.store.load_page(addr)?;
                Ok(Some(ptr.into()))
            }
        }
    }
}

impl BTree {
    fn find_leaf<'a: 'g, 'g>(&'a self, key: &[u8], ghost: &'g Ghost) -> Result<Node<'g>> {
        let mut cursor = ROOT_INDEX;
        let mut parent = None;
        loop {
            let node = self.node(cursor.id, ghost).expect("the node must be valid");
            if node.view.ver() != cursor.ver {
                self.reconcile_node(node, parent, ghost)?;
                return Err(Error::Again);
            }
            if node.view.is_data() {
                return Ok(node);
            }
            cursor = self
                .lookup_index(key, node, ghost)?
                .expect("the index must be found");
            parent = Some(node);
        }
    }

    fn walk_node<'a: 'g, 'g, F>(&'a self, node: Node<'g>, ghost: &'g Ghost, mut f: F) -> Result<()>
    where
        F: FnMut(PageRef<'g>) -> bool,
    {
        let mut page = self.load_page_with_view(node.id, node.view, ghost)?;
        loop {
            if f(page) {
                break;
            }
            match self.load_page_with_addr(node.id, page.next().into(), ghost)? {
                Some(next) => page = next,
                None => break,
            }
        }
        Ok(())
    }

    fn lookup_value<'a: 'g, 'g>(
        &'a self,
        key: &Key<'_>,
        node: Node<'g>,
        ghost: &'g Ghost,
    ) -> Result<Option<&'g [u8]>> {
        let mut value = None;
        self.walk_node(node, ghost, |page| {
            if let TypedPage::Data(page) = page.into() {
                if let Some((_, v)) = page.find(key) {
                    value = v.into();
                    return true;
                }
            }
            false
        })?;
        Ok(value)
    }

    fn lookup_index<'a: 'g, 'g>(
        &'a self,
        key: &[u8],
        node: Node<'g>,
        ghost: &'g Ghost,
    ) -> Result<Option<Index>> {
        let mut value = None;
        self.walk_node(node, ghost, |page| {
            if let TypedPage::Index(page) = page.into() {
                if let Some((_, v)) = page.find(key) {
                    if v != NULL_INDEX {
                        value = v.into();
                        return true;
                    }
                }
            }
            false
        })?;
        Ok(value)
    }

    fn lookup_index_range<'a: 'g, 'g>(
        &'a self,
        key: &[u8],
        node: Node<'g>,
        ghost: &'g Ghost,
    ) -> Result<(Option<IndexItem<'g>>, Option<IndexItem<'g>>)> {
        let mut left_index = None;
        let mut right_index = None;
        self.walk_node(node, ghost, |page| {
            if let TypedPage::Index(page) = page.into() {
                let (left, right) = page.find_range(key);
                if let Some(left) = left {
                    if left.1 != NULL_INDEX {
                        left_index = Some(left);
                        right_index = right;
                        return true;
                    }
                }
            }
            false
        })?;
        Ok((left_index, right_index))
    }

    fn data_iter_chain<'a: 'g, 'g>(
        &'a self,
        node: Node<'g>,
        ghost: &'g Ghost,
    ) -> Result<DataIterChain<'g>> {
        let mut size = 0;
        let mut next = 0;
        let mut highest = None;
        let mut children = Vec::with_capacity(node.view.rank() as usize + 1);
        self.walk_node(node, ghost, |page| {
            match TypedPage::from(page) {
                TypedPage::Data(page) => {
                    // TODO: explores other strategies here.
                    if size < page.size() && highest.is_none() && children.len() >= 2 {
                        return true;
                    }
                    size += page.size();
                    next = page.next();
                    children.push(page.into());
                }
                TypedPage::Split(page) => {
                    if highest == None {
                        let index = page.get();
                        highest = Some(index.0);
                    }
                }
                _ => unreachable!(),
            }
            false
        })?;
        Ok(DataIterChain::new(next.into(), highest, children))
    }

    fn index_iter_chain<'a: 'g, 'g>(
        &'a self,
        node: Node<'g>,
        ghost: &'g Ghost,
    ) -> Result<IndexIterChain<'g>> {
        let mut highest = None;
        let mut children = Vec::with_capacity(node.view.rank() as usize + 1);
        self.walk_node(node, ghost, |page| {
            match TypedPage::from(page) {
                TypedPage::Index(page) => {
                    children.push(page.into());
                }
                TypedPage::Split(page) => {
                    if highest == None {
                        let index = page.get();
                        highest = Some(index.0);
                    }
                }
                _ => unreachable!(),
            }
            false
        })?;
        Ok(IndexIterChain::new(highest, children))
    }

    fn split_data_node<'a: 'g, 'g>(
        &'a self,
        id: u64,
        page: PageRef<'g>,
        ghost: &'g Ghost,
    ) -> Result<PageRef<'g>> {
        let data_page = DataPageRef::from(page);
        if let Some((sep, mut iter)) = data_page.split() {
            let right_page = DataPageBuilder::default().build_from_iter(&self.cache, &mut iter)?;
            let split_page = self.install_split_page(id, page, sep, right_page, ghost)?;
            trace!("split data node {} {:?}", id, split_page.get());
            self.stats.num_data_splits.inc();
            Ok(split_page.into())
        } else {
            Ok(page)
        }
    }

    fn split_index_node<'a: 'g, 'g>(
        &'a self,
        id: u64,
        page: PageRef<'g>,
        ghost: &'g Ghost,
    ) -> Result<PageRef<'g>> {
        let index_page = IndexPageRef::from(page);
        if let Some((sep, mut iter)) = index_page.split() {
            let right_page = IndexPageBuilder::default().build_from_iter(&self.cache, &mut iter)?;
            let split_page = self.install_split_page(id, page, sep, right_page, ghost)?;
            self.stats.num_index_splits.inc();
            trace!("split index node {} {:?}", id, split_page.get());
            Ok(split_page.into())
        } else {
            Ok(page)
        }
    }

    fn install_split_page<'a: 'g, 'g>(
        &'a self,
        left_id: u64,
        left_page: PageRef<'g>,
        split_key: &[u8],
        right_page: PagePtr,
        ghost: &'g Ghost,
    ) -> Result<SplitPageRef<'g>> {
        let right_id = self.install_node(right_page, ghost)?;
        let split = || -> Result<SplitPageRef<'_>> {
            let mut split_page = SplitPageBuilder::default().build_with_index(
                &self.cache,
                split_key,
                Index::with_id(right_id),
            )?;
            split_page.set_ver(left_page.ver() + 1);
            split_page.set_rank(left_page.rank() + 1);
            split_page.set_next(left_page.into());
            split_page.set_data(left_page.is_data());
            self.update_node(left_id, left_page, split_page, ghost)
                .map(SplitPageRef::from)
        };
        split().map_err(|err| unsafe {
            self.table.dealloc(right_id, ghost.guard());
            self.cache.dealloc(right_page);
            err
        })
    }

    fn reconcile_node<'a: 'g, 'g>(
        &'a self,
        node: Node<'g>,
        parent: Option<Node<'g>>,
        ghost: &'g Ghost,
    ) -> Result<()> {
        let page = self.load_page_with_view(node.id, node.view, ghost)?;
        if let TypedPage::Split(page) = page.into() {
            if let Some(node) = parent {
                self.reconcile_split_node(node, page, ghost)?;
            } else {
                self.reconcile_split_root(node, page, ghost)?;
            }
        }
        Ok(())
    }

    fn reconcile_split_node<'a: 'g, 'g>(
        &'a self,
        mut node: Node<'g>,
        split: SplitPageRef<'g>,
        ghost: &'g Ghost,
    ) -> Result<PageRef<'g>> {
        let split_index = split.get();
        let (left_index, right_index) = self.lookup_index_range(split_index.0, node, ghost)?;
        let mut left_index = left_index.expect("the left index must exists when split");
        left_index.1.ver = split.ver();

        let mut index_page = if let Some(right_index) = right_index {
            assert!(right_index.0 > split_index.0);
            let delta_data = [left_index, split_index, (right_index.0, NULL_INDEX)];
            trace!("reconcile split node {} with {:?}", node.id, delta_data);
            let mut delta_iter = SliceIter::from(&delta_data);
            IndexPageBuilder::default().build_from_iter(&self.cache, &mut delta_iter)?
        } else {
            let delta_data = [left_index, split_index];
            trace!("reconcile split node {} with {:?}", node.id, delta_data);
            let mut delta_iter = SliceIter::from(&delta_data);
            IndexPageBuilder::default().build_from_iter(&self.cache, &mut delta_iter)?
        };

        index_page.set_ver(node.view.ver());
        index_page.set_rank(node.view.rank() + 1);
        index_page.set_next(node.view.as_addr().into());
        let page = self.update_node(node.id, index_page.next(), index_page, ghost)?;
        trace!("reconcile split node {} {:?}", node.id, page);

        if page.rank() as usize >= self.opts.index_delta_length {
            node.view = page.into();
            self.consolidate_index_node(node, ghost)
        } else {
            Ok(page)
        }
    }

    fn reconcile_split_root<'a: 'g, 'g>(
        &'a self,
        node: Node<'g>,
        split: SplitPageRef<'g>,
        ghost: &'g Ghost,
    ) -> Result<PageRef<'g>> {
        assert_eq!(node.id, ROOT_INDEX.id);
        let root_addr = node.view.as_addr();

        // Builds a new root with the original root in the left and the split node in the right.
        let left_id = self.install_node(root_addr, ghost)?;
        let left_index = Index::new(left_id, node.view.ver());
        let split_index = split.get();
        let root_data = [([].as_slice(), left_index), split_index];
        let mut root_iter = SliceIter::from(&root_data);
        let root_page = IndexPageBuilder::default().build_from_iter(&self.cache, &mut root_iter)?;

        self.update_node(node.id, root_addr, root_page, ghost)
            .map(|page| {
                trace!("reconcile split root {} {:?}", node.id, page);
                page
            })
            .map_err(|err| {
                self.table.dealloc(left_id, ghost.guard());
                err
            })
    }

    fn consolidate_data_node<'a: 'g, 'g>(
        &'a self,
        node: Node<'g>,
        ghost: &'g Ghost,
    ) -> Result<PageRef<'g>> {
        let mut chain = self.data_iter_chain(node, ghost)?;
        let mut iter = chain.iter();
        let mut page = DataPageBuilder::default().build_from_iter(&self.cache, &mut iter)?;
        page.set_ver(node.view.ver());
        page.set_next(chain.next.into());

        let addr = node.view.as_addr();
        let page = self.update_node(node.id, addr, page, ghost)?;
        self.dealloc_node(addr, chain.next, ghost);
        self.stats.num_data_consolidations.inc();
        trace!("consolidate data node {} {:?}", node.id, page);

        if page.size() >= self.opts.data_node_size {
            self.split_data_node(node.id, page, ghost)
        } else {
            Ok(page)
        }
    }

    fn consolidate_index_node<'a: 'g, 'g>(
        &'a self,
        node: Node<'g>,
        ghost: &'g Ghost,
    ) -> Result<PageRef<'g>> {
        let mut chain = self.index_iter_chain(node, ghost)?;
        let mut iter = chain.iter();
        let mut page = IndexPageBuilder::default().build_from_iter(&self.cache, &mut iter)?;
        page.set_ver(node.view.ver());

        let addr = node.view.as_addr();
        let page = self.update_node(node.id, addr, page, ghost)?;
        self.stats.num_index_consolidations.inc();
        trace!("consolidate index node {} {:?}", node.id, page);

        if page.size() >= self.opts.index_node_size {
            self.split_index_node(node.id, page, ghost)
        } else {
            Ok(page)
        }
    }
}
