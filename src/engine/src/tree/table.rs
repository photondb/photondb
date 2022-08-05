use std::sync::Arc;

use crossbeam_epoch::{pin, unprotected, Guard};
use log::trace;

use super::{
    node::*,
    page::*,
    pagecache::PageCache,
    pagestore::PageStore,
    pagetable::PageTable,
    stats::{AtomicStats, Stats},
    Error, Result,
};

#[derive(Clone, Debug)]
pub struct Options {
    pub cache_size: usize,
    pub data_node_size: usize,
    pub data_delta_length: usize,
    pub index_node_size: usize,
    pub index_delta_length: usize,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            cache_size: usize::MAX,
            data_node_size: 8 * 1024,
            data_delta_length: 8,
            index_node_size: 4 * 1024,
            index_delta_length: 4,
        }
    }
}

#[derive(Clone)]
pub struct Table {
    tree: Arc<Tree>,
}

impl Table {
    pub fn open(opts: Options) -> Result<Self> {
        let tree = Tree::open(opts)?;
        Ok(Self {
            tree: Arc::new(tree),
        })
    }

    pub fn get(&self, key: &[u8], lsn: u64) -> Result<Option<Vec<u8>>> {
        let guard = &pin();
        let key = Key::new(key, lsn);
        let value = self.tree.get(key, guard)?;
        Ok(value.map(|v| v.to_vec()))
    }

    pub fn iter(&self) -> Iter {
        Iter::new(self.tree.clone())
    }

    pub fn put<'g>(&self, key: &[u8], lsn: u64, value: &[u8]) -> Result<()> {
        let guard = &pin();
        let key = Key::new(key, lsn);
        let value = Value::Put(value);
        self.tree.insert(key, value, guard)
    }

    pub fn delete<'g>(&self, key: &[u8], lsn: u64) -> Result<()> {
        let guard = &pin();
        let key = Key::new(key, lsn);
        let value = Value::Delete;
        self.tree.insert(key, value, guard)
    }

    pub fn stats(&self) -> Stats {
        self.tree.stats()
    }
}

struct Tree {
    opts: Options,
    table: PageTable,
    cache: PageCache,
    store: PageStore,
    stats: AtomicStats,
}

impl Tree {
    fn open(opts: Options) -> Result<Self> {
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

    fn init(self) -> Result<Self> {
        let guard = unsafe { unprotected() };
        // Initializes the tree as root -> leaf.
        let root_id = self.table.alloc(guard).unwrap();
        let leaf_id = self.table.alloc(guard).unwrap();
        let leaf_page = DataPageBuilder::default().build(&self.cache)?;
        self.table.set(leaf_id, leaf_page.into());
        let mut root_iter = OptionIter::from(([].as_slice(), Index::with_id(leaf_id)));
        let root_page = IndexPageBuilder::default().build_from_iter(&self.cache, &mut root_iter)?;
        self.table.set(root_id, root_page.into());
        Ok(self)
    }

    fn get<'a: 'g, 'g>(&'a self, key: Key<'_>, guard: &'g Guard) -> Result<Option<&'g [u8]>> {
        loop {
            match self.try_get(key, guard) {
                Err(Error::Again) => continue,
                other => return other,
            }
        }
    }

    fn try_get<'a: 'g, 'g>(&'a self, key: Key<'_>, guard: &'g Guard) -> Result<Option<&'g [u8]>> {
        let node = self.find_leaf(key.raw, guard)?;
        self.lookup_value(key, node, guard)
    }

    fn insert<'g>(&self, key: Key<'_>, value: Value<'_>, guard: &'g Guard) -> Result<()> {
        let mut iter = OptionIter::from((key, value));
        let page = DataPageBuilder::default().build_from_iter(&self.cache, &mut iter)?;
        loop {
            match self.try_insert(key.raw, page, guard) {
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

    fn try_insert<'g>(&self, key: &[u8], mut page: PagePtr, guard: &'g Guard) -> Result<()> {
        let mut node = self.find_leaf(key, guard)?;
        loop {
            page.set_ver(node.view.ver());
            page.set_rank(node.view.rank() + 1);
            page.set_next(node.view.as_addr().into());
            match self.table.cas(node.id, page.next(), page.into()) {
                Ok(_) => {
                    if page.rank() as usize >= self.opts.data_delta_length {
                        node.view = page.into();
                        let _ = self.consolidate_data_node(node, guard);
                    }
                    return Ok(());
                }
                Err(addr) => {
                    if let Some(view) = self.page_view(addr.into(), guard) {
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

    fn stats(&self) -> Stats {
        Stats {
            cache_size: self.cache.size() as u64,
            num_data_splits: self.stats.num_data_splits.get(),
            num_data_consolidations: self.stats.num_data_consolidations.get(),
            num_index_splits: self.stats.num_index_splits.get(),
            num_index_consolidations: self.stats.num_index_consolidations.get(),
        }
    }
}

impl Tree {
    fn node<'a: 'g, 'g>(&'a self, id: u64, guard: &'g Guard) -> Option<Node<'g>> {
        let addr = self.table.get(id).into();
        self.page_view(addr, guard).map(|view| Node::new(id, view))
    }

    fn page_view<'a: 'g, 'g>(&'a self, addr: PageAddr, _: &'g Guard) -> Option<PageView<'g>> {
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
        _: &'g Guard,
    ) -> Result<PageRef<'g>> {
        self.table
            .cas(id, old.into(), new.into())
            .map(|_| new.into())
            .map_err(|_| unsafe {
                self.cache.dealloc(new);
                Error::Again
            })
    }

    fn dealloc_node<'a: 'g, 'g>(&'a self, addr: PageAddr, until: PageAddr, guard: &'g Guard) {
        let cache = self.cache.clone();
        guard.defer(move || unsafe {
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

    fn install_node<'a: 'g, 'g>(&'a self, new: impl Into<u64>, guard: &'g Guard) -> Result<u64> {
        let id = self.table.alloc(guard).ok_or(Error::Alloc)?;
        self.table.set(id, new.into());
        Ok(id)
    }

    fn load_page_with_view<'a: 'g, 'g>(
        &'a self,
        _: u64,
        view: PageView<'g>,
        _: &'g Guard,
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
        _: &'g Guard,
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

impl Tree {
    fn find_leaf<'a: 'g, 'g>(&'a self, key: &[u8], guard: &'g Guard) -> Result<Node<'g>> {
        let mut cursor = ROOT_INDEX;
        let mut parent = None;
        loop {
            let node = self.node(cursor.id, guard).expect("the node must be valid");
            if node.view.ver() != cursor.ver {
                self.reconcile_node(node, parent, guard)?;
                return Err(Error::Again);
            }
            if node.view.is_data() {
                return Ok(node);
            }
            cursor = self
                .lookup_index(key, node, guard)?
                .expect("the index must be found");
            parent = Some(node);
        }
    }

    fn walk_node<'a: 'g, 'g, F>(&'a self, node: Node<'g>, guard: &'g Guard, mut f: F) -> Result<()>
    where
        F: FnMut(PageRef<'g>) -> bool,
    {
        let mut page = self.load_page_with_view(node.id, node.view, guard)?;
        loop {
            if f(page) {
                break;
            }
            match self.load_page_with_addr(node.id, page.next().into(), guard)? {
                Some(next) => page = next,
                None => break,
            }
        }
        Ok(())
    }

    fn lookup_value<'a: 'g, 'g>(
        &'a self,
        key: Key<'_>,
        node: Node<'g>,
        guard: &'g Guard,
    ) -> Result<Option<&'g [u8]>> {
        let mut value = None;
        self.walk_node(node, guard, |page| {
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
        guard: &'g Guard,
    ) -> Result<Option<Index>> {
        let mut value = None;
        self.walk_node(node, guard, |page| {
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
        guard: &'g Guard,
    ) -> Result<(Option<IndexItem<'g>>, Option<IndexItem<'g>>)> {
        let mut left_index = None;
        let mut right_index = None;
        self.walk_node(node, guard, |page| {
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

    fn data_node_iter<'a: 'g, 'g>(
        &'a self,
        node: Node<'g>,
        guard: &'g Guard,
    ) -> Result<DataNodeIter<'g>> {
        let mut size = 0;
        let mut next = 0;
        let mut highest = None;
        let mut children = Vec::with_capacity(node.view.rank() as usize + 1);
        self.walk_node(node, guard, |page| {
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
        Ok(DataNodeIter::new(next.into(), highest, children))
    }

    fn data_iter_chain<'a: 'g, 'g>(
        &'a self,
        node: Node<'g>,
        guard: &'g Guard,
    ) -> Result<DataIterChain<'g>> {
        let mut size = 0;
        let mut next = 0;
        let mut highest = None;
        let mut children = Vec::with_capacity(node.view.rank() as usize + 1);
        self.walk_node(node, guard, |page| {
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
        guard: &'g Guard,
    ) -> Result<IndexIterChain<'g>> {
        let mut highest = None;
        let mut children = Vec::with_capacity(node.view.rank() as usize + 1);
        self.walk_node(node, guard, |page| {
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
        guard: &'g Guard,
    ) -> Result<PageRef<'g>> {
        let data_page = DataPageRef::from(page);
        if let Some((sep, mut iter)) = data_page.split() {
            let right_page = DataPageBuilder::default().build_from_iter(&self.cache, &mut iter)?;
            let split_page = self.install_split_page(id, page, sep, right_page, guard)?;
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
        guard: &'g Guard,
    ) -> Result<PageRef<'g>> {
        let index_page = IndexPageRef::from(page);
        if let Some((sep, mut iter)) = index_page.split() {
            let right_page = IndexPageBuilder::default().build_from_iter(&self.cache, &mut iter)?;
            let split_page = self.install_split_page(id, page, sep, right_page, guard)?;
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
        guard: &'g Guard,
    ) -> Result<SplitPageRef<'g>> {
        let right_id = self.install_node(right_page, guard)?;
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
            self.update_node(left_id, left_page, split_page, guard)
                .map(SplitPageRef::from)
        };
        split().map_err(|err| unsafe {
            self.table.dealloc(right_id, guard);
            self.cache.dealloc(right_page);
            err
        })
    }

    fn reconcile_node<'a: 'g, 'g>(
        &'a self,
        node: Node<'g>,
        parent: Option<Node<'g>>,
        guard: &'g Guard,
    ) -> Result<()> {
        let page = self.load_page_with_view(node.id, node.view, guard)?;
        if let TypedPage::Split(page) = page.into() {
            if let Some(node) = parent {
                self.reconcile_split_node(node, page, guard)?;
            } else {
                self.reconcile_split_root(node, page, guard)?;
            }
        }
        Ok(())
    }

    fn reconcile_split_node<'a: 'g, 'g>(
        &'a self,
        mut node: Node<'g>,
        split: SplitPageRef<'g>,
        guard: &'g Guard,
    ) -> Result<PageRef<'g>> {
        let split_index = split.get();
        let (left_index, right_index) = self.lookup_index_range(split_index.0, node, guard)?;
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
        let page = self.update_node(node.id, index_page.next(), index_page, guard)?;
        trace!("reconcile split node {} {:?}", node.id, page);

        if page.rank() as usize >= self.opts.index_delta_length {
            node.view = page.into();
            self.consolidate_index_node(node, guard)
        } else {
            Ok(page)
        }
    }

    fn reconcile_split_root<'a: 'g, 'g>(
        &'a self,
        node: Node<'g>,
        split: SplitPageRef<'g>,
        guard: &'g Guard,
    ) -> Result<PageRef<'g>> {
        assert_eq!(node.id, ROOT_INDEX.id);
        let root_addr = node.view.as_addr();

        // Builds a new root with the original root in the left and the split node in the right.
        let left_id = self.install_node(root_addr, guard)?;
        let left_index = Index::new(left_id, node.view.ver());
        let split_index = split.get();
        let root_data = [([].as_slice(), left_index), split_index];
        let mut root_iter = SliceIter::from(&root_data);
        let root_page = IndexPageBuilder::default().build_from_iter(&self.cache, &mut root_iter)?;

        self.update_node(node.id, root_addr, root_page, guard)
            .map(|page| {
                trace!("reconcile split root {} {:?}", node.id, page);
                page
            })
            .map_err(|err| {
                self.table.dealloc(left_id, guard);
                err
            })
    }

    fn consolidate_data_node<'a: 'g, 'g>(
        &'a self,
        node: Node<'g>,
        guard: &'g Guard,
    ) -> Result<PageRef<'g>> {
        //let mut chain = self.data_iter_chain(node, guard)?;
        // let mut iter = chain.iter();
        let mut chain = self.data_node_iter(node, guard)?;
        let mut page = DataPageBuilder::default().build_from_iter(&self.cache, &mut chain)?;
        page.set_ver(node.view.ver());
        page.set_next(chain.next().into());

        let addr = node.view.as_addr();
        let page = self.update_node(node.id, addr, page, guard)?;
        self.dealloc_node(addr, chain.next(), guard);
        self.stats.num_data_consolidations.inc();
        trace!("consolidate data node {} {:?}", node.id, page);

        if page.size() >= self.opts.data_node_size {
            self.split_data_node(node.id, page, guard)
        } else {
            Ok(page)
        }
    }

    fn consolidate_index_node<'a: 'g, 'g>(
        &'a self,
        node: Node<'g>,
        guard: &'g Guard,
    ) -> Result<PageRef<'g>> {
        let mut chain = self.index_iter_chain(node, guard)?;
        let mut iter = chain.iter();
        let mut page = IndexPageBuilder::default().build_from_iter(&self.cache, &mut iter)?;
        page.set_ver(node.view.ver());

        let addr = node.view.as_addr();
        let page = self.update_node(node.id, addr, page, guard)?;
        self.dealloc_node(addr, page.next().into(), guard);
        self.stats.num_index_consolidations.inc();
        trace!("consolidate index node {} {:?}", node.id, page);

        if page.size() >= self.opts.index_node_size {
            self.split_index_node(node.id, page, guard)
        } else {
            Ok(page)
        }
    }
}

pub struct Iter {
    tree: Arc<Tree>,
}

impl Iter {
    fn new(tree: Arc<Tree>) -> Self {
        Self { tree }
    }

    pub fn next(&mut self) -> Option<(&[u8], &[u8])> {
        todo!()
    }

    pub fn seek(&mut self, _target: &[u8]) {
        todo!()
    }
}
