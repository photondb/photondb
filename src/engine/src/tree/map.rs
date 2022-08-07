use std::sync::Arc;

use bumpalo::Bump;
use crossbeam_epoch::{pin, Guard};
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
pub struct Map {
    tree: Arc<Tree>,
}

impl Map {
    pub fn open(opts: Options) -> Result<Self> {
        let tree = Tree::open(opts)?;
        Ok(Self {
            tree: Arc::new(tree),
        })
    }

    pub fn get<F>(&self, key: &[u8], lsn: u64, f: F) -> Result<()>
    where
        F: FnMut(Option<&[u8]>),
    {
        let guard = &pin();
        let key = Key::new(key, lsn);
        self.tree.get(key, guard).map(f)
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
        // Initializes the tree as root -> leaf.
        let root_id = self.table.alloc().unwrap();
        let leaf_id = self.table.alloc().unwrap();
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
        let (node, _) = self.find_leaf(key.raw, guard)?;
        self.lookup_value(key, &node, guard)
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
                        self.cache.dealloc_page(page);
                    }
                    return Err(err);
                }
            }
        }
    }

    fn try_insert<'g>(&self, key: &[u8], mut page: PagePtr, guard: &'g Guard) -> Result<()> {
        let (mut node, _) = self.find_leaf(key, guard)?;
        loop {
            page.set_ver(node.page.ver());
            page.set_rank(node.page.rank() + 1);
            page.set_next(node.page.as_addr().into());
            match self.table.cas(node.id, page.next(), page.into()) {
                Ok(_) => {
                    if page.rank() as usize >= self.opts.data_delta_length {
                        node.page = page.into();
                        let _ = self.consolidate_data_node(&node, guard);
                    }
                    return Ok(());
                }
                Err(addr) => {
                    if let Some(page) = self.page_view(addr.into(), guard) {
                        // We can keep retrying as long as the page version doesn't change.
                        if page.ver() == node.page.ver() {
                            node.page = page;
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
    fn page_addr(&self, id: u64) -> PageAddr {
        self.table.get(id).into()
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
                self.cache.dealloc_page(new);
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
                        cache.dealloc_page(page);
                        continue;
                    }
                }
                break;
            }
        });
    }

    fn install_node<'a: 'g, 'g>(&'a self, new: impl Into<u64>) -> Result<u64> {
        let id = self.table.alloc().ok_or(Error::Alloc)?;
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
    fn find_leaf<'a: 'g, 'g>(
        &'a self,
        key: &[u8],
        guard: &'g Guard,
    ) -> Result<(Node<'g>, Option<Node<'g>>)> {
        let mut index = ROOT_INDEX;
        let mut start = [].as_slice();
        let mut right = None;
        let mut parent = None;
        loop {
            let addr = self.page_addr(index.id);
            let page = self.page_view(addr, guard).expect("the node must be valid");
            let node = Node {
                id: index.id,
                page,
                start,
                right,
            };
            if node.page.ver() != index.ver {
                self.reconcile_node(node, parent, guard)?;
                return Err(Error::Again);
            }
            if node.page.is_data() {
                return Ok((node, parent));
            }
            let (child, right_child) = self.lookup_index(key, &node, guard)?;
            let child = child.expect("the index must exists");
            index = child.1;
            start = child.0;
            right = right_child;
            parent = Some(node);
        }
    }

    fn walk_node<'a: 'g, 'g, F>(&'a self, node: &Node<'g>, guard: &'g Guard, mut f: F) -> Result<()>
    where
        F: FnMut(PageRef<'g>) -> bool,
    {
        let mut page = self.load_page_with_view(node.id, node.page, guard)?;
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
        node: &Node<'g>,
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
        node: &Node<'g>,
        guard: &'g Guard,
    ) -> Result<(Option<IndexItem<'g>>, Option<IndexItem<'g>>)> {
        let mut left_index = None;
        let mut right_index = None;
        self.walk_node(node, guard, |page| {
            if let TypedPage::Index(page) = page.into() {
                let (left, right) = page.find(key);
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

    fn data_node_iter<'a: 'g, 'b, 'g>(
        &'a self,
        node: &Node<'g>,
        bump: &'b Bump,
        guard: &'g Guard,
    ) -> Result<DataNodeIter<'g, 'b>> {
        let mut limit = None;
        let mut merger = MergingIterBuilder::with_len(node.page.rank() as usize + 1);
        self.walk_node(node, guard, |page| {
            match TypedPage::from(page) {
                TypedPage::Data(page) => {
                    merger.add(bump.alloc(page.into()));
                }
                TypedPage::Split(page) => {
                    if limit == None {
                        let index = page.get();
                        limit = Some(index.0);
                    }
                }
                _ => unreachable!(),
            }
            false
        })?;
        Ok(DataNodeIter::new(merger.build(), limit))
    }

    fn delta_data_iter<'a: 'g, 'b, 'g>(
        &'a self,
        node: &Node<'g>,
        bump: &'b Bump,
        guard: &'g Guard,
    ) -> Result<(DataNodeIter<'g, 'b>, PageAddr)> {
        let mut next = 0;
        let mut size = 0;
        let mut limit = None;
        let mut merger = MergingIterBuilder::with_len(node.page.rank() as usize + 1);
        self.walk_node(node, guard, |page| {
            match TypedPage::from(page) {
                TypedPage::Data(page) => {
                    if size < page.size() && limit.is_none() && merger.len() >= 2 {
                        return true;
                    }
                    next = page.next();
                    size += page.size();
                    merger.add(bump.alloc(page.into()));
                }
                TypedPage::Split(page) => {
                    if limit == None {
                        let index = page.get();
                        limit = Some(index.0);
                    }
                }
                _ => unreachable!(),
            }
            false
        })?;
        Ok((DataNodeIter::new(merger.build(), limit), next.into()))
    }

    fn index_node_iter<'a: 'g, 'b, 'g>(
        &'a self,
        node: &Node<'g>,
        bump: &'b Bump,
        guard: &'g Guard,
    ) -> Result<IndexNodeIter<'g, 'b>> {
        let mut limit = None;
        let mut merger = MergingIterBuilder::with_len(node.page.rank() as usize + 1);
        self.walk_node(node, guard, |page| {
            match TypedPage::from(page) {
                TypedPage::Index(page) => {
                    merger.add(bump.alloc(page.into()));
                }
                TypedPage::Split(page) => {
                    if limit == None {
                        let index = page.get();
                        limit = Some(index.0);
                    }
                }
                _ => unreachable!(),
            }
            false
        })?;
        let iter = merger.build();
        Ok(IndexNodeIter::new(iter, limit))
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
            self.stats.num_data_splits.inc();
            trace!("split data node {} {:?}", id, split_page.get());
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
        let right_id = self.install_node(right_page)?;
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
            self.cache.dealloc_page(right_page);
            err
        })
    }

    fn reconcile_node<'a: 'g, 'g>(
        &'a self,
        node: Node<'a>,
        parent: Option<Node<'a>>,
        guard: &'g Guard,
    ) -> Result<()> {
        let page = self.load_page_with_view(node.id, node.page, guard)?;
        if let TypedPage::Split(page) = page.into() {
            let split_index = page.get();
            if let Some(mut parent) = parent {
                self.reconcile_split_node(&node, &mut parent, split_index, guard)?;
            } else {
                self.reconcile_split_root(&node, split_index, guard)?;
            }
        }
        Ok(())
    }

    fn reconcile_split_node<'a: 'g, 'g>(
        &'a self,
        node: &Node<'g>,
        parent: &mut Node<'g>,
        split_index: IndexItem<'g>,
        guard: &'g Guard,
    ) -> Result<PageRef<'g>> {
        let left_index = (node.start, Index::new(node.id, node.page.ver()));
        let mut index_page = if let Some(right_index) = node.right {
            assert!(right_index.0 > split_index.0);
            let delta_data = [left_index, split_index, (right_index.0, NULL_INDEX)];
            let mut delta_iter = SliceIter::from(&delta_data);
            IndexPageBuilder::default().build_from_iter(&self.cache, &mut delta_iter)?
        } else {
            let delta_data = [left_index, split_index];
            let mut delta_iter = SliceIter::from(&delta_data);
            IndexPageBuilder::default().build_from_iter(&self.cache, &mut delta_iter)?
        };

        index_page.set_ver(parent.page.ver());
        index_page.set_rank(parent.page.rank() + 1);
        index_page.set_next(parent.page.as_addr().into());
        let page = self.update_node(parent.id, index_page.next(), index_page, guard)?;
        trace!("reconcile split node {} parent {}", node.id, parent.id);

        if page.rank() as usize >= self.opts.index_delta_length {
            parent.page = page.into();
            self.consolidate_index_node(parent, guard)
        } else {
            Ok(page)
        }
    }

    fn reconcile_split_root<'a: 'g, 'g>(
        &'a self,
        node: &Node<'g>,
        split_index: IndexItem<'g>,
        guard: &'g Guard,
    ) -> Result<PageRef<'g>> {
        assert_eq!(node.id, ROOT_INDEX.id);
        let root_addr = node.page.as_addr();

        // Builds a new root with the original root in the left and the split node in the right.
        let left_id = self.install_node(root_addr)?;
        let left_index = Index::new(left_id, node.page.ver());
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
        node: &Node<'g>,
        guard: &'g Guard,
    ) -> Result<PageRef<'g>> {
        let mut bump = Bump::new();
        let (mut iter, next) = self.delta_data_iter(node, &mut bump, guard)?;
        let mut page = DataPageBuilder::default().build_from_iter(&self.cache, &mut iter)?;
        page.set_ver(node.page.ver());
        page.set_next(next.into());

        let addr = node.page.as_addr();
        let page = self.update_node(node.id, addr, page, guard)?;
        self.dealloc_node(addr, page.next().into(), guard);
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
        node: &Node<'g>,
        guard: &'g Guard,
    ) -> Result<PageRef<'g>> {
        let mut bump = Bump::new();
        let mut iter = self.index_node_iter(node, &mut bump, guard)?;
        let mut page = IndexPageBuilder::default().build_from_iter(&self.cache, &mut iter)?;
        page.set_ver(node.page.ver());

        let addr = node.page.as_addr();
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
    bump: Bump,
    guard: Guard,
    cursor: Option<Vec<u8>>,
}

impl Iter {
    fn new(tree: Arc<Tree>) -> Self {
        Self {
            tree,
            bump: Bump::new(),
            guard: pin(),
            cursor: Some(Vec::new()),
        }
    }

    pub fn next_with<F>(&mut self, mut f: F) -> Result<()>
    where
        F: FnMut((&[u8], &[u8])),
    {
        while let Some(mut iter) = self.next_node()? {
            while let Some(item) = iter.next() {
                f(item);
            }
        }
        Ok(())
    }

    fn next_node(&mut self) -> Result<Option<NodeIter<'_>>> {
        if let Some(cursor) = self.cursor.take() {
            self.bump.reset();
            self.guard.repin();
            let (node, _) = self.tree.find_leaf(&cursor, &self.guard)?;
            self.cursor = node.right.map(|r| r.0.to_vec());
            let iter = self.tree.data_node_iter(&node, &self.bump, &self.guard)?;
            Ok(Some(NodeIter(iter)))
        } else {
            Ok(None)
        }
    }
}

struct NodeIter<'a>(DataNodeIter<'a, 'a>);

impl NodeIter<'_> {
    fn next(&mut self) -> Option<(&[u8], &[u8])> {
        while let Some((k, v)) = self.0.next() {
            if let Value::Put(value) = v {
                return Some((k.raw, value));
            }
        }
        None
    }
}
