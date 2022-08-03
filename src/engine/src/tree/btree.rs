use std::sync::atomic::{AtomicUsize, Ordering};

use log::trace;

use super::{
    node::*,
    page::*,
    pagecache::{PageAddr, PageCache, PageView},
    pagestore::PageStore,
    pagetable::PageTable,
    Error, Ghost, Options, Result,
};

#[derive(Debug)]
pub struct Stats {
    pub num_data_splits: usize,
    pub num_data_consolidations: usize,
    pub num_index_splits: usize,
    pub num_index_consolidations: usize,
}

struct AtomicStats {
    num_data_splits: AtomicUsize,
    num_data_consolidations: AtomicUsize,
    num_index_splits: AtomicUsize,
    num_index_consolidations: AtomicUsize,
}

impl AtomicStats {
    fn snapshot(&self) -> Stats {
        Stats {
            num_data_splits: self.num_data_splits.load(Ordering::Relaxed),
            num_data_consolidations: self.num_data_consolidations.load(Ordering::Relaxed),
            num_index_splits: self.num_index_splits.load(Ordering::Relaxed),
            num_index_consolidations: self.num_index_consolidations.load(Ordering::Relaxed),
        }
    }
}

impl Default for AtomicStats {
    fn default() -> Self {
        AtomicStats {
            num_data_splits: AtomicUsize::new(0),
            num_data_consolidations: AtomicUsize::new(0),
            num_index_splits: AtomicUsize::new(0),
            num_index_consolidations: AtomicUsize::new(0),
        }
    }
}

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
        self.stats.snapshot()
    }

    pub fn get<'a: 'g, 'g>(
        &'a self,
        key: &[u8],
        lsn: u64,
        ghost: &'g Ghost,
    ) -> Result<Option<&'g [u8]>> {
        let key = Key::new(key, lsn);
        loop {
            match self.try_get(key, ghost) {
                Err(Error::Again) => continue,
                other => return other,
            }
        }
    }

    fn try_get<'a: 'g, 'g>(&'a self, key: Key<'_>, ghost: &'g Ghost) -> Result<Option<&'g [u8]>> {
        let node = self.find_data_node(key.raw, ghost)?;
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
        let mut node = self.find_data_node(key, ghost)?;
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

    fn load_page_with_view<'a: 'g, 'g>(
        &'a self,
        _: u64,
        view: PageView<'g>,
        _: &'g Ghost,
    ) -> Result<PageRef<'g>> {
        match view {
            PageView::Mem(page) => Ok(page),
            PageView::Disk(_, _) => todo!(),
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
            PageAddr::Disk(_) => todo!(),
        }
    }

    fn node<'a: 'g, 'g>(&'a self, id: u64, ghost: &'g Ghost) -> Option<Node<'g>> {
        let addr = self.table.get(id).into();
        self.page_view(addr, ghost).map(|view| Node::new(id, view))
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

    fn replace_node<'a: 'g, 'g>(
        &'a self,
        id: u64,
        old: PageAddr,
        new: PagePtr,
        ghost: &'g Ghost,
    ) -> Result<PageRef<'g>> {
        self.update_node(id, old, new, ghost)?;

        // Deallocates the old page chain.
        let cache = self.cache.clone();
        ghost.guard().defer(move || unsafe {
            let mut next = old;
            while let PageAddr::Mem(addr) = next {
                if let Some(page) = PagePtr::new(addr as *mut u8) {
                    next = page.next().into();
                    cache.dealloc(page);
                } else {
                    break;
                }
            }
        });

        Ok(new.into())
    }

    fn install_node<'a: 'g, 'g>(&'a self, new: impl Into<u64>, ghost: &'g Ghost) -> Result<u64> {
        let id = self.table.alloc(ghost.guard()).ok_or(Error::Alloc)?;
        self.table.set(id, new.into());
        Ok(id)
    }

    fn traverse_node<'a: 'g, 'g, F>(
        &'a self,
        node: Node<'g>,
        ghost: &'g Ghost,
        mut f: F,
    ) -> Result<()>
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

    fn iter_data_node<'a: 'g, 'g>(
        &'a self,
        node: Node<'g>,
        ghost: &'g Ghost,
    ) -> Result<DataNodeIter<'g>> {
        let mut merger = MergingIterBuilder::default();
        let mut highest = None;
        self.traverse_node(node, ghost, |page| {
            match page.kind() {
                PageKind::Data => {
                    merger.add(page.into());
                }
                PageKind::Split => {
                    if highest == None {
                        let split = SplitPageRef::from(page);
                        let index = split.get();
                        highest = Some(index.0);
                    }
                }
                PageKind::Index => unreachable!(),
            }
            false
        })?;
        Ok(DataNodeIter::new(merger.build(), highest))
    }

    fn iter_index_node<'a: 'g, 'g>(
        &'a self,
        node: Node<'g>,
        ghost: &'g Ghost,
    ) -> Result<IndexNodeIter<'g>> {
        let mut merger = MergingIterBuilder::default();
        let mut highest = None;
        self.traverse_node(node, ghost, |page| {
            match page.kind() {
                PageKind::Index => {
                    merger.add(page.into());
                }
                PageKind::Split => {
                    if highest == None {
                        let split = SplitPageRef::from(page);
                        let index = split.get();
                        highest = Some(index.0);
                    }
                }
                PageKind::Data => unreachable!(),
            }
            false
        })?;
        Ok(IndexNodeIter::new(merger.build(), highest))
    }

    fn lookup_value<'a: 'g, 'g>(
        &'a self,
        key: Key<'_>,
        node: Node<'g>,
        ghost: &'g Ghost,
    ) -> Result<Option<&'g [u8]>> {
        let mut value = None;
        self.traverse_node(node, ghost, |page| {
            if page.kind() == PageKind::Data {
                let page = DataPageRef::from(page);
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
        self.traverse_node(node, ghost, |page| {
            if page.kind() == PageKind::Index {
                let page = IndexPageRef::from(page);
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
    ) -> Result<(Option<(&'g [u8], Index)>, Option<(&'g [u8], Index)>)> {
        let mut left_index = None;
        let mut right_index = None;
        self.traverse_node(node, ghost, |page| {
            if page.kind() == PageKind::Index {
                let page = IndexPageRef::from(page);
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

    fn find_data_node<'a: 'g, 'g>(&'a self, key: &[u8], ghost: &'g Ghost) -> Result<Node<'g>> {
        let mut cursor = ROOT_INDEX;
        let mut parent = None;
        loop {
            // Our access pattern guarantees that the node must exists.
            let node = self.node(cursor.id, ghost).unwrap();
            if node.view.ver() != cursor.ver {
                self.reconcile_node(node, parent, ghost)?;
                return Err(Error::Again);
            }
            if node.view.is_data() {
                return Ok(node);
            }
            // Our access pattern guarantees that the index must exists.
            cursor = self.lookup_index(key, node, ghost)?.unwrap();
            parent = Some(node);
        }
    }

    fn reconcile_node<'a: 'g, 'g>(
        &'a self,
        node: Node<'g>,
        parent: Option<Node<'g>>,
        ghost: &'g Ghost,
    ) -> Result<()> {
        let page = self.load_page_with_view(node.id, node.view, ghost)?;
        if page.kind() == PageKind::Split {
            let split = SplitPageRef::from(page);
            if let Some(node) = parent {
                self.reconcile_split_node(node, split, ghost)?;
            } else {
                self.reconcile_split_root(node, split, ghost)?;
            }
        }
        Ok(())
    }

    fn reconcile_split_node<'a: 'g, 'g>(
        &'a self,
        node: Node<'g>,
        split: SplitPageRef<'g>,
        ghost: &'g Ghost,
    ) -> Result<PageRef<'g>> {
        let split_index = split.get();
        let (left_index, right_index) = self.lookup_index_range(split_index.0, node, ghost)?;
        // The left index must exists when split.
        let mut left_index = left_index.unwrap();
        left_index.1.ver = split.ver();

        let mut delta_page = if let Some(right_index) = right_index {
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

        delta_page.set_ver(node.view.ver());
        delta_page.set_rank(node.view.rank() + 1);
        delta_page.set_next(node.view.as_addr().into());
        let mut new_page = self.update_node(node.id, delta_page.next(), delta_page, ghost)?;
        trace!("reconcile split node {} done {:?}", node.id, new_page);
        if new_page.rank() as usize >= self.opts.index_delta_length {
            let new_node = Node::new(node.id, new_page.into());
            new_page = self.consolidate_index_node(new_node, ghost)?;
        }
        Ok(new_page)
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
        trace!("reconcile split root {} with {:?}", node.id, root_data);
        let root_page = IndexPageBuilder::default().build_from_iter(&self.cache, &mut root_iter)?;

        // Replaces the original root with the new root.
        let new_page = self
            .update_node(node.id, root_addr, root_page, ghost)
            .map_err(|err| {
                self.table.dealloc(left_id, ghost.guard());
                err
            })?;
        trace!("reconcile split root {} done {:?}", node.id, new_page);
        Ok(new_page)
    }

    fn install_split<'a: 'g, 'g>(
        &'a self,
        left_id: u64,
        left_page: PageRef<'g>,
        split_key: &[u8],
        right_page: PageRef<'_>,
        ghost: &'g Ghost,
    ) -> Result<PageRef<'g>> {
        let right_id = self.install_node(right_page, ghost)?;
        // Makes sure that we will deallocate the right node on error.
        let split = || {
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
        };
        let new_page = split().map_err(|err| {
            self.table.dealloc(right_id, ghost.guard());
            err
        })?;
        trace!(
            "split node {} {:?} at {:?} to node {} {:?}",
            left_id,
            left_page,
            split_key,
            right_id,
            right_page,
        );
        Ok(new_page)
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
            let new_page = self
                .install_split(id, page, sep, right_page.into(), ghost)
                .map_err(|err| unsafe {
                    self.cache.dealloc(right_page);
                    err
                })?;
            self.stats.num_data_splits.fetch_add(1, Ordering::Relaxed);
            Ok(new_page)
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
            let new_page = self
                .install_split(id, page, sep, right_page.into(), ghost)
                .map_err(|err| unsafe {
                    self.cache.dealloc(right_page);
                    err
                })?;
            self.stats.num_index_splits.fetch_add(1, Ordering::Relaxed);
            Ok(new_page)
        } else {
            Ok(page)
        }
    }

    fn consolidate_data_node<'a: 'g, 'g>(
        &'a self,
        node: Node<'g>,
        ghost: &'g Ghost,
    ) -> Result<PageRef<'g>> {
        let mut iter = self.iter_data_node(node, ghost)?;
        let mut page = DataPageBuilder::default().build_from_iter(&self.cache, &mut iter)?;
        page.set_ver(node.view.ver());
        let mut new_page = self.replace_node(node.id, node.view.as_addr(), page, ghost)?;
        self.stats
            .num_data_consolidations
            .fetch_add(1, Ordering::Relaxed);
        trace!("consolidate data node {} done {:?}", node.id, new_page);
        if new_page.size() >= self.opts.data_node_size {
            new_page = self.split_data_node(node.id, new_page, ghost)?;
        }
        Ok(new_page)
    }

    fn consolidate_index_node<'a: 'g, 'g>(
        &'a self,
        node: Node<'g>,
        ghost: &'g Ghost,
    ) -> Result<PageRef<'g>> {
        let mut iter = self.iter_index_node(node, ghost)?;
        let mut page = IndexPageBuilder::default().build_from_iter(&self.cache, &mut iter)?;
        page.set_ver(node.view.ver());
        let mut new_page = self.replace_node(node.id, node.view.as_addr(), page, ghost)?;
        self.stats
            .num_index_consolidations
            .fetch_add(1, Ordering::Relaxed);
        trace!("consolidate index node {} done {:?}", node.id, new_page);
        if new_page.size() >= self.opts.index_node_size {
            new_page = self.split_index_node(node.id, new_page, ghost)?;
        }
        Ok(new_page)
    }
}
