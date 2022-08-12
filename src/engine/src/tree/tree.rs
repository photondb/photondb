use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use bumpalo::Bump;
use crossbeam_epoch::{pin, unprotected, Guard};

use super::{
    node::*,
    page::*,
    pagecache::PageCache,
    pagestore::PageStore,
    pagetable::PageTable,
    stats::{AtomicStats, Stats},
    Error, Options, Result,
};

pub struct Tree {
    opts: Options,
    table: PageTable,
    cache: PageCache,
    store: PageStore,
    stats: AtomicStats,
    min_lsn: MinLsn,
}

impl Tree {
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
            min_lsn: MinLsn::new(),
        };
        tree.init()
    }

    fn init(self) -> Result<Self> {
        // Initializes the tree as root -> leaf.
        let root_id = self.table.alloc().unwrap();
        let leaf_id = self.table.alloc().unwrap();
        let leaf_page = DataPageBuilder::default().build(&self.cache)?;
        self.table.set(leaf_id, leaf_page.into());
        let mut root_iter = OptionIter::from(([].as_slice(), Index::new(leaf_id, 0)));
        let root_page = IndexPageBuilder::default().build_from_iter(&self.cache, &mut root_iter)?;
        self.table.set(root_id, root_page.into());
        Ok(self)
    }

    pub fn get<'a: 'g, 'g>(&'a self, key: Key<'_>, guard: &'g Guard) -> Result<Option<&'g [u8]>> {
        loop {
            match self.try_get(key, guard) {
                Ok(value) => {
                    self.stats.succeeded.num_gets.inc();
                    return Ok(value);
                }
                Err(Error::Again) => {
                    self.stats.conflicted.num_gets.inc();
                    continue;
                }
                Err(err) => return Err(err),
            }
        }
    }

    fn try_get<'a: 'g, 'g>(&'a self, key: Key<'_>, guard: &'g Guard) -> Result<Option<&'g [u8]>> {
        let (node, _) = self.find_leaf(key.raw, guard)?;
        self.lookup_value(key, &node, guard)
    }

    pub fn insert<'a: 'g, 'g>(
        &'a self,
        key: Key<'_>,
        value: Value<'_>,
        guard: &'g Guard,
    ) -> Result<()> {
        let mut iter = OptionIter::from((key, value));
        let page = DataPageBuilder::default().build_from_iter(&self.cache, &mut iter)?;
        loop {
            match self.try_insert(key.raw, page, guard) {
                Ok(_) => {
                    self.stats.succeeded.num_inserts.inc();
                    return Ok(());
                }
                Err(Error::Again) => {
                    self.stats.conflicted.num_inserts.inc();
                    continue;
                }
                Err(err) => {
                    unsafe {
                        self.cache.dealloc_page(page);
                    }
                    return Err(err);
                }
            }
        }
    }

    fn try_insert<'a: 'g, 'g>(
        &'a self,
        key: &[u8],
        mut page: PagePtr,
        guard: &'g Guard,
    ) -> Result<()> {
        let (mut node, _) = self.find_leaf(key, guard)?;
        loop {
            page.set_ver(node.view.ver());
            page.set_len(node.view.len() + 1);
            page.set_next(node.view.as_addr().into());
            match self.table.cas(node.id, page.next(), page.into()) {
                Ok(_) => {
                    if page.len() as usize >= self.opts.data_delta_length {
                        node.view = page.into();
                        let _ = self.consolidate_data_node(&node, guard);
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

    pub fn stats(&self) -> Stats {
        Stats {
            cache_size: self.cache.size() as u64,
            succeeded: self.stats.succeeded.snapshot(),
            conflicted: self.stats.conflicted.snapshot(),
        }
    }

    pub fn min_lsn(&self) -> u64 {
        self.min_lsn.get()
    }

    pub fn set_min_lsn(&self, lsn: u64) {
        self.min_lsn.set(lsn)
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
        let mut range = Range::default();
        let mut parent = None;
        loop {
            let addr = self.page_addr(index.id);
            let view = self.page_view(addr, guard).expect("the node must be valid");
            let node = Node {
                id: index.id,
                view,
                range,
            };
            // Ensures that future updates will not overflow the length.
            if node.view.len() >= u8::MAX / 2 {
                self.consolidate_node(&node, guard)?;
                return Err(Error::Again);
            }
            // The node version has changed since we accessed its parent.
            if node.view.ver() != index.ver {
                self.reconcile_node(node, parent, guard)?;
                return Err(Error::Again);
            }
            if node.view.is_leaf() {
                return Ok((node, parent));
            }
            let (child, right) = self.lookup_index(key, &node, guard)?;
            let child = child.expect("the index must exists");
            index = child.1;
            range.start = child.0;
            if let Some(right) = right {
                range.end = Some(right.0);
            }
            parent = Some(node);
        }
    }

    fn walk_node<'a: 'g, 'g, F>(&'a self, node: &Node<'g>, guard: &'g Guard, mut f: F) -> Result<()>
    where
        F: FnMut(TypedPageRef<'g>) -> bool,
    {
        let mut page = self.load_page_with_view(node.id, node.view, guard)?;
        loop {
            if f(page.into()) {
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
            if let TypedPageRef::Data(page) = page {
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
            if let TypedPageRef::Index(page) = page {
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
        let mut merger = MergingIterBuilder::with_len(node.view.len() as usize + 1);
        self.walk_node(node, guard, |page| {
            match page {
                TypedPageRef::Data(page) => {
                    merger.add(bump.alloc(page.into()));
                }
                TypedPageRef::Split(page) => {
                    if limit == None {
                        let index = page.split_index();
                        limit = Some(index.0);
                    }
                }
                _ => unreachable!(),
            }
            false
        })?;
        Ok(DataNodeIter::new(merger.build(), limit))
    }

    fn index_node_iter<'a: 'g, 'b, 'g>(
        &'a self,
        node: &Node<'g>,
        bump: &'b Bump,
        guard: &'g Guard,
    ) -> Result<IndexNodeIter<'g, 'b>> {
        let mut limit = None;
        let mut merger = MergingIterBuilder::with_len(node.view.len() as usize + 1);
        self.walk_node(node, guard, |page| {
            match page {
                TypedPageRef::Index(page) => {
                    merger.add(bump.alloc(page.into()));
                }
                TypedPageRef::Split(page) => {
                    if limit == None {
                        let index = page.split_index();
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
        assert_eq!(page.next(), 0);
        let data_page = DataPageRef::from(page);
        if let Some((sep, mut iter)) = data_page.split() {
            let right_page = DataPageBuilder::default().build_from_iter(&self.cache, &mut iter)?;
            self.install_split_page(id, page, sep, right_page, guard)
                .map(|page| {
                    self.stats.succeeded.num_data_splits.inc();
                    page
                })
                .map_err(|err| {
                    self.stats.conflicted.num_data_splits.inc();
                    err
                })
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
        assert_eq!(page.next(), 0);
        let index_page = IndexPageRef::from(page);
        if let Some((sep, mut iter)) = index_page.split() {
            let right_page = IndexPageBuilder::default().build_from_iter(&self.cache, &mut iter)?;
            self.install_split_page(id, page, sep, right_page, guard)
                .map(|page| {
                    self.stats.succeeded.num_index_splits.inc();
                    page
                })
                .map_err(|err| {
                    self.stats.conflicted.num_index_splits.inc();
                    err
                })
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
    ) -> Result<PageRef<'g>> {
        let right_id = self.install_node(right_page)?;
        let split = || -> Result<PageRef<'_>> {
            let mut split_page = SplitPageBuilder::default().build_with_index(
                &self.cache,
                split_key,
                Index::new(right_id, 0),
            )?;
            split_page.set_ver(left_page.ver() + 1);
            split_page.set_len(left_page.len() + 1);
            split_page.set_next(left_page.into());
            split_page.set_leaf(left_page.is_leaf());
            self.update_node(left_id, left_page, split_page, guard)
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
        let page = self.load_page_with_view(node.id, node.view, guard)?;
        if let TypedPageRef::Split(page) = page.into() {
            let index = page.split_index();
            if let Some(parent) = parent {
                self.reconcile_split_node(node, parent, index, guard)?;
            } else {
                self.reconcile_split_root(node, index, guard)?;
            }
        }
        Ok(())
    }

    fn reconcile_split_node<'a: 'g, 'g>(
        &'a self,
        node: Node<'g>,
        mut parent: Node<'g>,
        split_index: IndexItem<'g>,
        guard: &'g Guard,
    ) -> Result<()> {
        let left_index = (node.range.start, Index::new(node.id, node.view.ver()));
        let mut index_page = if let Some(right_start) = node.range.end {
            assert!(right_start > split_index.0);
            let delta_data = [left_index, split_index, (right_start, NULL_INDEX)];
            let mut delta_iter = SliceIter::from(&delta_data);
            IndexPageBuilder::default().build_from_iter(&self.cache, &mut delta_iter)?
        } else {
            let delta_data = [left_index, split_index];
            let mut delta_iter = SliceIter::from(&delta_data);
            IndexPageBuilder::default().build_from_iter(&self.cache, &mut delta_iter)?
        };

        index_page.set_ver(parent.view.ver());
        index_page.set_len(parent.view.len() + 1);
        index_page.set_next(parent.view.as_addr().into());
        let page = self.update_node(parent.id, index_page.next(), index_page, guard)?;

        if page.len() as usize >= self.opts.index_delta_length {
            parent.view = page.into();
            let _ = self.consolidate_index_node(&parent, guard);
        }

        Ok(())
    }

    fn reconcile_split_root<'a: 'g, 'g>(
        &'a self,
        node: Node<'g>,
        split_index: IndexItem<'g>,
        guard: &'g Guard,
    ) -> Result<()> {
        assert_eq!(node.id, ROOT_INDEX.id);
        let root_addr = node.view.as_addr();

        // Builds a new root with the original root in the left and the split node in the right.
        let left_id = self.install_node(root_addr)?;
        let left_index = Index::new(left_id, node.view.ver());
        let root_data = [([].as_slice(), left_index), split_index];
        let mut root_iter = SliceIter::from(&root_data);
        let root_page = IndexPageBuilder::default().build_from_iter(&self.cache, &mut root_iter)?;

        self.update_node(node.id, root_addr, root_page, guard)
            .map_err(|err| {
                self.table.dealloc(left_id, guard);
                err
            })?;

        Ok(())
    }

    fn consolidate_node<'a: 'g, 'g>(&'a self, node: &Node<'g>, guard: &'g Guard) -> Result<()> {
        if node.view.is_leaf() {
            self.consolidate_data_node(node, guard)
        } else {
            self.consolidate_index_node(node, guard)
        }
    }

    fn consolidate_data_node<'a: 'g, 'g>(
        &'a self,
        node: &Node<'g>,
        guard: &'g Guard,
    ) -> Result<()> {
        let bump = Bump::new();
        let mut data_iter = self.consolidate_data_iter(node, &bump, guard)?;
        let mut data_page =
            DataPageBuilder::default().build_from_iter(&self.cache, &mut data_iter)?;
        data_page.set_ver(node.view.ver());
        if let Some(base) = data_iter.base() {
            data_page.set_len(base.len() + 1);
            data_page.set_next(base.into());
        }

        let old_addr = node.view.as_addr();
        let new_page = self
            .update_node(node.id, old_addr, data_page, guard)
            .map(|page| {
                self.dealloc_page_chain(old_addr, page.next(), guard);
                self.stats.succeeded.num_data_consolidates.inc();
                page
            })
            .map_err(|err| {
                self.stats.conflicted.num_data_consolidates.inc();
                err
            })?;

        if new_page.next() == 0 && new_page.size() >= self.opts.data_node_size {
            let _ = self.split_data_node(node.id, new_page, guard);
        }

        Ok(())
    }

    fn consolidate_data_iter<'a: 'g, 'b, 'g>(
        &'a self,
        node: &Node<'g>,
        bump: &'b Bump,
        guard: &'g Guard,
    ) -> Result<ConsolidateDataIter<'g, 'b>> {
        let mut size = 0;
        let mut base = None;
        let mut limit = None;
        let mut merger = MergingIterBuilder::with_len(node.view.len() as usize + 1);
        self.walk_node(node, guard, |page| {
            match page {
                TypedPageRef::Data(page) => {
                    if size < page.content_size() / 2 && limit.is_none() && merger.len() >= 2 {
                        base = Some(page.base());
                        return true;
                    }
                    size += page.content_size();
                    merger.add(bump.alloc(page.into()));
                }
                TypedPageRef::Split(page) => {
                    if limit == None {
                        let index = page.split_index();
                        limit = Some(index.0);
                    }
                }
                _ => unreachable!(),
            }
            false
        })?;

        let iter = DataNodeIter::new(merger.build(), limit);
        Ok(ConsolidateDataIter::new(iter, base, self.min_lsn.get()))
    }

    fn consolidate_index_node<'a: 'g, 'g>(
        &'a self,
        node: &Node<'g>,
        guard: &'g Guard,
    ) -> Result<()> {
        let bump = Bump::new();
        let mut index_iter = self.index_node_iter(node, &bump, guard)?;
        let mut index_page =
            IndexPageBuilder::default().build_from_iter(&self.cache, &mut index_iter)?;
        index_page.set_ver(node.view.ver());

        let old_addr = node.view.as_addr();
        let new_page = self
            .update_node(node.id, old_addr, index_page, guard)
            .map(|page| {
                self.dealloc_page_chain(old_addr, page.next(), guard);
                self.stats.succeeded.num_index_consolidates.inc();
                page
            })
            .map_err(|err| {
                self.stats.conflicted.num_index_consolidates.inc();
                err
            })?;

        if new_page.next() == 0 && new_page.size() >= self.opts.index_node_size {
            let _ = self.split_index_node(node.id, new_page, guard);
        }

        Ok(())
    }

    fn dealloc_tree<'a: 'g, 'g>(&'a self, id: u64, guard: &'g Guard) -> Result<()> {
        let addr = self.page_addr(id);
        let view = self.page_view(addr, guard).expect("the node must be valid");
        let node = Node {
            id,
            view,
            range: Range::default(),
        };
        if node.view.is_leaf() {
            self.dealloc_node(node.view.as_addr(), guard);
            return Ok(());
        }

        let bump = Bump::new();
        let mut index_iter = self.index_node_iter(&node, &bump, guard)?;
        index_iter.rewind();
        while let Some((_, index)) = index_iter.current() {
            self.dealloc_tree(index.id, guard)?;
            index_iter.next();
        }

        Ok(())
    }

    fn dealloc_node<'a: 'g, 'g>(&'a self, head: impl Into<u64>, guard: &'g Guard) {
        self.dealloc_page_chain(head, 0u64, guard);
    }

    fn dealloc_page_chain<'a: 'g, 'g>(
        &'a self,
        head: impl Into<u64>,
        until: impl Into<u64>,
        guard: &'g Guard,
    ) {
        let mut next = head.into();
        let until = until.into();
        let cache = self.cache.clone();
        guard.defer(move || unsafe {
            while next != until {
                if let PageAddr::Mem(addr) = next.into() {
                    if let Some(page) = PagePtr::new(addr as *mut u8) {
                        next = page.next();
                        cache.dealloc_page(page);
                        continue;
                    }
                }
                break;
            }
        });
    }
}

impl Drop for Tree {
    fn drop(&mut self) {
        let guard = unsafe { unprotected() };
        self.dealloc_tree(ROOT_INDEX.id, guard).unwrap();
    }
}

pub struct Iter {
    tree: Arc<Tree>,
    bump: Bump,
    guard: Guard,
    cursor: Option<Vec<u8>>,
}

impl Iter {
    pub fn new(tree: Arc<Tree>) -> Self {
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
        while let Some(mut iter) = self.next_iter()? {
            iter.rewind();
            while let Some(item) = iter.current() {
                f(item);
                iter.next();
            }
        }
        Ok(())
    }

    fn next_iter(&mut self) -> Result<Option<NodeIter<'_>>> {
        if let Some(cursor) = self.cursor.take() {
            self.bump.reset();
            self.guard.repin();
            let node = loop {
                // TODO: refactor this
                match self.tree.find_leaf(&cursor, &self.guard) {
                    Ok((node, _)) => break node,
                    Err(Error::Again) => continue,
                    Err(err) => return Err(err),
                }
            };
            self.cursor = node.range.end.map(|end| end.to_vec());
            let iter = self.tree.data_node_iter(&node, &self.bump, &self.guard)?;
            Ok(Some(NodeIter::new(iter)))
        } else {
            Ok(None)
        }
    }
}

struct NodeIter<'a> {
    iter: DataNodeIter<'a, 'a>,
    last: &'a [u8],
    current: Option<(&'a [u8], &'a [u8])>,
}

impl<'a> NodeIter<'a> {
    fn new(iter: DataNodeIter<'a, 'a>) -> Self {
        Self {
            iter,
            last: [].as_slice(),
            current: None,
        }
    }

    fn current(&self) -> Option<(&[u8], &[u8])> {
        self.current
    }

    fn rewind(&mut self) {
        self.iter.rewind();
        self.find_next();
    }

    fn next(&mut self) {
        self.iter.next();
        self.find_next();
    }

    fn find_next(&mut self) {
        while let Some((k, v)) = self.iter.current() {
            if self.last != k.raw {
                self.last = k.raw;
                if let Value::Put(value) = v {
                    self.current = Some((k.raw, value));
                    return;
                }
            }
            self.iter.next();
        }
        self.current = None;
    }
}

struct MinLsn(AtomicU64);

impl MinLsn {
    fn new() -> Self {
        Self(AtomicU64::new(0))
    }

    fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }

    fn set(&self, lsn: u64) {
        let mut min = self.0.load(Ordering::Relaxed);
        while min < lsn {
            match self
                .0
                .compare_exchange(min, lsn, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => break,
                Err(new) => min = new,
            }
        }
    }
}
