use std::collections::VecDeque;

use log::trace;

use super::{
    page::*,
    pagecache::{PageAddr, PageCache, PageView},
    pagestore::PageStore,
    pagetable::PageTable,
    Error, Ghost, Options, Result,
};

const ROOT_INDEX: Index = Index::with_id(PageTable::MIN);
const NULL_INDEX: Index = Index::with_id(PageTable::NAN);

#[derive(Copy, Clone)]
pub struct Node<'a> {
    pub id: u64,
    pub view: PageView<'a>,
}

impl<'a> Node<'a> {
    pub fn new(id: u64, view: PageView<'a>) -> Self {
        Self { id, view }
    }
}

pub struct DataNodeIter<'a> {
    iter: MergingIter<DataPageIter<'a>>,
    highest: Option<&'a [u8]>,
}

impl<'a> DataNodeIter<'a> {
    pub fn new(iter: MergingIter<DataPageIter<'a>>, highest: Option<&'a [u8]>) -> Self {
        Self { iter, highest }
    }
}

impl<'a> ForwardIter for DataNodeIter<'a> {
    type Key = Key<'a>;
    type Value = Value<'a>;

    fn last(&self) -> Option<&(Self::Key, Self::Value)> {
        self.iter.last()
    }

    fn next(&mut self) -> Option<&(Self::Key, Self::Value)> {
        if let Some((key, value)) = self.iter.next() {
            if let Some(highest) = self.highest {
                if key.raw >= highest {
                    self.iter.skip_all();
                }
            }
        }
        self.iter.last()
    }
}

impl<'a> SeekableIter for DataNodeIter<'a> {
    fn seek<T>(&mut self, target: &T)
    where
        T: Comparable<Self::Key>,
    {
        self.iter.seek(target);
    }
}

impl<'a> RewindableIter for DataNodeIter<'a> {
    fn rewind(&mut self) {
        self.iter.rewind();
    }
}

pub struct IndexNodeIter<'a> {
    iter: MergingIter<IndexPageIter<'a>>,
    highest: Option<&'a [u8]>,
}

impl<'a> IndexNodeIter<'a> {
    pub fn new(iter: MergingIter<IndexPageIter<'a>>, highest: Option<&'a [u8]>) -> Self {
        Self { iter, highest }
    }
}

impl<'a> ForwardIter for IndexNodeIter<'a> {
    type Key = &'a [u8];
    type Value = Index;

    fn last(&self) -> Option<&(Self::Key, Self::Value)> {
        self.iter.last()
    }

    fn next(&mut self) -> Option<&(Self::Key, Self::Value)> {
        let last_index = self.iter.last().map(|(_, i)| i.clone());
        while let Some((lowest, index)) = self.iter.next() {
            if index == &NULL_INDEX {
                continue;
            }
            if let Some(highest) = self.highest {
                if lowest >= &highest {
                    self.iter.skip_all();
                    break;
                }
            }
            if let Some(last_index) = last_index {
                if index.id == last_index.id {
                    assert!(index.ver < last_index.ver);
                    continue;
                }
            }
            break;
        }
        self.iter.last()
    }
}

impl<'a> SeekableIter for IndexNodeIter<'a> {
    fn seek<T>(&mut self, target: &T)
    where
        T: Comparable<Self::Key>,
    {
        self.iter.seek(target);
    }
}

impl<'a> RewindableIter for IndexNodeIter<'a> {
    fn rewind(&mut self) {
        self.iter.rewind();
    }
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

    pub async fn get<'a: 'g, 'g>(
        &'a self,
        key: &[u8],
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

    async fn try_get<'a: 'g, 'g>(
        &'a self,
        key: Key<'_>,
        ghost: &'g Ghost,
    ) -> Result<Option<&'g [u8]>> {
        let node = self.find_data_node(key.raw, ghost).await?;
        self.lookup_value(key, node, ghost).await
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

    async fn try_update<'g>(&self, key: &[u8], mut page: PagePtr, ghost: &'g Ghost) -> Result<()> {
        let mut node = self.find_data_node(key, ghost).await?;
        loop {
            page.set_ver(node.view.ver());
            page.set_rank(node.view.rank() + 1);
            page.set_next(node.view.as_addr().into());
            match self.table.cas(node.id, page.next(), page.into()) {
                Ok(_) => {
                    if page.rank() as usize >= self.opts.data_delta_length {
                        node.view = page.into();
                        let _ = self.consolidate_data_node(node, ghost).await;
                    }
                    return Ok(());
                }
                Err(addr) => {
                    if let Some(view) = self.page_view(addr.into(), ghost) {
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

    pub async fn trace<'g>(&self, ghost: &'g Ghost) -> Result<()> {
        let mut queue = VecDeque::from([ROOT_INDEX]);
        while let Some(index) = queue.pop_front() {
            let node = self.node(index.id, ghost).unwrap();
            if node.view.is_data() {
                trace!("data node {:?} view {:?}", index, node.view);
                let mut iter = self.iter_data_node(node, ghost).await?;
                while let Some(item) = iter.next() {
                    trace!("- {:?}", item);
                }
            } else {
                trace!("index node {:?} view {:?}", index, node.view);
                let mut iter = self.iter_index_node(node, ghost).await?;
                while let Some(item) = iter.next() {
                    trace!("- {:?}", item);
                    queue.push_back(item.1);
                }
            }
        }
        Ok(())
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
                page.map(|page| PageView::Mem(page))
            }
            PageAddr::Disk(addr) => self
                .store
                .page_info(addr)
                .map(|info| PageView::Disk(info, addr)),
        }
    }

    async fn load_page_with_view<'a: 'g, 'g>(
        &'a self,
        _: u64,
        view: PageView<'g>,
        _: &'g Ghost,
    ) -> Result<PageRef<'g>> {
        match view {
            PageView::Mem(page) => Ok(page),
            PageView::Disk(_, _) => {
                // self.swapin_page(id, addr).await,
                todo!()
            }
        }
    }

    async fn load_page_with_addr<'a: 'g, 'g>(
        &'a self,
        _: u64,
        addr: PageAddr,
        _: &'g Ghost,
    ) -> Result<Option<PageRef<'g>>> {
        match addr {
            PageAddr::Mem(addr) => Ok(unsafe { PageRef::new(addr as *mut u8) }),
            PageAddr::Disk(_) => {
                // self.swapin_page(id, addr).await,
                todo!()
            }
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

    async fn traverse_node<'a: 'g, 'g, F>(
        &'a self,
        node: Node<'g>,
        ghost: &'g Ghost,
        mut f: F,
    ) -> Result<()>
    where
        F: FnMut(PageRef<'g>) -> bool,
    {
        let mut page = self.load_page_with_view(node.id, node.view, ghost).await?;
        loop {
            if f(page) {
                break;
            }
            let next = page.next().into();
            match self.load_page_with_addr(node.id, next, ghost).await? {
                Some(next) => page = next,
                None => break,
            }
        }
        Ok(())
    }

    async fn iter_data_node<'a: 'g, 'g>(
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
                _ => {}
            }
            false
        })
        .await?;
        Ok(DataNodeIter::new(merger.build(), highest))
    }

    async fn iter_index_node<'a: 'g, 'g>(
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
                _ => {}
            }
            false
        })
        .await?;
        Ok(IndexNodeIter::new(merger.build(), highest))
    }

    async fn lookup_value<'a: 'g, 'g>(
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
        })
        .await?;
        Ok(value)
    }

    async fn lookup_index<'a: 'g, 'g>(
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
        })
        .await?;
        Ok(value)
    }

    async fn lookup_index_range<'a: 'g, 'g>(
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
                    left_index = Some(left);
                    right_index = right;
                    return true;
                }
            }
            false
        })
        .await?;
        Ok((left_index, right_index))
    }

    async fn find_data_node<'a: 'g, 'g>(
        &'a self,
        key: &[u8],
        ghost: &'g Ghost,
    ) -> Result<Node<'g>> {
        let mut cursor = ROOT_INDEX;
        let mut parent = None;
        loop {
            // Our access pattern guarantees that the node must exists.
            trace!("access node {:?} for {:?}", cursor, key);
            let node = self.node(cursor.id, ghost).unwrap();
            if node.view.ver() != cursor.ver {
                self.reconcile_node(node, parent, ghost).await?;
                return Err(Error::Again);
            }
            if node.view.is_data() {
                return Ok(node);
            }
            // Our access pattern guarantees that the index must exists.
            cursor = self.lookup_index(key, node, ghost).await?.unwrap();
            parent = Some(node);
        }
    }

    async fn reconcile_node<'a: 'g, 'g>(
        &'a self,
        node: Node<'g>,
        parent: Option<Node<'g>>,
        ghost: &'g Ghost,
    ) -> Result<()> {
        let page = self.load_page_with_view(node.id, node.view, ghost).await?;
        if page.kind() == PageKind::Split {
            let split = SplitPageRef::from(page);
            if let Some(node) = parent {
                self.reconcile_split_node(node, split, ghost).await?;
            } else {
                self.reconcile_split_root(node, split, ghost)?;
            }
        }
        Ok(())
    }

    async fn reconcile_split_node<'a: 'g, 'g>(
        &'a self,
        node: Node<'g>,
        split: SplitPageRef<'g>,
        ghost: &'g Ghost,
    ) -> Result<PageRef<'g>> {
        let split_index = split.get();

        let (left_index, right_index) = self.lookup_index_range(split_index.0, node, ghost).await?;
        let mut left_index = left_index.unwrap();
        left_index.1.ver = split.ver();

        let mut delta_page = if let Some(right_index) = right_index {
            assert!(right_index.0 > split_index.0);
            let delta_data = [left_index, split_index, (right_index.0, NULL_INDEX)];
            trace!("reconcile split node {} {:?}", node.id, delta_data);
            let mut delta_iter = SliceIter::from(&delta_data);
            IndexPageBuilder::default().build_from_iter(&self.cache, &mut delta_iter)?
        } else {
            let delta_data = [left_index, split_index];
            trace!("reconcile split node {} {:?}", node.id, delta_data);
            let mut delta_iter = SliceIter::from(&delta_data);
            IndexPageBuilder::default().build_from_iter(&self.cache, &mut delta_iter)?
        };

        let node_addr = node.view.as_addr();
        delta_page.set_ver(node.view.ver());
        delta_page.set_rank(node.view.rank() + 1);
        delta_page.set_next(node_addr.into());
        let mut new_page = self.update_node(node.id, node_addr, delta_page, ghost)?;
        if new_page.rank() as usize >= self.opts.index_delta_length {
            let new_node = Node::new(node.id, new_page.into());
            new_page = self.consolidate_index_node(new_node, ghost).await?;
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
        trace!("reconcile split root {:?}", root_data);
        let root_page = IndexPageBuilder::default().build_from_iter(&self.cache, &mut root_iter)?;

        // Replaces the original root with the new root.
        self.update_node(node.id, root_addr, root_page, ghost)
            .map_err(|err| {
                self.table.dealloc(left_id, ghost.guard());
                err
            })
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
        split()
            .map(|page| {
                trace!(
                    "split node {} at {:?} to node {}",
                    left_id,
                    split_key,
                    right_id
                );
                page
            })
            .map_err(|err| {
                self.table.dealloc(right_id, ghost.guard());
                err
            })
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
            self.install_split(id, page, sep, right_page.into(), ghost)
                .map_err(|err| unsafe {
                    self.cache.dealloc(right_page);
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
        ghost: &'g Ghost,
    ) -> Result<PageRef<'g>> {
        let index_page = IndexPageRef::from(page);
        if let Some((sep, mut iter)) = index_page.split() {
            let right_page = IndexPageBuilder::default().build_from_iter(&self.cache, &mut iter)?;
            self.install_split(id, page, sep, right_page.into(), ghost)
                .map_err(|err| unsafe {
                    self.cache.dealloc(right_page);
                    err
                })
        } else {
            Ok(page)
        }
    }

    async fn consolidate_data_node<'a: 'g, 'g>(
        &'a self,
        node: Node<'g>,
        ghost: &'g Ghost,
    ) -> Result<PageRef<'g>> {
        let mut iter = self.iter_data_node(node, ghost).await?;
        let mut page = DataPageBuilder::default().build_from_iter(&self.cache, &mut iter)?;
        page.set_ver(node.view.ver());
        let mut new_page = self.replace_node(node.id, node.view.as_addr(), page, ghost)?;
        trace!("consolidated data node {}", node.id);
        if new_page.size() >= self.opts.data_node_size {
            new_page = self.split_data_node(node.id, new_page, ghost)?;
        }
        Ok(new_page)
    }

    async fn consolidate_index_node<'a: 'g, 'g>(
        &'a self,
        node: Node<'g>,
        ghost: &'g Ghost,
    ) -> Result<PageRef<'g>> {
        let mut iter = self.iter_index_node(node, ghost).await?;
        let mut page = IndexPageBuilder::default().build_from_iter(&self.cache, &mut iter)?;
        page.set_ver(node.view.ver());
        let mut new_page = self.replace_node(node.id, node.view.as_addr(), page, ghost)?;
        trace!("consolidated index node {}", node.id);
        if new_page.size() >= self.opts.index_node_size {
            new_page = self.split_index_node(node.id, new_page, ghost)?;
        }
        Ok(new_page)
    }
}
