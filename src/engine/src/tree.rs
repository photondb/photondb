use crossbeam_epoch::{unprotected, Guard};

use crate::{
    BaseData, BaseIndex, DeltaData, DeltaIndex, PageBuf, PageCache, PageContent, PageId, PageIndex,
    PageRef, SplitNode,
};

pub struct Options {
    pub data_node_size: usize,
    pub index_node_size: usize,
    pub delta_chain_length: usize,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            data_node_size: 8192,
            index_node_size: 4096,
            delta_chain_length: 8,
        }
    }
}

pub struct Tree {
    opts: Options,
    cache: PageCache,
}

impl Tree {
    pub fn new(opts: Options) -> Self {
        let cache = PageCache::new();

        let guard = unsafe { unprotected() };
        let root_page = PageBuf::with_content(PageContent::BaseData(BaseData::new()));
        let (root_id, _) = cache.install(root_page, guard).unwrap();
        assert_eq!(root_id, PageId::zero());

        Self { opts, cache }
    }

    pub fn get<'a>(&self, key: &[u8], guard: &'a Guard) -> Option<&'a [u8]> {
        loop {
            let (_, page) = self.search(key, guard);
            if let Ok(value) = self.lookup_data(key, page, guard) {
                return value;
            }
        }
    }

    pub fn write(&self, key: &[u8], value: Option<&[u8]>, guard: &Guard) {
        loop {
            let (pid, mut page) = self.search(key, guard);
            let mut delta = DeltaData::new();
            delta.add(key.to_owned(), value.map(|v| v.to_owned()));
            let mut new_page_opt = Some(PageBuf::with_content(PageContent::DeltaData(delta)));
            while let Some(mut new_page) = new_page_opt.take() {
                new_page.link(page);
                match self.cache.update(pid, page, new_page, guard) {
                    Ok(_) => return,
                    Err((old_page, new_page)) => {
                        if old_page.epoch() == page.epoch() {
                            page = old_page;
                            new_page_opt = Some(new_page);
                        }
                    }
                }
            }
        }
    }

    fn page<'a>(&self, pid: PageId, guard: &'a Guard) -> PageRef<'a> {
        let mut page = self.cache.get(pid, guard).unwrap();
        if page.len() >= self.opts.delta_chain_length {
            page = if page.is_data() {
                self.consolidate_data(pid, page, self.opts.data_node_size, guard)
            } else {
                self.consolidate_index(pid, page, self.opts.index_node_size, guard)
            }
        }
        page
    }

    fn search<'a>(&self, key: &[u8], guard: &'a Guard) -> (PageId, PageRef<'a>) {
        loop {
            if let Some(result) = self.search_inner(key, guard) {
                return result;
            }
        }
    }

    fn search_inner<'a>(&self, key: &[u8], guard: &'a Guard) -> Option<(PageId, PageRef<'a>)> {
        let mut pid = PageId::zero();
        let mut epoch = None;
        let mut parent = None;
        loop {
            let page = self.page(pid, guard);
            if let Some(epoch) = epoch {
                if page.epoch() != epoch {
                    self.handle_pending_split(pid, page, parent, guard);
                    return None;
                }
            }
            if page.is_data() {
                return Some((pid, page));
            }
            if let Ok(index) = self.lookup_index(key, page, guard) {
                parent = Some((pid, page));
                pid = index.id;
                epoch = Some(index.epoch);
            } else {
                return None;
            }
        }
    }

    fn handle_pending_split<'a>(
        &self,
        pid: PageId,
        page: PageRef<'a>,
        parent: Option<(PageId, PageRef<'a>)>,
        guard: &'a Guard,
    ) {
        let split = match page.content() {
            PageContent::SplitData(split) => split,
            PageContent::SplitIndex(split) => split,
            _ => return,
        };

        if let Some((id, page)) = parent {
            let delta = DeltaIndex::from_split(split);
            let new_page = PageBuf::with_next(page, PageContent::DeltaIndex(delta));
            let _ = self.cache.update(id, page, new_page, guard);
        } else {
            let mut base = BaseIndex::new();
            let left_index = PageIndex {
                id: pid,
                epoch: page.epoch(),
            };
            base.add(Vec::new(), left_index);
            base.add(split.lowest.clone(), split.right_page.clone());
            let new_page = PageBuf::with_content(PageContent::BaseIndex(base));
            let _ = self.cache.move_and_replace(pid, page, new_page, guard);
        }
    }

    fn lookup_data<'a>(
        &self,
        key: &[u8],
        page: PageRef<'a>,
        guard: &'a Guard,
    ) -> Result<Option<&'a [u8]>, ()> {
        let mut cursor = page;
        loop {
            match cursor.content() {
                PageContent::BaseData(base) => return Ok(base.get(key)),
                PageContent::DeltaData(delta) => {
                    if let Some(value) = delta.get(key) {
                        return Ok(value);
                    }
                }
                PageContent::SplitData(split) => {
                    if key >= &split.lowest {
                        cursor = self.page(split.right_page.id, guard);
                        if cursor.epoch() == split.right_page.epoch {
                            continue;
                        } else {
                            return Err(());
                        }
                    }
                }
                PageContent::MergeData(merge) => {
                    if key >= &merge.lowest {
                        cursor = merge.right_page.as_ref();
                    }
                }
                PageContent::RemoveData => return Err(()),
                _ => unreachable!(),
            }
            cursor = cursor.next().unwrap();
        }
    }

    fn lookup_index<'a>(
        &self,
        key: &[u8],
        page: PageRef<'a>,
        guard: &'a Guard,
    ) -> Result<PageIndex, ()> {
        let mut cursor = page;
        loop {
            match cursor.content() {
                PageContent::BaseIndex(base) => return base.get(key).ok_or(()),
                PageContent::DeltaIndex(delta) => {
                    if let Some(index) = delta.covers(key) {
                        return Ok(index);
                    }
                }
                PageContent::SplitIndex(split) => {
                    if key >= &split.lowest {
                        cursor = self.page(split.right_page.id, guard);
                        if cursor.epoch() == split.right_page.epoch {
                            continue;
                        } else {
                            return Err(());
                        }
                    }
                }
                PageContent::MergeIndex(merge) => {
                    if key >= &merge.lowest {
                        cursor = merge.right_page.as_ref();
                    }
                }
                PageContent::RemoveIndex => return Err(()),
                _ => unreachable!(),
            }
            cursor = cursor.next().unwrap();
        }
    }

    fn consolidate_data<'a>(
        &self,
        pid: PageId,
        page: PageRef<'a>,
        split_size: usize,
        guard: &'a Guard,
    ) -> PageRef<'a> {
        let mut cursor = page;
        let mut acc_delta = DeltaData::new();
        let (mut new_base, new_header) = loop {
            match cursor.content() {
                PageContent::BaseData(base) => {
                    let mut new_base = base.clone();
                    new_base.apply(acc_delta);
                    let new_header = cursor.header().clone();
                    break (new_base, new_header);
                }
                PageContent::DeltaData(delta) => {
                    acc_delta.merge(delta.clone());
                }
                PageContent::SplitData(_) => {
                    // We split pages on consolidation, so we don't need to do anything here.
                }
                PageContent::MergeData(_) => return page,
                _ => unreachable!(),
            }
            cursor = cursor.next().unwrap();
        };

        if new_base.size() < split_size {
            let new_page = PageBuf::new(new_header, PageContent::BaseData(new_base));
            return self
                .cache
                .replace(pid, page, new_page, guard)
                .unwrap_or(page);
        }

        if let Some(right_base) = new_base.split() {
            let right_lowest = right_base.lowest().to_owned();
            let right_highest = right_base.highest().to_owned();
            let right_page = PageBuf::with_content(PageContent::BaseData(right_base));
            if let Ok((right_id, right_page)) = self.cache.install(right_page, guard) {
                let split = SplitNode {
                    lowest: right_lowest,
                    highest: right_highest,
                    right_page: PageIndex {
                        id: right_id,
                        epoch: right_page.epoch(),
                    },
                };
                let new_header = new_header.into_next_epoch();
                let new_page = PageBuf::new(new_header, PageContent::BaseData(new_base));
                let left_page = PageBuf::with_next(new_page, PageContent::SplitData(split));
                match self.cache.replace(pid, page, left_page, guard) {
                    Ok(left_page) => return left_page,
                    Err(_) => {
                        self.cache.uninstall(right_id, guard);
                    }
                }
            }
        }

        page
    }

    fn consolidate_index<'a>(
        &self,
        pid: PageId,
        page: PageRef<'a>,
        split_size: usize,
        guard: &'a Guard,
    ) -> PageRef<'a> {
        let mut cursor = page;
        let mut acc_delta = Vec::new();
        let (mut new_base, new_header) = loop {
            match cursor.content() {
                PageContent::BaseIndex(base) => {
                    let mut new_base = base.clone();
                    new_base.apply(acc_delta);
                    let new_header = cursor.header().clone();
                    break (new_base, new_header);
                }
                PageContent::DeltaIndex(delta) => {
                    acc_delta.push(delta.clone());
                }
                PageContent::SplitIndex(_) => {
                    // We split pages on consolidation, so we don't need to do anything here.
                }
                PageContent::MergeIndex(_) => return page,
                _ => unreachable!(),
            }
            cursor = cursor.next().unwrap();
        };

        if new_base.size() < split_size {
            let new_page = PageBuf::new(new_header, PageContent::BaseIndex(new_base));
            return self
                .cache
                .replace(pid, page, new_page, guard)
                .unwrap_or(page);
        }

        if let Some(right_base) = new_base.split() {
            let right_lowest = right_base.lowest().to_owned();
            let right_highest = right_base.highest().to_owned();
            let right_page = PageBuf::with_content(PageContent::BaseIndex(right_base));
            if let Ok((right_id, right_page)) = self.cache.install(right_page, guard) {
                let split = SplitNode {
                    lowest: right_lowest,
                    highest: right_highest,
                    right_page: PageIndex {
                        id: right_id,
                        epoch: right_page.epoch(),
                    },
                };
                let new_header = new_header.into_next_epoch();
                let new_page = PageBuf::new(new_header, PageContent::BaseIndex(new_base));
                let left_page = PageBuf::with_next(new_page, PageContent::SplitIndex(split));
                match self.cache.replace(pid, page, left_page, guard) {
                    Ok(left_page) => return left_page,
                    Err(_) => {
                        self.cache.uninstall(right_id, guard);
                    }
                }
            }
        }

        page
    }
}
