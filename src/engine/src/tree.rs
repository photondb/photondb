use crossbeam_epoch::{unprotected, Guard};

use crate::{
    BaseData, BaseIndex, DeltaData, DeltaIndex, PageBuf, PageCache, PageContent, PageHandle,
    PageId, PageIndex, PageRef, SplitNode,
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

        // Sets up an empty root page and an empty data page.
        let root_page = PageBuf::with_content(PageContent::BaseIndex(BaseIndex::new()));
        let (root_id, root_page) = cache.install(root_page, guard).unwrap();
        assert_eq!(root_id, PageId::zero());
        let base_page = PageBuf::with_content(PageContent::BaseData(BaseData::new()));
        let (base_id, base_page) = cache.install(base_page, guard).unwrap();
        let base_index = PageIndex {
            lowest: Vec::new(),
            highest: Vec::new(),
            handle: PageHandle {
                id: base_id,
                epoch: base_page.epoch(),
            },
        };
        let mut delta = DeltaIndex::new();
        delta.add(base_index);
        let delta_page = PageBuf::with_next(root_page, PageContent::DeltaIndex(delta));
        cache.update(root_id, root_page, delta_page, guard).unwrap();

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
                    Err((old_page, mut new_page)) => {
                        if old_page.epoch() == page.epoch() {
                            page = old_page;
                            new_page_opt = Some(new_page);
                        } else {
                            new_page.unlink();
                        }
                    }
                }
            }
        }
    }

    fn page<'a>(&self, pid: PageId, guard: &'a Guard) -> PageRef<'a> {
        let mut page = self.cache.get(pid, guard).unwrap();
        if page.len() >= self.opts.delta_chain_length {
            let new_page = if page.is_data() {
                self.consolidate_data(pid, page, self.opts.data_node_size, guard)
            } else {
                self.consolidate_index(pid, page, self.opts.index_node_size, guard)
            };
            if let Ok(new_page) = new_page {
                // println!(
                //     "consolidated page {:?} from {:?} into {:?}",
                //     pid, page, new_page
                // );
                page = new_page;
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
            } else if self.handle_pending_split(pid, page, parent, guard) {
                return None;
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
    ) -> bool {
        let split = match page.content() {
            PageContent::SplitData(split) => split,
            PageContent::SplitIndex(split) => split,
            _ => return false,
        };

        if let Some(parent) = parent {
            let left_index = PageIndex {
                lowest: split.lowest.clone(),
                highest: split.middle.clone(),
                handle: PageHandle {
                    id: pid,
                    epoch: page.epoch(),
                },
            };
            let right_index = PageIndex {
                lowest: split.middle.clone(),
                highest: split.highest.clone(),
                handle: split.right_page.clone(),
            };
            let mut delta = DeltaIndex::new();
            delta.add(left_index);
            delta.add(right_index);
            let new_page = PageBuf::with_next(parent.1, PageContent::DeltaIndex(delta));
            if let Err((_, mut new_page)) = self.cache.update(parent.0, parent.1, new_page, guard) {
                new_page.unlink();
            }
        } else {
            let left_id = self.cache.attach(page, guard).unwrap();
            let left_index = PageHandle {
                id: left_id,
                epoch: page.epoch(),
            };
            let mut base = BaseIndex::new();
            base.add(Vec::new(), left_index);
            base.add(split.middle.clone(), split.right_page.clone());
            let new_page = PageBuf::with_content(PageContent::BaseIndex(base));
            if let Ok(new_page) = self.cache.update(pid, page, new_page, guard) {
                println!("update {:?} {:?}", pid, new_page);
            } else {
                self.cache.detach(left_id, guard);
            }
        }

        true
    }

    fn lookup_data<'a>(
        &self,
        key: &[u8],
        page: PageRef<'a>,
        guard: &'a Guard,
    ) -> Result<Option<&'a [u8]>, ()> {
        let mut cursor = page;
        loop {
            // println!("lookup data {:?} {:?}", key, cursor);
            match cursor.content() {
                PageContent::BaseData(base) => return Ok(base.get(key)),
                PageContent::DeltaData(delta) => {
                    if let Some(value) = delta.get(key) {
                        return Ok(value);
                    }
                }
                PageContent::SplitData(split) => {
                    if let Some(index) = split.covers(key) {
                        cursor = self.page(index.id, guard);
                        if cursor.epoch() == index.epoch {
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
    ) -> Result<PageHandle, ()> {
        let mut cursor = page;
        loop {
            // println!("lookup index {:?} {:?}", key, cursor);
            match cursor.content() {
                PageContent::BaseIndex(base) => return base.get(key).ok_or(()),
                PageContent::DeltaIndex(delta) => {
                    if let Some(index) = delta.get(key) {
                        return Ok(index);
                    }
                }
                PageContent::SplitIndex(split) => {
                    if let Some(index) = split.covers(key) {
                        cursor = self.page(index.id, guard);
                        if cursor.epoch() == index.epoch {
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
    ) -> Result<PageRef<'a>, ()> {
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
                PageContent::MergeData(_) => return Err(()),
                _ => unreachable!(),
            }
            cursor = cursor.next().unwrap();
        };

        if new_base.size() < split_size {
            let new_page = PageBuf::new(new_header, PageContent::BaseData(new_base));
            return self
                .cache
                .replace(pid, page, new_page, guard)
                .map_err(|_| ());
        }

        if let Some(right_base) = new_base.split() {
            let lowest = new_base.lowest().to_owned();
            let middle = right_base.lowest().to_owned();
            let highest = right_base.highest().to_owned();
            let right_page = PageBuf::with_content(PageContent::BaseData(right_base));
            if let Ok((right_id, right_page)) = self.cache.install(right_page, guard) {
                let split = SplitNode {
                    lowest,
                    middle,
                    highest,
                    right_page: PageHandle {
                        id: right_id,
                        epoch: right_page.epoch(),
                    },
                };
                let new_header = new_header.into_next_epoch();
                let new_page = PageBuf::new(new_header, PageContent::BaseData(new_base));
                let left_page = PageBuf::with_next(new_page, PageContent::SplitData(split));
                match self.cache.replace(pid, page, left_page, guard) {
                    Ok(left_page) => return Ok(left_page),
                    Err(_) => {
                        self.cache.uninstall(right_id, guard);
                    }
                }
            }
        }

        Err(())
    }

    fn consolidate_index<'a>(
        &self,
        pid: PageId,
        page: PageRef<'a>,
        split_size: usize,
        guard: &'a Guard,
    ) -> Result<PageRef<'a>, ()> {
        let mut cursor = page;
        let mut acc_delta = DeltaIndex::new();
        let (mut new_base, new_header) = loop {
            match cursor.content() {
                PageContent::BaseIndex(base) => {
                    let mut new_base = base.clone();
                    new_base.apply(acc_delta);
                    let new_header = cursor.header().clone();
                    break (new_base, new_header);
                }
                PageContent::DeltaIndex(delta) => {
                    acc_delta.merge(delta.clone());
                }
                PageContent::SplitIndex(_) => {
                    // We split pages on consolidation, so we don't need to do anything here.
                }
                PageContent::MergeIndex(_) => return Err(()),
                _ => unreachable!(),
            }
            cursor = cursor.next().unwrap();
        };

        if new_base.size() < split_size {
            let new_page = PageBuf::new(new_header, PageContent::BaseIndex(new_base));
            return self
                .cache
                .replace(pid, page, new_page, guard)
                .map_err(|_| ());
        }

        if let Some(right_base) = new_base.split() {
            let lowest = new_base.lowest().to_owned();
            let middle = right_base.lowest().to_owned();
            let highest = right_base.highest().to_owned();
            let right_page = PageBuf::with_content(PageContent::BaseIndex(right_base));
            if let Ok((right_id, right_page)) = self.cache.install(right_page, guard) {
                let split = SplitNode {
                    lowest,
                    middle,
                    highest,
                    right_page: PageHandle {
                        id: right_id,
                        epoch: right_page.epoch(),
                    },
                };
                let new_header = new_header.into_next_epoch();
                let new_page = PageBuf::new(new_header, PageContent::BaseIndex(new_base));
                let left_page = PageBuf::with_next(new_page, PageContent::SplitIndex(split));
                match self.cache.replace(pid, page, left_page, guard) {
                    Ok(left_page) => return Ok(left_page),
                    Err(_) => {
                        self.cache.uninstall(right_id, guard);
                    }
                }
            }
        }

        Err(())
    }
}
