use std::thread;

use crossbeam_epoch::{unprotected, Guard};

use crate::{
    BaseData, BaseIndex, DeltaData, DeltaIndex, MergeNode, PageBuf, PageCache, PageContent,
    PageHandle, PageId, PageIndex, PageRef, SplitNode,
};

pub struct Options {
    pub node_split_size: usize,
    pub node_merge_size: usize,
    pub delta_chain_length: usize,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            node_split_size: 1024 * 16,
            node_merge_size: 1024,
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
        let root_page = PageBuf::with(PageContent::BaseIndex(BaseIndex::new()));
        let (root_id, root_page) = cache.install(root_page, guard).unwrap();
        assert_eq!(root_id, PageId::zero());
        let base_page = PageBuf::with(PageContent::BaseData(BaseData::new()));
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
            let mut new_page_opt = Some(PageBuf::with(PageContent::DeltaData(delta)));
            while let Some(mut new_page) = new_page_opt.take() {
                new_page.link(page);
                match self.cache.update(pid, page, new_page, guard) {
                    Ok(_) => return,
                    Err((now_page, mut new_page)) => {
                        if now_page.epoch() == page.epoch() {
                            page = now_page;
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
        let page = self.cache.get(pid, guard).unwrap();
        match self.maybe_consolidate(pid, page, guard) {
            Ok(new_page) => {
                /*
                println!(
                    "[{:?}] consolidated {:?} from {:?} to {:?}",
                    thread::current().id(),
                    pid,
                    page,
                    new_page
                );
                */
                new_page
            }
            Err(page) => page,
        }
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
            let mut page = self.page(pid, guard);
            match self.maybe_smo(pid, page, parent, guard) {
                Ok(_) => return None,
                Err(now_page) => page = now_page,
            }
            if let Some(epoch) = epoch {
                if page.epoch() != epoch {
                    self.handle_pending_smo(pid, page, parent, guard);
                    return None;
                }
            } else if self.handle_pending_smo(pid, page, parent, guard) {
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

    fn maybe_smo<'a>(
        &self,
        pid: PageId,
        page: PageRef<'a>,
        parent: Option<(PageId, PageRef<'a>)>,
        guard: &'a Guard,
    ) -> Result<(), PageRef<'a>> {
        let new_page = if page.is_data() {
            self.maybe_split_data(pid, page, guard)?
        } else {
            self.maybe_split_index(pid, page, guard)?
        };
        /*
        println!(
            "[{:?}] split {:?} from {:?} to {:?}",
            thread::current().id(),
            pid,
            page,
            new_page
        );
        */
        self.handle_pending_smo(pid, new_page, parent, guard);
        Ok(())
    }

    fn handle_pending_smo<'a>(
        &self,
        pid: PageId,
        page: PageRef<'a>,
        parent: Option<(PageId, PageRef<'a>)>,
        guard: &'a Guard,
    ) -> bool {
        match page.content() {
            PageContent::SplitData(split) => {
                self.handle_pending_split(pid, page, parent, split, guard);
            }
            PageContent::SplitIndex(split) => {
                self.handle_pending_split(pid, page, parent, split, guard);
            }
            /*
            PageContent::MergeData(merge) => {
                self.handle_pending_merge(pid, page, parent, merge, guard)
            }
            PageContent::MergeIndex(merge) => {
                self.handle_pending_merge(pid, page, parent, merge, guard)
            }
            PageContent::RemoveData | PageContent::RemoveIndex => {
                self.handle_pending_remove(pid, page, parent, guard)
            }
            */
            _ => return false,
        }
        true
    }

    fn handle_pending_split<'a>(
        &self,
        pid: PageId,
        page: PageRef<'a>,
        parent: Option<(PageId, PageRef<'a>)>,
        split: &SplitNode,
        guard: &'a Guard,
    ) {
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
            let new_page = PageBuf::with(PageContent::BaseIndex(base));
            if let Err(_) = self.cache.update(pid, page, new_page, guard) {
                self.cache.detach(left_id, guard);
            }
        }
    }

    fn maybe_split_data<'a>(
        &self,
        pid: PageId,
        page: PageRef<'a>,
        guard: &'a Guard,
    ) -> Result<PageRef<'a>, PageRef<'a>> {
        if let PageContent::BaseData(left_base) = page.content() {
            if left_base.size() >= self.opts.node_split_size {
                if let Some(right_base) = left_base.split() {
                    let lowest = left_base.lowest().to_owned();
                    let middle = right_base.lowest().to_owned();
                    let highest = right_base.highest().to_owned();
                    let right_page = PageBuf::with(PageContent::BaseData(right_base));
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
                        let left_page =
                            PageBuf::with_next_epoch(page, PageContent::SplitData(split));
                        match self.cache.update(pid, page, left_page, guard) {
                            Ok(new_page) => {
                                return Ok(new_page);
                            }
                            Err((now_page, mut new_page)) => {
                                new_page.unlink();
                                self.cache.uninstall(right_id, guard);
                                return Err(now_page);
                            }
                        }
                    }
                }
            }
        }
        Err(page)
    }

    fn maybe_split_index<'a>(
        &self,
        pid: PageId,
        page: PageRef<'a>,
        guard: &'a Guard,
    ) -> Result<PageRef<'a>, PageRef<'a>> {
        if let PageContent::BaseIndex(left_base) = page.content() {
            if left_base.size() >= self.opts.node_split_size {
                if let Some(right_base) = left_base.split() {
                    let lowest = left_base.lowest().to_owned();
                    let middle = right_base.lowest().to_owned();
                    let highest = right_base.highest().to_owned();
                    let right_page = PageBuf::with(PageContent::BaseIndex(right_base));
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
                        let left_page =
                            PageBuf::with_next_epoch(page, PageContent::SplitIndex(split));
                        match self.cache.update(pid, page, left_page, guard) {
                            Ok(new_page) => {
                                return Ok(new_page);
                            }
                            Err((now_page, mut new_page)) => {
                                new_page.unlink();
                                self.cache.uninstall(right_id, guard);
                                return Err(now_page);
                            }
                        }
                    }
                }
            }
        }
        Err(page)
    }

    fn maybe_consolidate<'a>(
        &self,
        pid: PageId,
        page: PageRef<'a>,
        guard: &'a Guard,
    ) -> Result<PageRef<'a>, PageRef<'a>> {
        if page.len() >= self.opts.delta_chain_length {
            if page.is_data() {
                self.maybe_consolidate_data(pid, page, guard)
            } else {
                self.maybe_consolidate_index(pid, page, guard)
            }
        } else {
            Err(page)
        }
    }

    fn maybe_consolidate_data<'a>(
        &self,
        pid: PageId,
        page: PageRef<'a>,
        guard: &'a Guard,
    ) -> Result<PageRef<'a>, PageRef<'a>> {
        let mut cursor = page;
        let mut acc_delta = DeltaData::new();
        let mut split_opt: Option<&'a SplitNode> = None;
        loop {
            match cursor.content() {
                PageContent::BaseData(base) => {
                    let mut new_base = base.clone();
                    new_base.apply(acc_delta);
                    if let Some(split) = split_opt {
                        new_base.retain(&split.lowest, &split.middle);
                    }
                    let new_page =
                        PageBuf::with_epoch(page.epoch(), PageContent::BaseData(new_base));
                    return self
                        .cache
                        .replace(pid, page, new_page, guard)
                        .map_err(|(now_page, _)| now_page);
                }
                PageContent::DeltaData(delta) => {
                    acc_delta.merge(delta.clone());
                }
                PageContent::SplitData(split) => {
                    split_opt.get_or_insert(split);
                }
                PageContent::MergeData(_) => todo!(),
                _ => unreachable!(),
            }
            cursor = cursor.next().unwrap();
        }
    }

    fn maybe_consolidate_index<'a>(
        &self,
        pid: PageId,
        page: PageRef<'a>,
        guard: &'a Guard,
    ) -> Result<PageRef<'a>, PageRef<'a>> {
        let mut cursor = page;
        let mut acc_delta = DeltaIndex::new();
        let mut split_opt: Option<&'a SplitNode> = None;
        loop {
            match cursor.content() {
                PageContent::BaseIndex(base) => {
                    let mut new_base = base.clone();
                    new_base.apply(acc_delta);
                    if let Some(split) = split_opt {
                        new_base.retain(&split.lowest, &split.middle);
                    }
                    let new_page =
                        PageBuf::with_epoch(page.epoch(), PageContent::BaseIndex(new_base));
                    return self
                        .cache
                        .replace(pid, page, new_page, guard)
                        .map_err(|(now_page, _)| now_page);
                }
                PageContent::DeltaIndex(delta) => {
                    acc_delta.merge(delta.clone());
                }
                PageContent::SplitIndex(split) => {
                    split_opt.get_or_insert(split);
                }
                PageContent::MergeIndex(_) => todo!(),
                _ => unreachable!(),
            }
            cursor = cursor.next().unwrap();
        }
    }
}
