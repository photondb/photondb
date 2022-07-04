use crossbeam_epoch::{unprotected, Guard};

use crate::{
    BaseData, DeltaData, PageBuf, PageCache, PageContent, PageHeader, PageId, PageIndex, PageRef,
    SplitNode,
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
            let (id, page) = self.search(key, guard);
            if let Ok(value) = self.lookup_data(id, page, key, guard) {
                return value;
            }
        }
    }

    pub fn write(&self, key: &[u8], value: Option<&[u8]>, guard: &Guard) {
        loop {
            let (id, mut page) = self.search(key, guard);
            let mut delta = DeltaData::new();
            delta.add(key.to_owned(), value.map(|v| v.to_owned()));
            let mut new_page_opt = Some(PageBuf::with_content(PageContent::DeltaData(delta)));
            while let Some(mut new_page) = new_page_opt.take() {
                new_page.link(page);
                match self.cache.update(id, page, new_page, guard) {
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

    fn page<'a>(&self, id: PageId, guard: &'a Guard) -> PageRef<'a> {
        self.cache.get(id, guard).unwrap()
    }

    fn search<'a>(&self, key: &[u8], guard: &'a Guard) -> (PageId, PageRef<'a>) {
        let mut id = PageId::zero();
        let mut page = self.page(id, guard);
        let mut parent = None;
        loop {
            if page.content().is_data() {
                if page.len() >= self.opts.delta_chain_length {
                    page = self.consolidate_data(id, page, self.opts.data_node_size, guard);
                }
                return (id, page);
            } else {
                todo!()
            }
            parent = Some((id, page));
        }
    }

    fn lookup_data<'a>(
        &self,
        id: PageId,
        page: PageRef<'a>,
        key: &[u8],
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

    fn consolidate_data<'a>(
        &self,
        id: PageId,
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
                PageContent::SplitData(_) | PageContent::MergeData(_) => return page,
                _ => unreachable!(),
            }
            cursor = cursor.next().unwrap();
        };

        if new_base.size() < split_size {
            let new_page = PageBuf::new(new_header, PageContent::BaseData(new_base));
            return self
                .cache
                .replace(id, page, new_page, guard)
                .unwrap_or(page);
        }

        if let Some(right_base) = new_base.split() {
            let right_page = PageBuf::with_content(PageContent::BaseData(right_base));
            if let Ok((right_id, right_page)) = self.cache.install(right_page, guard) {
                let split = SplitNode {
                    lowest: new_base.highest().to_vec(),
                    right_page: PageIndex {
                        id: right_id,
                        epoch: right_page.epoch(),
                    },
                };
                let new_header = new_header.into_next_epoch();
                let new_page = PageBuf::new(new_header, PageContent::BaseData(new_base));
                let left_page = PageBuf::with_next(new_page, PageContent::SplitData(split));
                match self.cache.replace(id, page, left_page, guard) {
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
