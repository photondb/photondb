use crossbeam_epoch::{unprotected, Guard};

use crate::{
    BaseData, DeltaData, OwnedPage, PageCache, PageContent, PageHeader, PageId, SharedPage,
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

        let data = PageContent::BaseData(BaseData::new());
        let root = OwnedPage::with_content(data);
        let guard = unsafe { unprotected() };
        let (root_id, _) = cache.install(root, guard).unwrap();
        assert_eq!(root_id, PageId::zero());

        Self { opts, cache }
    }

    pub fn get<'a>(&self, key: &[u8], guard: &'a Guard) -> Option<&'a [u8]> {
        let (id, page) = self.search(key, guard);
        self.lookup_data(id, page, key, guard)
    }

    pub fn write(&self, key: &[u8], value: Option<&[u8]>, guard: &Guard) {
        loop {
            let (id, mut page) = self.search(key, guard);
            let mut delta = DeltaData::new();
            delta.add(key.to_owned(), value.map(|v| v.to_owned()));
            let content = PageContent::DeltaData(delta);
            let mut new_page_opt = Some(OwnedPage::with_content(content));
            while let Some(mut new_page) = new_page_opt.take() {
                new_page.link(page);
                match self.cache.update(id, page, new_page, guard) {
                    Ok(_) => return,
                    Err((old, new)) => {
                        if old.header().covers(key) {
                            page = old;
                            new_page_opt = Some(new);
                        }
                    }
                }
            }
        }
    }

    fn search<'a>(&self, key: &[u8], guard: &'a Guard) -> (PageId, SharedPage<'a>) {
        let mut id = PageId::zero();
        loop {
            let mut page = self.cache.get(id, guard);
            let header = page.header();
            let content = page.content();
            if content.is_data() {
                if header.len() >= self.opts.delta_chain_length {
                    page = self.consolidate_data(id, page, self.opts.data_node_size, guard);
                }
                return (id, page);
            } else {
                todo!()
            }
        }
    }

    fn lookup_data<'a>(
        &self,
        id: PageId,
        page: SharedPage<'a>,
        key: &[u8],
        guard: &'a Guard,
    ) -> Option<&'a [u8]> {
        let mut cursor = page;
        loop {
            match cursor.content() {
                PageContent::BaseData(base) => return base.get(key),
                PageContent::DeltaData(delta) => {
                    if let Some(value) = delta.get(key) {
                        return value;
                    }
                }
                PageContent::SplitData(_) | PageContent::MergeData(_) | PageContent::RemoveData => {
                    todo!()
                }
                _ => unreachable!(),
            }
            cursor = cursor.next().unwrap();
        }
    }

    fn consolidate_data<'a>(
        &self,
        id: PageId,
        page: SharedPage<'a>,
        split_size: usize,
        guard: &'a Guard,
    ) -> SharedPage<'a> {
        let mut cursor = page;
        let mut acc_delta = DeltaData::new();
        let (mut new_base, mut new_header) = loop {
            match cursor.content() {
                PageContent::BaseData(base) => {
                    let mut new_base = base.clone();
                    new_base.merge(acc_delta);
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
            let new_page = OwnedPage::new(new_header, PageContent::BaseData(new_base));
            return self
                .cache
                .replace(id, page, new_page, guard)
                .unwrap_or(page);
        }

        if let Some((split_key, right_base)) = new_base.split() {
            let right_header = new_header.split_at(&split_key);
            let right_page = OwnedPage::new(right_header, PageContent::BaseData(right_base));
            let left_page = OwnedPage::new(new_header, PageContent::BaseData(new_base));
            if let Ok((right_id, right_page)) = self.cache.install(right_page, guard) {
                let split = SplitNode {
                    lowest: right_page.header().lowest().to_owned(),
                    right_id,
                };
                let new_page =
                    OwnedPage::with_next(left_page.into_shared(), PageContent::SplitData(split));
                match self.cache.replace(id, page, new_page, guard) {
                    Ok(new_page) => return new_page,
                    Err(_) => {
                        self.cache.uninstall(right_id, guard);
                    }
                }
            }
        }

        page
    }
}
