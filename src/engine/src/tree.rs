use crossbeam_epoch::{unprotected, Guard};

use crate::{
    BasePage, DeltaPage, Page, PageCache, PageContent, PageHeader, PageId, PageRef, SplitPage,
};

const ROOT_ID: PageId = PageId::min();

struct Node<'a> {
    id: PageId,
    page: PageRef<'a>,
}

pub struct Config {
    pub data_node_size: usize,
    pub index_node_size: usize,
    pub delta_chain_length: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            data_node_size: 8192,
            index_node_size: 4096,
            delta_chain_length: 8,
        }
    }
}

pub struct Tree {
    cfg: Config,
    cache: PageCache,
}

impl Tree {
    pub fn new(cfg: Config) -> Self {
        let cache = PageCache::new();

        let page = Page::new(PageHeader::new(), PageContent::Base(BasePage::new()));
        let root = cache.alloc(page);
        let guard = unsafe { unprotected() };
        assert_eq!(cache.install(root, guard), Some(ROOT_ID));

        Self { cfg, cache }
    }

    pub fn get<'a>(&self, key: &[u8], guard: &'a Guard) -> Option<&'a [u8]> {
        let (_, node) = self.search(key, guard);
    }

    pub fn write(&self, key: &[u8], value: Option<&[u8]>, guard: &Guard) {
        loop {
            let (id, node) = self.search(key, guard);
            let mut delta = DeltaPage::new();
            delta.add(key.to_owned(), value.map(|v| v.to_owned()));
            let new_page = Page::with_next(node, PageContent::Delta(delta));
            let new_node = self.cache.alloc(new_page);
            match self.cache.cas(id, node, new_node) {
                Ok(_) => break,
                Err(_) => self.cache.dealloc(new_node, guard),
            }
        }
    }

    fn root<'a>(&self, guard: &'a Guard) -> Node<'a> {
        let page = self.cache.get(ROOT_ID, guard);
        Node { id: ROOT_ID, page }
    }

    fn search<'a>(&self, key: &[u8], guard: &'a Guard) -> (PageId, PageRef<'a>) {
        loop {
            let mut node = self.root(guard);
            let mut parent = None;
            loop {
                if node.page.len() >= self.cfg.delta_chain_length {
                    let right_node = self.consolidate(&mut node, guard);
                }
                while node.covers(key) {
                    if node.is_data() {
                        return (id, node);
                    } else {
                        id = self.search_index(key, node, guard);
                        node = self.cache.get(id, guard);
                    }
                }
            }
        }
    }

    fn lookup_index<'a>(
        &self,
        key: &[u8],
        node: &mut Node<'a>,
        guard: &'a Guard,
    ) -> Option<Node<'a>> {
        loop {
            match node.content() {
                PageContent::Base(base) => {
                    if let Some(value) = base.get_le(key) {
                        return Some((value.1, self.cache.get(value.0, guard)));
                    }
                    let mut base = base.clone();
                    base.merge(DeltaPage::new());
                    return base.lookup(key);
                }
            }
        }
        todo!()
    }

    fn consolidate<'a>(&self, node: &mut Node<'a>, guard: &'a Guard) -> Option<Node<'a>> {
        let (left_page, right_page) = if node.page.is_data() {
            node.page.consolidate(self.cfg.data_node_size)
        } else {
            node.page.consolidate(self.cfg.index_node_size)
        };
        let mut left_page = self.cache.alloc(left_page);
        let mut right_node = None;
        if let Some(right_page) = right_page {
            let right_page = self.cache.alloc(right_page);
            let right_id = self.cache.install(right_page, guard).unwrap();
            right_node = Some(Node {
                id: right_id,
                page: right_page,
            });
            let split = SplitPage {
                key: right_page.lower_bound().to_owned(),
                right: right_id,
            };
            let split_page = Page::with_next(left_page, PageContent::Split(split));
            left_page = self.cache.alloc(split_page);
        }
        match self.cache.replace(node.id, node.page, left_page, guard) {
            Ok(_) => {
                node.page = left_page;
                right_node
            }
            Err(_) => {
                if let Some(right_node) = right_node {
                    self.cache.uninstall(right_node.id, guard);
                }
                self.cache.dealloc(left_page, guard);
                None
            }
        }
    }
}
