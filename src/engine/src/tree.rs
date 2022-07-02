use crossbeam_epoch::{unprotected, Guard};

use crate::{
    BasePage, DeltaPage, Page, PageCache, PageContent, PageHeader, PageId, PageRef, SplitPage,
};

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
        assert_eq!(cache.install(root, guard), Some(PageId::zero()));

        Self { cfg, cache }
    }

    pub fn get<'a>(&self, key: &[u8], guard: &'a Guard) -> Option<&'a [u8]> {
        let (_, node) = self.search(key, guard);
    }

    pub fn write(&self, key: &[u8], value: Option<&[u8]>, guard: &Guard) {
        loop {
            let node = self.search(key, guard);
            let header = PageHeader::with_next(node.page);
            let mut content = DeltaPage::new();
            content.add(key.to_owned(), value.map(|v| v.to_owned()));
            let new_page = Page::new(header, PageContent::Delta(content));
            let new_page = self.cache.alloc(new_page);
            match self.cache.cas(node.id, node.page, new_page) {
                Ok(_) => break,
                Err(_) => self.cache.dealloc(new_page, guard),
            }
        }
    }

    fn root<'a>(&self, guard: &'a Guard) -> Node<'a> {
        let id = PageId::zero();
        let page = self.cache.get(id, guard);
        Node { id, page }
    }

    fn search<'a>(&self, key: &[u8], guard: &'a Guard) -> Node<'a> {
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
            if let Some(right_id) = self.cache.install(right_page, guard) {
                right_node = Some(Node {
                    id: right_id,
                    page: right_page,
                });
                let split_header = PageHeader::with_next(left_page);
                let split_content = SplitPage {
                    key: right_page.lower_bound().to_owned(),
                    right: right_id,
                };
                let split_page = Page::new(split_header, PageContent::Split(split_content));
                left_page = self.cache.alloc(split_page);
            } else {
                self.cache.dealloc(right_page, guard);
            }
        }
        match self.cache.replace(node.id, node.page, left_page, guard) {
            Ok(_) => {
                node.page = left_page;
                right_node
            }
            Err(actual) => {
                node.page = actual;
                self.cache.dealloc(left_page, guard);
                if let Some(right_node) = right_node {
                    self.cache.uninstall(right_node.id, guard);
                }
                None
            }
        }
    }
}
