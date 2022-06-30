use crossbeam_epoch::{unprotected, Guard};

use crate::{BaseData, DeltaData, Page, PageCache, PageContent, PageHeader, PageId, PageRef};

const ROOT_ID: PageId = PageId::min();

pub struct Tree {
    cache: PageCache,
}

impl Tree {
    pub fn new() -> Self {
        let cache = PageCache::new();

        let page = Page::new(PageHeader::new(), PageContent::BaseData(BaseData::new()));
        let root = cache.alloc(page);
        let guard = unsafe { unprotected() };
        assert_eq!(cache.install(root, guard), Some(ROOT_ID));

        Self { cache }
    }

    pub fn get<'a>(&self, key: &[u8], guard: &'a Guard) -> Option<&'a [u8]> {
        let (_, node) = self.search(key, guard);
        node.lookup_data(key)
    }

    pub fn write(&self, key: &[u8], value: Option<&[u8]>, guard: &Guard) {
        loop {
            let (id, node) = self.search(key, guard);
            let mut delta = DeltaData::new();
            delta.add(key.to_owned(), value.map(|v| v.to_owned()));
            let new_page = Page::with_next(node, PageContent::DeltaData(delta));
            let new_node = self.cache.alloc(new_page);
            match self.cache.cas(id, node, new_node) {
                Ok(_) => break,
                Err(_) => self.cache.dealloc(new_node, guard),
            }
        }
    }

    fn search<'a>(&self, key: &[u8], guard: &'a Guard) -> (PageId, PageRef<'a>) {
        loop {
            let mut id = ROOT_ID;
            let mut node = self.cache.get(id, guard);
            while node.covers(key) {
                if node.is_data() {
                    return (id, node);
                } else {
                    id = node.lookup_index(key);
                    node = self.cache.get(id, guard);
                }
            }
        }
    }
}
