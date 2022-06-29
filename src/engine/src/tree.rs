use crate::{BaseData, DeltaData, Page, PageCache, PageContent, PageHeader, PageId, PagePtr};

type NodeId = PageId;

const ROOT_ID: NodeId = NodeId::zero();

#[derive(Copy, Clone)]
struct Node<'a>(PagePtr<'a>);

impl<'a> Node<'a> {
    fn as_ptr(self) -> PagePtr<'a> {
        self.0
    }

    fn covers(self, key: &[u8]) -> bool {
        self.0.covers(key)
    }

    fn is_data(self) -> bool {
        self.0.is_data()
    }

    fn lookup_data(self, key: &[u8]) -> Option<&'a [u8]> {
        let mut current = self.0;
        loop {
            match current.content() {
                PageContent::BaseData(page) => return page.get(key),
                PageContent::DeltaData(page) => {
                    if let Some(value) = page.get(key) {
                        return value;
                    }
                }
            }
            match current.next() {
                Some(next) => current = next,
                None => return None,
            }
        }
    }

    fn lookup_index(self, key: &[u8]) -> NodeId {
        todo!()
    }
}

pub struct Tree {
    cache: PageCache,
}

impl Tree {
    pub fn new() -> Self {
        let cache = PageCache::new();

        let root = Box::new(Page::new(
            PageHeader::default(),
            PageContent::BaseData(BaseData::new()),
        ));
        let root_ptr = PagePtr::new(Box::leak(root));
        assert_eq!(cache.install(root_ptr), ROOT_ID);

        Self { cache }
    }

    pub fn get(&self, key: &[u8], guard: &Guard) -> Option<&[u8]> {
        let (_, node) = self.search(key);
        node.lookup_data(key)
    }

    pub fn write(&self, key: &[u8], value: Option<&[u8]>, guard: &Guard) {
        let (id, node) = self.search(key);
        let mut delta = DeltaData::new();
        delta.add(key.to_owned(), value.map(|v| v.to_owned()));
        let header = node.header().clone();
        header.next = node;
        let content = PageContent::DeltaData(delta);
        let new_page = self.cache.alloc(node.header().clone(), content);

        // TODO: validate the node range
        while let Some(next_ptr) = self.cache.cas(id, delta.next(), delta_ptr) {
            delta.set_next(next_ptr);
        }
        std::mem::forget(delta);
    }

    fn node<'a>(&self, id: NodeId) -> Node<'a> {
        Node(self.cache.get(id))
    }

    fn search<'a>(&self, key: &[u8]) -> (NodeId, Node<'a>) {
        loop {
            let mut id = ROOT_ID;
            let mut current = self.node(id);
            while current.covers(key) {
                if current.is_data() {
                    return (id, current);
                } else {
                    id = current.lookup_index(key);
                    current = self.node(id);
                }
            }
        }
    }
}
