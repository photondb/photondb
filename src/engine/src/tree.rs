use crate::{BaseData, DeltaData, Page, PageCache, PageId, PageKind, PagePtr};

type NodeId = PageId;

#[derive(Copy, Clone)]
enum Node<'a> {
    Data(DataNode<'a>),
    Index(IndexNode<'a>),
}

#[derive(Copy, Clone)]
struct DataNode<'a>(Page<'a>);

impl<'a> DataNode<'a> {
    fn as_ptr(self) -> PagePtr {
        self.0.as_ptr()
    }

    fn lookup(self, key: &[u8]) -> Option<&'a [u8]> {
        let mut current = self.0;
        loop {
            match current {
                Page::BaseData(page) => return page.get(key),
                Page::DeltaData(page) => {
                    if let Some(value) = page.get(key) {
                        return value;
                    }
                    current = page.next().deref();
                }
            }
        }
    }
}

#[derive(Copy, Clone)]
struct IndexNode<'a>(Page<'a>);

impl IndexNode<'_> {
    fn as_ptr(self) -> PagePtr {
        self.0.as_ptr()
    }

    fn search(self, target: &[u8]) -> NodeId {
        todo!()
    }
}

pub struct Tree {
    cache: PageCache,
}

const ROOT_ID: NodeId = NodeId::zero();

impl Tree {
    pub fn new() -> Self {
        let cache = PageCache::new();

        let root = Box::new(BaseData::default());
        let root_ptr = PagePtr::new(PageKind::BaseData, &*root as *const BaseData as u64);
        assert_eq!(cache.install(root_ptr), ROOT_ID);

        Self { cache }
    }

    fn node<'a>(&self, id: NodeId) -> Node<'a> {
        let ptr = self.cache.get(id);
        let page = ptr.deref();
        if page.is_data() {
            Node::Data(DataNode(page))
        } else {
            Node::Index(IndexNode(page))
        }
    }

    fn search(&self, target: &[u8]) -> (NodeId, DataNode) {
        let mut id = ROOT_ID;
        let mut current = self.node(id);
        loop {
            match current {
                Node::Data(node) => return (id, node),
                Node::Index(node) => {
                    id = node.search(target);
                    current = self.node(id);
                }
            }
        }
    }

    pub fn lookup(&self, key: &[u8]) -> Option<&[u8]> {
        let (_, node) = self.search(key);
        node.lookup(key)
    }

    pub fn update(&self, key: &[u8], value: Option<&[u8]>) {
        let (id, node) = self.search(key);
        let mut delta = Box::new(DeltaData::new(node.as_ptr()));
        delta.add(key.to_owned(), value.map(|v| v.to_owned()));
        let delta_ptr = PagePtr::new(PageKind::DeltaData, &*delta as *const DeltaData as u64);

        // TODO: validate the node range
        while let Some(next_ptr) = self.cache.cas(id, delta.next(), delta_ptr) {
            delta.set_next(next_ptr);
        }
        std::mem::forget(delta);
    }
}
