use crossbeam_epoch::{pin, Guard, Owned, Shared};

use crate::{DeltaDataNode, Link, Node, NodeCache};

pub struct Store {
    cache: NodeCache,
}

impl Store {
    pub fn new() -> Self {
        Self {
            cache: NodeCache::new(),
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let guard = &pin();
        let mut link = self.root(guard);
        while !link.is_null() {
            let node = unsafe { &link.deref().node };
            match node {
                Node::BaseData(node) => return node.map.get(key).cloned(),
                Node::DeltaData(node) => match node {
                    DeltaDataNode::Put(k, v) => {
                        if k == key {
                            return Some(v.clone());
                        }
                    }
                    DeltaDataNode::Delete(k) => {
                        if k == key {
                            return None;
                        }
                    }
                },
            }
            link = unsafe { (link.deref().next as *const Link).into() };
        }
        None
    }

    pub fn put(&self, key: &[u8], value: &[u8]) {
        let guard = &pin();
        let delta = DeltaDataNode::Put(key.to_vec(), value.to_vec());
        let mut new = Owned::new(Link {
            node: Node::DeltaData(delta),
            next: 0,
        })
        .into_shared(guard);
        let mut old = self.root(guard);
        loop {
            unsafe {
                new.deref_mut().next = old.as_raw() as u64;
            }
            match self.cache.update(0, old, new) {
                Ok(_) => return,
                Err(link) => old = link,
            }
        }
    }
}

impl Store {
    fn root<'g>(&self, guard: &'g Guard) -> Shared<'g, Link> {
        self.cache.load(0, guard)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_store() {
        let store = Store::new();
        store.put(&[1, 2, 3], &[4, 5, 6]);
        assert_eq!(store.get(&[1, 2, 3]), Some(vec![4, 5, 6]));
    }
}
