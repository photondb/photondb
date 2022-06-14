use crossbeam_epoch::{pin, Guard, Owned, Shared};

use crate::{DeltaData, Node, NodeCache, NodeData, NodeLink};

const NODE_CHAIN_MAXLEN: usize = 8;

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
        let mut node = self.root(guard);
        while !node.is_null() {
            match node.data() {
                NodeData::BaseData(node) => return node.map.get(key).cloned(),
                NodeData::DeltaData(node) => match node {
                    DeltaData::Put(k, v) => {
                        if k == key {
                            return Some(v.clone());
                        }
                    }
                    DeltaData::Delete(k) => {
                        if k == key {
                            return None;
                        }
                    }
                },
                _ => todo!(),
            }
            node = node.next();
        }
        None
    }

    pub fn put(&self, key: &[u8], value: &[u8]) {
        let guard = &pin();

        let mut old = self.root(guard);
        if !old.is_null() && old.len() >= NODE_CHAIN_MAXLEN {
            let new = old.consolidate(guard);
            match self.cache.update(0, old, new, guard) {
                Ok(_) => old = new,
                Err(node) => old = node,
            }
        }

        let data = NodeData::DeltaData(DeltaData::Put(key.to_vec(), value.to_vec()));
        let mut new = Node::from(Owned::new(NodeLink { data, next: 0 }).into_shared(guard));
        loop {
            new.set_next(old);
            match self.cache.update(0, old, new, guard) {
                Ok(_) => {
                    return;
                }
                Err(node) => old = node,
            }
        }
    }
}

impl Store {
    fn root<'g>(&self, guard: &'g Guard) -> Node<'g> {
        self.cache.load(0, guard)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_store() {
        let store = Store::new();
        for i in 0..10u8 {
            println!("put {}", i);
            store.put(&[i], &[i]);
            assert_eq!(store.get(&[i]), Some(vec![i]));
        }
    }
}
