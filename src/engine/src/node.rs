use std::collections::BTreeMap;

use crossbeam_epoch::{Guard, Owned, Shared};

#[derive(Clone, Copy)]
pub struct Node<'g>(Shared<'g, NodeLink>);

impl<'g> From<Shared<'g, NodeLink>> for Node<'g> {
    fn from(link: Shared<'g, NodeLink>) -> Self {
        Node(link)
    }
}

impl<'g> Node<'g> {
    pub fn from_u64(addr: u64, _: &'g Guard) -> Self {
        Node((addr as *const NodeLink).into())
    }

    pub fn as_u64(&self) -> u64 {
        self.0.as_raw() as u64
    }

    pub fn is_null(&self) -> bool {
        self.0.is_null()
    }

    pub fn data(&self) -> &NodeData {
        unsafe { &self.0.deref().data }
    }

    pub fn next(&self) -> Node<'g> {
        Node(unsafe { (self.0.deref().next as *const NodeLink).into() })
    }

    pub fn set_next(&mut self, next: Node<'g>) {
        unsafe {
            self.0.deref_mut().next = next.as_u64();
        }
    }

    pub fn len(&self) -> usize {
        let mut len = 0;
        let mut cursor = self.clone();
        while !cursor.is_null() {
            len += 1;
            cursor = cursor.next();
        }
        len
    }

    pub fn consolidate(&self, guard: &'g Guard) -> Node<'g> {
        let mut map = BTreeMap::new();
        let mut deltas = Vec::new();
        let mut cursor = self.clone();
        while !cursor.is_null() {
            match cursor.data() {
                NodeData::BaseData(node) => {
                    map = node.map.clone();
                    for (k, v) in deltas {
                        match v {
                            Some(v) => {
                                map.insert(k, v);
                            }
                            None => {
                                map.remove(&k);
                            }
                        }
                    }
                    break;
                }
                NodeData::DeltaData(node) => match node {
                    DeltaData::Put(k, v) => {
                        deltas.push((k.clone(), Some(v.clone())));
                    }
                    DeltaData::Delete(k) => {
                        deltas.push((k.clone(), None));
                    }
                },
                _ => todo!(),
            }
            cursor = cursor.next();
        }
        let data = NodeData::BaseData(BaseData { map });
        Node(Owned::new(NodeLink { data, next: 0 }).into_shared(guard))
    }
}

pub struct NodeLink {
    pub data: NodeData,
    pub next: u64,
}

#[derive(Debug)]
pub enum NodeData {
    BaseData(BaseData),
    DeltaData(DeltaData),
    BaseIndex(BaseIndex),
    DeltaIndex(DeltaIndex),
}

#[derive(Debug)]
pub struct BaseData {
    pub map: BTreeMap<Vec<u8>, Vec<u8>>,
}

#[derive(Debug)]
pub enum DeltaData {
    Put(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
}

#[derive(Debug)]
pub struct BaseIndex {
    pub map: BTreeMap<Vec<u8>, u64>,
}

#[derive(Debug)]
pub struct DeltaIndex {}
