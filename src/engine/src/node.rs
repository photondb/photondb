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
    pub fn null() -> Self {
        Node(Shared::null())
    }

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

    pub fn size(&self) -> usize {
        let mut size = 0;
        let mut cursor = self.clone();
        while !cursor.is_null() {
            size += cursor.data().size();
            cursor = cursor.next();
        }
        size
    }

    pub fn stat(&self) -> NodeStat {
        let mut stat = NodeStat { len: 0, size: 0 };
        let mut cursor = self.clone();
        while !cursor.is_null() {
            stat.len += 1;
            stat.size += cursor.data().size();
            cursor = cursor.next();
        }
        stat
    }

    pub fn split(&self, guard: &'g Guard) -> Option<(Vec<u8>, Node<'g>)> {
        if let Some(pivot) = self.split_key() {
            let mut map = BTreeMap::new();
            let mut deltas = Vec::new();
            let mut cursor = self.clone();
            while !cursor.is_null() {
                match cursor.data() {
                    NodeData::BaseData(node) => {
                        map = node
                            .map
                            .iter()
                            .filter(|(k, _)| k > &&pivot)
                            .map(|(k, v)| (k.clone(), v.clone()))
                            .collect();
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
                            if k > &pivot {
                                deltas.push((k.clone(), Some(v.clone())));
                            }
                        }
                        DeltaData::Delete(k) => {
                            if k > &pivot {
                                deltas.push((k.clone(), None));
                            }
                        }
                        _ => todo!(),
                    },
                    _ => todo!(),
                }
                cursor = cursor.next();
            }
            let data = NodeData::BaseData(BaseData { map });
            let node = Node(Owned::new(NodeLink { data, next: 0 }).into_shared(guard));
            Some((pivot, node))
        } else {
            None
        }
    }

    pub fn split_key(&self) -> Option<Vec<u8>> {
        let mut cursor = self.clone();
        while !cursor.is_null() {
            match cursor.data() {
                NodeData::BaseData(node) => {
                    let mid = node.map.len() / 2;
                    return node.map.iter().nth(mid).map(|(k, _)| k.clone());
                }
                _ => {}
            }
            cursor = cursor.next();
        }
        None
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
                    _ => todo!(),
                },
                _ => todo!(),
            }
            cursor = cursor.next();
        }
        let data = NodeData::BaseData(BaseData { map });
        Node(Owned::new(NodeLink { data, next: 0 }).into_shared(guard))
    }
}

pub struct NodeStat {
    pub len: usize,
    pub size: usize,
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

impl NodeData {
    pub fn size(&self) -> usize {
        match self {
            NodeData::BaseData(node) => node.size(),
            NodeData::DeltaData(node) => node.size(),
            NodeData::BaseIndex(node) => todo!(),
            NodeData::DeltaIndex(node) => todo!(),
        }
    }
}

#[derive(Debug)]
pub struct BaseData {
    pub map: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl BaseData {
    pub fn size(&self) -> usize {
        self.map.iter().map(|(k, v)| k.len() + v.len()).sum()
    }
}

#[derive(Debug)]
pub enum DeltaData {
    Put(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
    SplitNode(Vec<u8>, u64),
    MergeNode(Vec<u8>, u64),
    RemoveNode,
}

impl DeltaData {
    pub fn size(&self) -> usize {
        match self {
            DeltaData::Put(k, v) => k.len() + v.len(),
            DeltaData::Delete(k) => k.len(),
            _ => todo!(),
        }
    }
}

#[derive(Debug)]
pub struct BaseIndex {
    pub map: BTreeMap<Vec<u8>, u64>,
}

#[derive(Debug)]
pub enum DeltaIndex {
    AddChild(Vec<u8>, Vec<u8>, u64),
    RemoveChild,
}
