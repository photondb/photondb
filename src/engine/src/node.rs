use std::collections::BTreeMap;

use crossbeam_epoch::Shared;

pub struct Link {
    pub node: Node,
    pub next: u64,
}

impl Link {
    pub fn len(&self) -> usize {
        let mut len = 0;
        let mut link = self;
        loop {
            len += 1;
            match unsafe { link.next().as_ref() } {
                Some(next) => link = next,
                None => break,
            }
        }
        len
    }

    pub fn next(&self) -> Shared<'_, Link> {
        unsafe { (self.next as *const Link).into() }
    }

    pub fn print(&self) {
        let mut link = self;
        loop {
            println!("{:?}", link.node);
            match unsafe { link.next().as_ref() } {
                Some(next) => link = next,
                None => break,
            }
        }
    }

    pub fn consolidate(&self) -> Link {
        let mut base = BaseDataNode {
            map: BTreeMap::new(),
        };
        let mut link = self;
        loop {
            match &link.node {
                Node::BaseData(node) => {
                    base.map.extend(node.map.clone());
                    break;
                }
                Node::DeltaData(node) => match node {
                    DeltaDataNode::Put(k, v) => {
                        base.map.insert(k.clone(), v.clone());
                    }
                    DeltaDataNode::Delete(k) => {
                        base.map.remove(k);
                    }
                },
            }
            match unsafe { link.next().as_ref() } {
                Some(next) => link = next,
                None => break,
            }
        }
        Link {
            node: Node::BaseData(base),
            next: 0,
        }
    }
}

#[derive(Debug)]
pub enum Node {
    BaseData(BaseDataNode),
    DeltaData(DeltaDataNode),
}

#[derive(Debug)]
pub struct BaseDataNode {
    pub map: BTreeMap<Vec<u8>, Vec<u8>>,
}

#[derive(Debug)]
pub enum DeltaDataNode {
    Put(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
}
