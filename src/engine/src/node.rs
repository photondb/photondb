use std::collections::BTreeMap;

pub struct Link {
    pub node: Node,
    pub next: u64,
}

pub enum Node {
    BaseData(BaseDataNode),
    DeltaData(DeltaDataNode),
}

pub struct BaseDataNode {
    pub map: BTreeMap<Vec<u8>, Vec<u8>>,
}

pub enum DeltaDataNode {
    Put(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
}
