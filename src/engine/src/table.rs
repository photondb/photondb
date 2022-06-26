use crate::Tree;

pub struct Table {
    tree: Tree,
}

impl Table {
    pub fn new() -> Self {
        Self { tree: Tree::new() }
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.tree.lookup(key).map(|v| v.to_owned())
    }

    pub fn iter(&self) {
        todo!()
    }

    pub fn put(&self, key: &[u8], value: &[u8]) {
        self.tree.update(key, Some(value));
    }

    pub fn delete(&self, key: &[u8]) {
        self.tree.update(key, None);
    }
}
