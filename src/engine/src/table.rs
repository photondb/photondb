use std::sync::Arc;

use crossbeam_epoch::pin;

use crate::{Options, Tree};

#[derive(Clone)]
pub struct Table {
    tree: Arc<Tree>,
}

impl Table {
    pub fn new(opts: Options) -> Self {
        Self {
            tree: Arc::new(Tree::new(opts)),
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let guard = &pin();
        self.tree.get(key, guard).map(|v| v.to_owned())
    }

    pub fn iter(&self) {
        todo!()
    }

    pub fn put(&self, key: &[u8], value: &[u8]) {
        let guard = &pin();
        self.tree.write(key, Some(value), guard);
    }

    pub fn delete(&self, key: &[u8]) {
        let guard = &pin();
        self.tree.write(key, None, guard);
    }
}

impl Default for Table {
    fn default() -> Self {
        Self::new(Options::default())
    }
}
