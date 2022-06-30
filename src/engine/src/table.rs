use crossbeam_epoch::pin;

use crate::Tree;

pub struct Table {
    tree: Tree,
}

impl Table {
    pub fn new() -> Self {
        Self { tree: Tree::new() }
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

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_table() {
        let table = Table::new();
        table.put(b"key", b"value");
        assert_eq!(table.get(b"key"), Some(b"value".to_vec()));
        assert_eq!(table.get(b"not_found"), None);
        table.delete(b"key");
        assert_eq!(table.get(b"key"), None);
    }
}
