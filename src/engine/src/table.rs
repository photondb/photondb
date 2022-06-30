use crossbeam_epoch::pin;

use crate::{Config, Tree};

pub struct Table {
    tree: Tree,
}

impl Table {
    pub fn new(cfg: Config) -> Self {
        Self {
            tree: Tree::new(cfg),
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
        Self::new(Config::default())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_table() {
        let table = Table::default();
        table.put(b"key", b"value");
        assert_eq!(table.get(b"key"), Some(b"value".to_vec()));
        assert_eq!(table.get(b"not_found"), None);
        table.delete(b"key");
        assert_eq!(table.get(b"key"), None);
    }

    #[test]
    fn test_consolidate() {
        let table = Table::default();
        for i in 0..16u64 {
            let key = &i.to_be_bytes();
            table.put(key, key);
            assert_eq!(table.get(key), Some(key.to_vec()));
        }
    }
}
