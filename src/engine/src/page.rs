use std::collections::BTreeMap;

use crate::PageId;

#[derive(Debug)]
pub enum PageContent {
    Base(BasePage),
    Delta(DeltaPage),
    Split(SplitPage),
}

#[derive(Clone, Debug)]
pub struct BasePage {
    size: usize,
    records: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl BasePage {
    pub fn new() -> Self {
        Self {
            size: 0,
            records: BTreeMap::new(),
        }
    }

    pub fn size(&self) -> usize {
        self.size
    }

    pub fn get(&self, key: &[u8]) -> Option<&[u8]> {
        self.records.get(key).map(|v| v.as_slice())
    }

    pub fn merge(&mut self, delta: DeltaPage) {
        for (key, value) in delta.records {
            if let Some(value) = value {
                self.size += key.len() + value.len();
                if let Some(old_value) = self.records.insert(key, value) {
                    self.size -= old_value.len();
                }
            } else {
                if let Some(old_value) = self.records.remove(&key) {
                    self.size -= key.len() + old_value.len();
                }
            }
        }
    }

    pub fn split(&mut self) -> Option<(Vec<u8>, BasePage)> {
        if let Some(key) = self.records.keys().nth(self.records.len() / 2).cloned() {
            let mut right = BasePage::new();
            right.records = self.records.split_off(&key);
            right.size = right
                .records
                .iter()
                .fold(0, |acc, (k, v)| acc + k.len() + v.len());
            Some((key, right))
        } else {
            None
        }
    }
}

#[derive(Clone, Debug)]
pub struct DeltaPage {
    records: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
}

impl DeltaPage {
    pub fn new() -> Self {
        Self {
            records: BTreeMap::new(),
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<Option<&[u8]>> {
        self.records
            .get(key)
            .map(|v| v.as_ref().map(|v| v.as_slice()))
    }

    pub fn add(&mut self, key: Vec<u8>, value: Option<Vec<u8>>) {
        self.records.insert(key, value);
    }

    pub fn merge(&mut self, other: DeltaPage) {
        for (key, value) in other.records {
            self.records.entry(key).or_insert(value);
        }
    }
}

#[derive(Debug)]
pub struct SplitPage {
    pub key: Vec<u8>,
    pub right: PageId,
}
