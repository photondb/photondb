use std::collections::BTreeMap;

#[derive(Debug)]
pub enum PageContent {
    BaseData(BaseData),
    DeltaData(DeltaData),
}

impl PageContent {
    pub fn is_data(&self) -> bool {
        match self {
            PageContent::BaseData(_) | PageContent::DeltaData(_) => true,
        }
    }
}

#[derive(Clone, Debug)]
pub struct BaseData {
    records: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl BaseData {
    pub fn new() -> Self {
        Self {
            records: BTreeMap::new(),
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<&[u8]> {
        self.records.get(key).map(|v| v.as_slice())
    }

    pub fn merge(&mut self, delta: DeltaData) {
        for (key, value) in delta.records {
            if let Some(value) = value {
                self.records.insert(key, value);
            } else {
                self.records.remove(&key);
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct DeltaData {
    records: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
}

impl DeltaData {
    pub fn new() -> Self {
        Self {
            records: BTreeMap::new(),
        }
    }

    pub fn add(&mut self, key: Vec<u8>, value: Option<Vec<u8>>) {
        self.records.insert(key, value);
    }

    pub fn merge(&mut self, other: DeltaData) {
        for (key, value) in other.records {
            self.records.entry(key).or_insert(value);
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<Option<&[u8]>> {
        self.records
            .get(key)
            .map(|v| v.as_ref().map(|v| v.as_slice()))
    }
}
