use std::collections::BTreeMap;

pub struct Page {
    header: PageHeader,
    content: PageContent,
}

impl Page {
    pub fn new(header: PageHeader, content: PageContent) -> Self {
        Self { header, content }
    }
}

#[derive(Clone)]
pub struct PageHeader {
    next: u64,
    lower_bound: Vec<u8>,
    upper_bound: Vec<u8>,
}

impl PageHeader {
    pub fn new() -> Self {
        Self {
            next: 0,
            lower_bound: Vec::new(),
            upper_bound: Vec::new(),
        }
    }

    pub fn contains(&self, key: &[u8]) -> bool {
        &self.lower_bound <= key && (key <= &self.upper_bound || self.upper_bound.is_empty())
    }
}

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
}

pub struct DeltaData {
    records: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
}

impl DeltaData {
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
}
