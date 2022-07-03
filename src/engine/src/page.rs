use std::{collections::BTreeMap, ptr::null_mut};

use crate::PageId;

#[derive(Debug)]
pub struct Page {
    header: PageHeader,
    content: PageContent,
}

impl Page {
    pub fn new(header: PageHeader, content: PageContent) -> Self {
        Self { header, content }
    }

    pub fn len(&self) -> usize {
        self.header.len
    }

    pub fn next<'a>(&self) -> Option<&'a Page> {
        unsafe { self.header.next.as_ref() }
    }

    pub fn lowest(&self) -> &[u8] {
        &self.header.lowest
    }

    pub fn highest(&self) -> &[u8] {
        &self.header.highest
    }

    pub fn content(&self) -> &PageContent {
        &self.content
    }
}

impl Drop for Page {
    fn drop(&mut self) {
        let mut next = self.header.next;
        while !next.is_null() {
            let page = unsafe { Box::from_raw(next) };
            if let PageContent::RemovePage = page.content() {
                // This page has been merged into the left page.
                next = null_mut();
            } else {
                next = page.header.next;
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct PageHeader {
    len: usize,
    next: *mut Page,
    lowest: Vec<u8>,
    highest: Vec<u8>,
}

impl PageHeader {
    pub fn new() -> Self {
        Self {
            len: 1,
            next: null_mut(),
            lowest: Vec::new(),
            highest: Vec::new(),
        }
    }

    pub fn next<'a>(&self) -> Option<&'a Page> {
        unsafe { self.next.as_ref() }
    }

    pub fn set_next(&mut self, next: &'_ Page) {
        self.len = next.len() + 1;
        self.next = next as *const _ as *mut _;
    }

    pub fn contains(&self, key: &[u8]) -> bool {
        key >= &self.lowest && (key < &self.highest || self.highest.is_empty())
    }

    pub fn split_at(&mut self, key: &[u8]) -> PageHeader {
        assert!(self.contains(key));
        let right = PageHeader {
            len: 1,
            next: null_mut(),
            lowest: key.to_vec(),
            highest: self.highest.clone(),
        };
        self.highest = key.to_vec();
        right
    }
}

#[derive(Debug)]
pub enum PageContent {
    BaseData(BaseData),
    DeltaData(DeltaData),
    BaseIndex(BaseIndex),
    DeltaIndex(DeltaIndex),
    SplitPage(SplitPage),
    MergePage(MergePage),
    RemovePage,
}

#[derive(Clone, Debug)]
pub struct BaseData {
    size: usize,
    records: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl BaseData {
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

    pub fn merge(&mut self, delta: DeltaData) {
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

    pub fn split(&mut self) -> Option<(Vec<u8>, BaseData)> {
        if let Some(key) = self.records.keys().nth(self.records.len() / 2).cloned() {
            let mut right = BaseData::new();
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

    pub fn merge(&mut self, other: DeltaData) {
        for (key, value) in other.records {
            self.records.entry(key).or_insert(value);
        }
    }
}

#[derive(Debug)]
pub struct BaseIndex {
    size: usize,
    children: BTreeMap<Vec<u8>, PageId>,
}

#[derive(Debug)]
pub struct DeltaIndex {
    lower_bound: Vec<u8>,
    upper_bound: Vec<u8>,
    child: PageId,
    remove_child: Option<PageId>,
}

#[derive(Debug)]
pub struct SplitPage {
    pub lowest: Vec<u8>,
    pub right_page: PageId,
}

#[derive(Debug)]
pub struct MergePage {
    pub lowest: Vec<u8>,
    pub right_page: Box<Page>,
}

/*
    pub fn consolidate(self, split_size: usize) -> (Page, Option<Page>) {
        let mut page = self;
        let mut delta = DeltaPage::new();
        loop {
            match page.content() {
                PageContent::Base(base) => {
                    let mut base = base.clone();
                    base.merge(delta);
                    let header = page.0.header.clone();
                    let mut right_page = None;
                    if base.size() >= split_size {
                        if let Some((split_key, right_base)) = base.split() {
                            let right_header = header.split_at(&split_key);
                            right_page =
                                Some(Page::new(right_header, PageContent::Base(right_base)));
                        }
                    }
                    let left_page = Page::new(header, PageContent::Base(base));
                    return (left_page, right_page);
                }
                PageContent::Delta(data) => {
                    delta.merge(data.clone());
                }
                PageContent::Split(_) => {}
            }
            page = page.next().unwrap();
        }
    }
}
*/
