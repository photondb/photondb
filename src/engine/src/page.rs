use std::{
    collections::BTreeMap,
    ops::{Deref, DerefMut},
};

use crate::PageId;

#[derive(Debug)]
pub struct OwnedPage(Box<Page>);

impl OwnedPage {
    pub fn new(header: PageHeader, content: PageContent) -> Self {
        Self(Box::new(Page { header, content }))
    }

    pub fn with_content(content: PageContent) -> Self {
        Self::new(PageHeader::new(), content)
    }

    pub fn with_next(next: SharedPage<'_>, content: PageContent) -> Self {
        Self::new(PageHeader::with_next(next), content)
    }

    pub fn from_usize(ptr: usize) -> Option<Self> {
        if ptr == 0 {
            None
        } else {
            Some(Self(unsafe { Box::from_raw(ptr as *mut Page) }))
        }
    }

    pub fn into_usize(self) -> usize {
        Box::into_raw(self.0) as usize
    }

    pub fn into_shared<'a>(self) -> SharedPage<'a> {
        SharedPage(unsafe { &*Box::into_raw(self.0) })
    }
}

impl Deref for OwnedPage {
    type Target = Page;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for OwnedPage {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[derive(Copy, Clone, Debug)]
pub struct SharedPage<'a>(&'a Page);

impl<'a> SharedPage<'a> {
    pub fn from_usize(ptr: usize) -> Option<Self> {
        if ptr == 0 {
            None
        } else {
            Some(Self(unsafe { &*(ptr as *const Page) }))
        }
    }

    pub fn into_usize(self) -> usize {
        self.0 as *const Page as usize
    }

    pub fn next(self) -> Option<SharedPage<'a>> {
        self.0.next()
    }

    pub fn content(self) -> &'a PageContent {
        self.0.content()
    }
}

impl<'a> Deref for SharedPage<'a> {
    type Target = Page;

    fn deref(&self) -> &'a Self::Target {
        self.0
    }
}

#[derive(Debug)]
pub struct Page {
    header: PageHeader,
    content: PageContent,
}

impl Page {
    pub fn len(&self) -> usize {
        self.header.len
    }

    pub fn next<'a>(&self) -> Option<SharedPage<'a>> {
        SharedPage::from_usize(self.header.next)
    }

    pub fn link(&mut self, next: SharedPage<'_>) {
        self.header = PageHeader::with_next(next);
    }

    pub fn header(&self) -> &PageHeader {
        &self.header
    }

    pub fn content(&self) -> &PageContent {
        &self.content
    }
}

impl Drop for Page {
    fn drop(&mut self) {
        let mut next = self.header.next;
        while let Some(page) = OwnedPage::from_usize(next) {
            if page.content.is_remove() {
                // This page has been merged into the left page.
                next = 0;
            } else {
                next = page.header.next;
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct PageHeader {
    len: usize,
    next: usize,
    lowest: Vec<u8>,
    highest: Vec<u8>,
}

impl PageHeader {
    fn new() -> Self {
        Self {
            len: 1,
            next: 0,
            lowest: Vec::new(),
            highest: Vec::new(),
        }
    }

    fn with_next<'a>(next: SharedPage<'a>) -> Self {
        let mut header = next.header.clone();
        header.len += 1;
        header.next = next.into_usize();
        header
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn covers(&self, key: &[u8]) -> bool {
        key >= &self.lowest && (key < &self.highest || self.highest.is_empty())
    }

    pub fn lowest(&self) -> &[u8] {
        &self.lowest
    }

    pub fn highest(&self) -> &[u8] {
        &self.highest
    }

    pub fn split_at(&mut self, key: &[u8]) -> PageHeader {
        assert!(self.covers(key));
        let right = PageHeader {
            len: 1,
            next: 0,
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
    SplitData(SplitNode),
    MergeData(MergeNode),
    RemoveData,
    BaseIndex(BaseIndex),
    DeltaIndex(DeltaIndex),
    SplitIndex(SplitNode),
    MergeIndex(MergeNode),
    RemoveIndex,
}

impl PageContent {
    pub fn is_data(&self) -> bool {
        match self {
            PageContent::BaseData(_)
            | PageContent::DeltaData(_)
            | PageContent::SplitData(_)
            | PageContent::MergeData(_)
            | PageContent::RemoveData => true,
            _ => false,
        }
    }

    pub fn is_remove(&self) -> bool {
        match self {
            PageContent::RemoveData | PageContent::RemoveIndex => true,
            _ => false,
        }
    }
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
pub struct SplitNode {
    pub lowest: Vec<u8>,
    pub right_id: PageId,
}

#[derive(Debug)]
pub struct MergeNode {
    pub lowest: Vec<u8>,
    pub right_page: OwnedPage,
}
