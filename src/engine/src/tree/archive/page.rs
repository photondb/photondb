#[derive(Debug)]
pub struct Page {
    header: PageHeader,
    content: PageContent,
}

impl Page {
    pub fn link(&mut self, next: PageRef<'_>) {
        self.header = PageHeader::with_next(next);
    }

    pub fn unlink(&mut self) {
        self.header.next = 0;
    }
}

impl Drop for Page {
    fn drop(&mut self) {
        let mut next = self.header.next;
        while let Some(mut page) = PageBuf::from_usize(next) {
            if page.content.is_removed() {
                break;
            }
            next = page.header.next;
            page.header.next = 0;
        }
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

    pub fn is_removed(&self) -> bool {
        match self {
            PageContent::RemoveData | PageContent::RemoveIndex => true,
            _ => false,
        }
    }
}

#[derive(Clone, Debug)]
pub struct SplitNode {
    pub lowest: Vec<u8>,
    pub middle: Vec<u8>,
    pub highest: Vec<u8>,
    pub right_page: PageHandle,
}

impl SplitNode {
    pub fn covers(&self, key: &[u8]) -> Option<PageHandle> {
        if key >= &self.middle && (key < &self.highest || self.highest.is_empty()) {
            Some(self.right_page.clone())
        } else {
            None
        }
    }
}

#[derive(Debug)]
pub struct MergeNode {
    pub lowest: Vec<u8>,
    pub highest: Vec<u8>,
    pub right_page: PageBuf,
}
