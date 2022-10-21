use std::{collections::BTreeMap, sync::Arc};

use crate::page_store::{Error, Result};

#[derive(Debug, Clone)]
pub(crate) struct PageHandle {
    pub(crate) offset: u32,
    pub(crate) size: u32,
}

#[derive(Clone)]
pub(crate) struct FileInfo {
    active_pages: roaring::RoaringBitmap,

    decline_rate: f64,
    active_size: usize,
    meta: Arc<FileMeta>,
}

impl FileInfo {
    pub(crate) fn new(
        active_pages: roaring::RoaringBitmap,

        decline_rate: f64,
        active_size: usize,
        meta: Arc<FileMeta>,
    ) -> Self {
        Self {
            active_pages,
            decline_rate,
            active_size,
            meta,
        }
    }

    #[inline]
    pub(crate) fn get_file_id(&self) -> u32 {
        self.meta.get_file_id()
    }

    #[inline]
    pub(crate) fn is_empty(&self) -> bool {
        self.active_pages.is_empty()
    }

    pub(crate) fn deactivate_page(&mut self, page_addr: u64) {
        let (_, index) = split_page_addr(page_addr);
        if self.active_pages.remove(index) {
            if let Some((_, page_size)) = self.meta.get_page_handle(page_addr) {
                self.active_size -= page_size;
            }
        }
    }

    /// Get the [`PageHandle`] of the corresponding page. Returns `None` if no
    /// such active page exists.
    pub(crate) fn get_page_handle(&self, page_addr: u64) -> Option<PageHandle> {
        if !self.is_page_active(page_addr) {
            return None;
        }
        let (offset, size) = self.meta.get_page_handle(page_addr)?;
        Some(PageHandle {
            offset: offset as u32,
            size: size as u32,
        })
    }

    #[inline]
    fn is_page_active(&self, page_addr: u64) -> bool {
        let index = page_addr as u32;
        self.active_pages.contains(index)
    }

    #[inline]
    pub(crate) fn effective_size(&self) -> u32 {
        self.active_size as u32
    }

    pub(crate) fn decline_rate(&self) -> f64 {
        todo!()
    }
}

pub(crate) struct FileMeta {
    file_id: u32,

    data_offsets: BTreeMap<u64, u64>, // TODO: reduce this size.
    meta_indexes: Vec<u64>,           // [0] -> page_table, [1] ->  delete page, [2], meta_bloc_end
}

impl FileMeta {
    pub(crate) fn new(file_id: u32, indexes: Vec<u64>, offsets: BTreeMap<u64, u64>) -> Self {
        Self {
            file_id,
            meta_indexes: indexes,
            data_offsets: offsets,
        }
    }

    #[inline]
    pub(crate) fn get_file_id(&self) -> u32 {
        self.file_id
    }

    /// Returns the page size for the page specified by `page_addr`.
    pub(crate) fn get_page_handle(
        &self,
        page_addr: u64,
    ) -> Option<(u64 /* offset */, usize /* size */)> {
        let mut iter = self.data_offsets.range(page_addr..);
        let start_offset = match iter.next() {
            Some((addr, offset)) if *addr == page_addr => *offset,
            _ => return None, // no page exist.
        };
        let end_offset = match iter.next() {
            Some((_, offset)) => *offset,
            None => self.total_page_size() as u64, /* it's the last page use total-page-size as
                                                    * end val. */
        };
        Some((start_offset, (end_offset - start_offset) as usize))
    }

    // Return the total page size(include inactive page).
    #[inline]
    pub(crate) fn total_page_size(&self) -> usize {
        (**self.meta_indexes.first().as_ref().unwrap()) as usize
    }

    pub(crate) fn get_page_table_meta_page(
        &self,
    ) -> Result<(u64 /* offset */, usize /* length */)> {
        if let &[start, end, ..] = self.meta_indexes.as_slice() {
            Ok((start, (end - start) as usize))
        } else {
            Err(Error::Corrupted)
        }
    }

    pub(crate) fn get_delete_pages_meta_page(
        &self,
    ) -> Result<(u64 /* offset */, usize /* length */)> {
        if let &[_, start, end] = self.meta_indexes.as_slice() {
            Ok((start, (end - start) as usize))
        } else {
            Err(Error::Corrupted)
        }
    }
}

// TODO: switch some common util method?
#[inline]
pub(crate) fn split_page_addr(page_addr: u64) -> (u32 /* file_id */, u32 /* index */) {
    ((page_addr >> 32) as u32, page_addr as u32)
}
