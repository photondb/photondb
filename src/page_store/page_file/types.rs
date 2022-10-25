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
    pub(crate) fn meta(&self) -> Arc<FileMeta> {
        self.meta.clone()
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

    #[inline]
    pub(crate) fn iter(&self) -> FileInfoIterator {
        FileInfoIterator::new(self)
    }
}

pub(crate) struct FileMeta {
    file_id: u32,

    data_offsets: BTreeMap<u64, u64>, // TODO: reduce this size.
    meta_indexes: Vec<u64>,           // [0] -> page_table, [1] ->  delete page, [2], meta_bloc_end

    block_size: usize,
}

impl FileMeta {
    pub(crate) fn new(
        file_id: u32,
        indexes: Vec<u64>,
        offsets: BTreeMap<u64, u64>,
        block_size: usize,
    ) -> Self {
        Self {
            file_id,
            meta_indexes: indexes,
            data_offsets: offsets,
            block_size,
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

    // Return the block_size for the file's device.
    #[inline]
    pub(crate) fn block_size(&self) -> usize {
        self.block_size
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

/// [`FileInfoIterator`] is used to traverse [`FileInfo`] to get the addr of all
/// active pages.
pub(crate) struct FileInfoIterator<'a> {
    info: &'a FileInfo,
    iter: roaring::bitmap::Iter<'a>,
}

impl<'a> FileInfoIterator<'a> {
    fn new(info: &'a FileInfo) -> Self {
        FileInfoIterator {
            info,
            iter: info.active_pages.iter(),
        }
    }
}

impl<'a> Iterator for FileInfoIterator<'a> {
    type Item = u64;

    fn next(&mut self) -> Option<Self::Item> {
        let file_id = self.info.get_file_id();
        self.iter.next().map(|v| (file_id as u64) | (v as u64))
    }
}

// TODO: switch some common util method?
#[inline]
pub(crate) fn split_page_addr(page_addr: u64) -> (u32 /* file_id */, u32 /* index */) {
    ((page_addr >> 32) as u32, page_addr as u32)
}
