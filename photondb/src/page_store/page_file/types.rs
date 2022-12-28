use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
};

use super::{compression::Compression, BlockHandle, ChecksumType};
use crate::{page::PageInfo, util::bitmap::FixedBitmap};

#[derive(Debug, Copy, Clone)]
pub(crate) struct PageHandle {
    pub(crate) offset: u32,
    pub(crate) size: u32,
}

struct PageMeta {
    index: u32,
    info: PageInfo,
    handle: PageHandle,
}

/// A group of pages has same id.
#[derive(Clone)]
pub(crate) struct PageGroup {
    /// The bitmap of deallocated pages.
    dealloc_pages: FixedBitmap,

    active_size: usize,

    meta: Arc<PageGroupMeta>,
}

/// [`PageGroupIterator`] is used to traverse [`PageGroup`] to get the addr of
/// all active pages.
pub(crate) struct PageGroupIterator {
    file_id: u32,
    index: usize,
    active_pages: Vec<(u32, u32)>,
}

/// The immutable metadata for a page group.
pub(crate) struct PageGroupMeta {
    /// The id of the group pages.
    pub(crate) group_id: u32,

    /// The id of file this page group belongs to.
    pub(crate) file_id: u32,

    /// The first offset of page group in file.
    base_offset: u64,
    /// The offset of page table.
    page_table_offset: u64,
    /// The last offset.
    meta_block_end: u64,

    page_meta_map: HashMap<u32, PageMeta>,
}

/// The volatile info of a file.
#[derive(Clone)]
pub(crate) struct FileInfo {
    up1: u32,
    up2: u32,

    meta: Arc<FileMeta>,
}

/// The meta of files.
pub(crate) struct FileMeta {
    pub(crate) file_id: u32,
    pub(crate) file_size: usize,
    pub(crate) block_size: usize,

    /// The dealloc pages referenced page groups.
    pub(crate) referenced_groups: HashSet<u32>,

    pub(crate) checksum_type: ChecksumType,
    pub(crate) compression: Compression,
    pub(crate) page_groups: HashMap<u32, Arc<PageGroupMeta>>,
}

impl PageGroup {
    pub(crate) fn new(meta: Arc<PageGroupMeta>) -> Self {
        let dealloc_pages = FixedBitmap::new(meta.page_meta_map.len() as u32);
        let active_size = meta.total_page_size();
        PageGroup {
            dealloc_pages,
            active_size,
            meta,
        }
    }

    #[inline]
    pub(crate) fn is_empty(&self) -> bool {
        self.dealloc_pages.is_full()
    }

    pub(crate) fn deactivate_page(&mut self, page_addr: u64) -> bool {
        let Some((index, handle)) = self.meta.get_page_handle(page_addr) else {
            return false;
        };

        if self.dealloc_pages.set(index as u32) {
            self.active_size -= handle.size as usize;
            return true;
        }

        false
    }

    /// Get the [`PageHandle`] of the corresponding page. Returns `None` if no
    /// such active page exists.
    pub(crate) fn get_page_handle(&self, page_addr: u64) -> Option<PageHandle> {
        let (index, handle) = self.meta.get_page_handle(page_addr)?;
        if self.dealloc_pages.test(index as u32) {
            return None;
        }

        Some(handle)
    }

    /// Get the [`PageInfo`] of the specified page.
    #[inline]
    pub(crate) fn get_page_info(&self, page_addr: u64) -> Option<PageInfo> {
        self.meta.get_page_info(page_addr)
    }

    #[inline]
    pub(crate) fn meta(&self) -> &Arc<PageGroupMeta> {
        &self.meta
    }

    #[cfg(test)]
    pub(crate) fn is_page_active(&self, page_addr: u64) -> bool {
        self.meta
            .get_page_handle(page_addr)
            .map(|(index, _)| index as u32)
            .map(|index| !self.dealloc_pages.test(index))
            .unwrap_or_default()
    }

    #[inline]
    pub(crate) fn num_active_pages(&self) -> usize {
        self.dealloc_pages.free() as usize
    }

    #[inline]
    pub(crate) fn empty_pages_rate(&self) -> f64 {
        let active_pages = self.dealloc_pages.free() as f64;
        let total_pages = self.meta.total_pages() as f64 + 0.1;
        debug_assert!(active_pages <= total_pages);
        1.0 - (active_pages / total_pages)
    }

    #[inline]
    pub(crate) fn effective_size(&self) -> usize {
        self.active_size as usize
    }

    #[inline]
    pub(crate) fn iter(&self) -> PageGroupIterator {
        PageGroupIterator::new(self)
    }
}

impl PageGroupMeta {
    pub(crate) fn new(
        group_id: u32,
        file_id: u32,
        base_offset: u64,
        meta_indexes: Vec<u64>,
        data_offsets: BTreeMap<u64, (u64, PageInfo)>,
    ) -> Self {
        let page_table_offset = meta_indexes[0];
        let meta_block_end = meta_indexes[1];
        let page_meta_map = Self::build_page_meta_map(data_offsets, &meta_indexes);
        PageGroupMeta {
            group_id,
            file_id,
            base_offset,
            page_table_offset,
            meta_block_end,
            page_meta_map,
        }
    }

    /// Returns the page size for the page specified by `page_addr`.
    pub(crate) fn get_page_handle(
        &self,
        page_addr: u64,
    ) -> Option<(usize /* index */, PageHandle)> {
        let addr = page_addr as u32;
        if let Some(page_meta) = self.page_meta_map.get(&addr) {
            return Some((page_meta.index as usize, page_meta.handle));
        }
        None
    }

    pub(crate) fn get_page_info(&self, page_addr: u64) -> Option<PageInfo> {
        let addr = page_addr as u32;
        if let Some(page_meta) = self.page_meta_map.get(&addr) {
            return Some(page_meta.info.clone());
        }
        None
    }

    /// Return the total page (include inactive page).
    #[inline]
    pub(crate) fn total_pages(&self) -> usize {
        self.page_meta_map.len()
    }

    /// Return the total page size(include inactive page), it alway large than
    /// zero.
    #[inline]
    pub(crate) fn total_page_size(&self) -> usize {
        let size = self.page_table_offset.saturating_sub(self.base_offset);
        if size == 0 {
            1
        } else {
            size as usize
        }
    }

    #[inline]
    pub(crate) fn get_page_table_meta_page(&self) -> BlockHandle {
        BlockHandle {
            offset: self.page_table_offset,
            length: self.meta_block_end - self.page_table_offset,
        }
    }

    fn build_page_meta_map(
        data_offsets: BTreeMap<u64, (u64, PageInfo)>,
        meta_indexes: &[u64],
    ) -> HashMap<u32, PageMeta> {
        let mut page_meta_map = HashMap::with_capacity(data_offsets.len());
        let mut last_addr = None;
        for (index, (addr, (offset, info))) in data_offsets.into_iter().enumerate() {
            let addr = addr as u32;
            let offset = offset as u32;
            let index = index as u32;
            page_meta_map.insert(
                addr,
                PageMeta {
                    info,
                    index,
                    handle: PageHandle { offset, size: 0 },
                },
            );
            if let Some(last_addr) = last_addr {
                let meta = page_meta_map
                    .get_mut(&last_addr)
                    .expect("The last one is existed");
                meta.handle.size = offset
                    .checked_sub(meta.handle.offset)
                    .expect("The offset is increasing");
            }
            last_addr = Some(addr);
        }
        if let Some(last_addr) = last_addr {
            let meta = page_meta_map
                .get_mut(&last_addr)
                .expect("The last one is existed");

            // it's the last page use total-page-size as end val.
            let page_table_offset = meta_indexes[0] as u32;
            meta.handle.size = page_table_offset
                .checked_sub(meta.handle.offset)
                .expect("The offset is increasing");
        }
        page_meta_map
    }
}

impl FileInfo {
    pub(crate) fn new(up1: u32, up2: u32, meta: Arc<FileMeta>) -> Self {
        FileInfo { up1, up2, meta }
    }

    pub(crate) fn on_update(&mut self, now: u32) {
        if self.up1 < now {
            self.up2 = self.up1;
            self.up1 = now;
        }
    }

    #[inline]
    pub(crate) fn meta(&self) -> &Arc<FileMeta> {
        &self.meta
    }

    #[inline]
    pub(crate) fn up1(&self) -> u32 {
        self.up1
    }

    #[inline]
    pub(crate) fn up2(&self) -> u32 {
        self.up2
    }
}

impl FileMeta {
    pub(crate) fn new(
        file_id: u32,
        file_size: usize,
        block_size: usize,
        checksum_type: ChecksumType,
        compression: Compression,
        referenced_groups: HashSet<u32>,
        page_groups: HashMap<u32, Arc<PageGroupMeta>>,
    ) -> Self {
        FileMeta {
            file_id,
            file_size,
            block_size,
            checksum_type,
            compression,
            referenced_groups,
            page_groups,
        }
    }
}

impl PageGroupIterator {
    fn new(info: &PageGroup) -> Self {
        let mut active_pages = info
            .meta
            .page_meta_map
            .iter()
            .filter(|(_, meta)| !info.dealloc_pages.test(meta.index))
            .map(|(&addr, meta)| (addr, meta.index))
            .collect::<Vec<_>>();
        active_pages.sort_by_key(|(_, index)| *index);
        let file_id = info.meta.group_id;
        PageGroupIterator {
            file_id,
            index: 0,
            active_pages,
        }
    }
}

impl Iterator for PageGroupIterator {
    type Item = u64;

    fn next(&mut self) -> Option<Self::Item> {
        let index = self.index;
        self.index += 1;
        if self.active_pages.len() <= index {
            return None;
        }

        return Some(
            ((self.file_id as u64) << 32)
                | (unsafe { self.active_pages.get_unchecked(index).0 } as u64),
        );
    }
}

// TODO: switch some common util method?
#[inline]
pub(crate) fn split_page_addr(page_addr: u64) -> (u32 /* file_id */, u32 /* index */) {
    ((page_addr >> 32) as u32, page_addr as u32)
}
