use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
};

use super::{compression::Compression, ChecksumType};
use crate::{
    page::PageInfo,
    page_store::{Error, Result},
    util::bitmap::FixedBitmap,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum FileId {
    Page(u32),
    Map(u32),
}

#[derive(Debug, Clone)]
pub(crate) struct PageHandle {
    pub(crate) offset: u32,
    pub(crate) size: u32,
}

/// The volatile info for page file, also include the partial page files
/// (partial of map file).
#[derive(Clone)]
pub(crate) struct FileInfo {
    dealloc_pages: FixedBitmap,

    up1: u32,
    up2: u32,

    active_size: usize,

    /// Records the files referenced by dealloc pages saved in the file.
    referenced_files: HashSet<u32>,

    meta: Arc<FileMeta>,
}

impl FileInfo {
    pub(crate) fn new(
        dealloc_pages: FixedBitmap,
        active_size: usize,
        up1: u32,
        up2: u32,
        referenced_files: HashSet<u32>,
        meta: Arc<FileMeta>,
    ) -> Self {
        Self {
            dealloc_pages,
            active_size,
            up1,
            up2,
            referenced_files,
            meta,
        }
    }

    #[inline]
    pub(crate) fn get_file_id(&self) -> u32 {
        self.meta.get_file_id()
    }

    #[inline]
    pub(crate) fn is_empty(&self) -> bool {
        self.dealloc_pages.is_full()
    }

    #[inline]
    pub(crate) fn is_obsoleted(&self, active_files: &HashSet<u32>) -> bool {
        self.is_empty()
            && self
                .referenced_files
                .iter()
                .all(|id| !active_files.contains(id))
    }

    pub(crate) fn deactivate_page(&mut self, now: u32, page_addr: u64) -> bool {
        let Some((index, _, page_size)) = self.meta.get_page_handle(page_addr) else {
            return false;
        };

        if self.dealloc_pages.set(index as u32) {
            self.active_size -= page_size;
            if self.up1 < now {
                self.up2 = self.up1;
                self.up1 = now;
            }
            return true;
        }

        false
    }

    /// Get the [`PageHandle`] of the corresponding page. Returns `None` if no
    /// such active page exists.
    pub(crate) fn get_page_handle(&self, page_addr: u64) -> Option<PageHandle> {
        let (index, offset, size) = self.meta.get_page_handle(page_addr)?;
        if self.dealloc_pages.test(index as u32) {
            return None;
        }

        Some(PageHandle {
            offset: offset as u32,
            size: size as u32,
        })
    }

    /// Get the [`PageInfo`] of the specified page.
    #[inline]
    pub(crate) fn get_page_info(&self, page_addr: u64) -> Option<PageInfo> {
        self.meta.get_page_info(page_addr)
    }

    #[inline]
    pub(crate) fn meta(&self) -> Arc<FileMeta> {
        self.meta.clone()
    }

    #[cfg(test)]
    pub(crate) fn is_page_active(&self, page_addr: u64) -> bool {
        self.meta
            .get_page_handle(page_addr)
            .map(|(index, _, _)| index as u32)
            .map(|index| !self.dealloc_pages.test(index))
            .unwrap_or_default()
    }

    #[inline]
    pub(crate) fn up1(&self) -> u32 {
        self.up1
    }

    #[inline]
    pub(crate) fn up2(&self) -> u32 {
        self.up2
    }

    #[inline]
    pub(crate) fn num_active_pages(&self) -> usize {
        self.dealloc_pages.free() as usize
    }

    #[inline]
    pub(crate) fn total_pages(&self) -> usize {
        self.meta.total_pages()
    }

    #[inline]
    pub(crate) fn total_page_size(&self) -> usize {
        self.meta.total_page_size()
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
    pub(crate) fn effective_rate(&self) -> f64 {
        let active_size = self.active_size as f64;
        let file_size = self.meta.total_page_size() as f64;
        active_size / file_size
    }

    #[inline]
    pub(crate) fn file_size(&self) -> usize {
        self.meta.file_size()
    }

    /// Return the id of the map file this file belongs to. `None` is returned
    /// if this file is not a partial page file.
    #[inline]
    pub(crate) fn get_map_file_id(&self) -> Option<u32> {
        self.meta.belong_to
    }

    #[inline]
    pub(crate) fn iter(&self) -> FileInfoIterator {
        FileInfoIterator::new(self)
    }
}

/// The immutable metadata for page file.
pub(crate) struct FileMeta {
    file_id: u32,
    /// The offset in map files.
    base_offset: u64,
    file_size: usize,
    block_size: usize,

    /// The id of map file which contains the page file.
    belong_to: Option<u32>,

    page_meta_map: HashMap<u32, PageMeta>,

    // [0] -> page_table, [1] ->  delete page, [2], meta_block_end
    meta_indexes: Vec<u64>,

    compression: Compression,
    checksum_type: ChecksumType,
}

struct PageMeta {
    info: PageInfo,
    index: u32,
    offset: u32,
    /// The disk size of page
    size: u32,
}

impl FileMeta {
    pub(crate) fn new(
        file_id: u32,
        file_size: usize,
        block_size: usize,
        meta_indexes: Vec<u64>,
        data_offsets: BTreeMap<u64, (u64, PageInfo)>,
        compression: Compression,
        checksum_type: ChecksumType,
    ) -> Self {
        let page_meta_map = Self::build_page_meta_map(data_offsets, &meta_indexes);
        Self {
            file_id,
            file_size,
            base_offset: 0,
            belong_to: None,
            meta_indexes,
            page_meta_map,
            block_size,
            compression,
            checksum_type,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_partial(
        file_id: u32,
        map_file_id: u32,
        base_offset: u64,
        block_size: usize,
        meta_indexes: Vec<u64>,
        data_offsets: BTreeMap<u64, (u64, PageInfo)>,
        compression: Compression,
        checksum_type: ChecksumType,
    ) -> Self {
        let page_metas = Self::build_page_meta_map(data_offsets, &meta_indexes);
        FileMeta {
            file_id,
            file_size: 0,
            base_offset,
            block_size,
            belong_to: Some(map_file_id),
            meta_indexes,
            page_meta_map: page_metas,
            compression,
            checksum_type,
        }
    }

    #[inline]
    pub(crate) fn get_file_id(&self) -> u32 {
        self.file_id
    }

    #[inline]
    pub(crate) fn file_size(&self) -> usize {
        self.file_size
    }

    /// Returns the page size for the page specified by `page_addr`.
    pub(crate) fn get_page_handle(
        &self,
        page_addr: u64,
    ) -> Option<(
        usize, /* index */
        u64,   /* offset */
        usize, /* size */
    )> {
        let addr = page_addr as u32;
        if let Some(page_meta) = self.page_meta_map.get(&addr) {
            return Some((
                page_meta.index as usize,
                page_meta.offset as u64,
                page_meta.size as usize,
            ));
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
        let size = self.page_table_offset().saturating_sub(self.base_offset);
        if size == 0 {
            1
        } else {
            size as usize
        }
    }

    fn page_table_offset(&self) -> u64 {
        **self.meta_indexes.first().as_ref().unwrap()
    }

    /// Return the block_size for the file's device.
    #[inline]
    pub(crate) fn block_size(&self) -> usize {
        self.block_size
    }

    #[inline]
    pub(crate) fn compression(&self) -> Compression {
        self.compression
    }

    #[inline]
    pub(crate) fn checksum_type(&self) -> ChecksumType {
        self.checksum_type
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

    #[inline]
    pub(crate) fn dealloc_pages_bitmap(&self) -> FixedBitmap {
        FixedBitmap::new(self.page_meta_map.len() as u32)
    }

    fn build_page_meta_map(
        data_offsets: BTreeMap<u64, (u64, PageInfo)>,
        meta_indexes: &[u64],
    ) -> HashMap<u32, PageMeta> {
        let mut page_metas = HashMap::with_capacity(data_offsets.len());
        let mut last_addr = None;
        for (index, (addr, (offset, info))) in data_offsets.into_iter().enumerate() {
            let addr = addr as u32;
            let offset = offset as u32;
            let index = index as u32;
            page_metas.insert(
                addr,
                PageMeta {
                    info,
                    index,
                    offset,
                    size: 0,
                },
            );
            if let Some(last_addr) = last_addr {
                let meta = page_metas
                    .get_mut(&last_addr)
                    .expect("The last one is existed");
                meta.size = offset
                    .checked_sub(meta.offset)
                    .expect("The offset is increasing");
            }
            last_addr = Some(addr);
        }
        if let Some(last_addr) = last_addr {
            let meta = page_metas
                .get_mut(&last_addr)
                .expect("The last one is existed");

            // it's the last page use total-page-size as end val.
            let page_table_offset = meta_indexes[0] as u32;
            meta.size = page_table_offset
                .checked_sub(meta.offset)
                .expect("The offset is increasing");
        }
        page_metas
    }
}

#[derive(Clone)]
pub(crate) struct MapFileInfo {
    up1: u32,
    up2: u32,

    /// The referenced page files, this should be placed in meta.
    referenced_files: HashSet<u32>,

    meta: Arc<MapFileMeta>,
}

impl MapFileInfo {
    pub(crate) fn new(
        up1: u32,
        up2: u32,
        referenced_files: HashSet<u32>,
        meta: Arc<MapFileMeta>,
    ) -> Self {
        MapFileInfo {
            up1,
            up2,
            referenced_files,
            meta,
        }
    }

    pub(crate) fn get_referenced_files(&self) -> &HashSet<u32> {
        &self.referenced_files
    }

    pub(crate) fn on_update(&mut self, now: u32) {
        if self.up1 < now {
            self.up2 = self.up1;
            self.up1 = now;
        }
    }

    #[inline]
    pub(crate) fn file_id(&self) -> u32 {
        self.meta.file_id
    }

    #[inline]
    pub(crate) fn file_size(&self) -> usize {
        self.meta.file_size
    }

    #[inline]
    pub(crate) fn meta(&self) -> &Arc<MapFileMeta> {
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

#[allow(unused)]
pub(crate) struct MapFileMeta {
    file_id: u32,
    file_size: usize,
    block_size: usize,
    page_files: HashMap<u32, Arc<FileMeta>>,
}

#[allow(unused)]
impl MapFileMeta {
    pub(crate) fn new(
        file_id: u32,
        file_size: usize,
        block_size: usize,
        page_files: HashMap<u32, Arc<FileMeta>>,
    ) -> Self {
        MapFileMeta {
            file_id,
            file_size,
            block_size,
            page_files,
        }
    }

    #[inline]
    pub(crate) fn file_id(&self) -> u32 {
        self.file_id
    }

    #[inline]
    pub(crate) fn file_size(&self) -> usize {
        self.file_size
    }

    #[inline]
    pub(crate) fn block_size(&self) -> usize {
        self.block_size
    }

    #[inline]
    pub(crate) fn contains(&self, file_id: u32) -> bool {
        self.page_files.contains_key(&file_id)
    }

    #[inline]
    pub(crate) fn num_page_files(&self) -> usize {
        self.page_files.len()
    }

    #[inline]
    pub(crate) fn page_files(&self) -> &HashMap<u32, Arc<FileMeta>> {
        &self.page_files
    }
}

/// [`FileInfoIterator`] is used to traverse [`FileInfo`] to get the addr of all
/// active pages.
pub(crate) struct FileInfoIterator {
    file_id: u32,
    index: usize,
    active_pages: Vec<(u32, u32)>,
}

impl FileInfoIterator {
    fn new(info: &FileInfo) -> Self {
        let mut active_pages = info
            .meta
            .page_meta_map
            .iter()
            .filter(|(_, meta)| !info.dealloc_pages.test(meta.index))
            .map(|(&addr, meta)| (addr, meta.index))
            .collect::<Vec<_>>();
        active_pages.sort_by_key(|(_, index)| *index);
        let file_id = info.meta.file_id;
        FileInfoIterator {
            file_id,
            index: 0,
            active_pages,
        }
    }
}

impl Iterator for FileInfoIterator {
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
