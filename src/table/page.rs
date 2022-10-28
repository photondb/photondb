use crate::{page::*, page_store::*};

/// The root id is always the minimal id in the page store.
pub(super) const ROOT_ID: u64 = MIN_ID;
pub(super) const ROOT_RANGE: Range = Range::full();
pub(super) const ROOT_INDEX: Index = Index::new(MIN_ID, 0);
pub(super) const NULL_INDEX: Index = Index::new(NAN_ID, 0);

pub(super) struct PageView<'a> {
    pub(super) id: u64,
    pub(super) addr: u64,
    pub(super) page: PageRef<'a>,
    pub(super) range: Option<Range<'a>>,
}

pub(super) struct MergingPageIter<'a, K, V>
where
    K: DecodeFrom + Ord,
    V: DecodeFrom,
{
    iter: MergingIter<SortedPageIter<'a, K, V>>,
    limit: Option<&'a [u8]>,
}

impl<'a, K, V> MergingPageIter<'a, K, V>
where
    K: DecodeFrom + Ord,
    V: DecodeFrom,
{
    pub(super) fn new(
        iter: MergingIter<SortedPageIter<'a, K, V>>,
        limit: Option<&'a [u8]>,
    ) -> Self {
        Self { iter, limit }
    }
}

/// An iterator that merges multiple leaf pages for consolidation.
pub(super) struct MergingLeafPageIter<'a> {
    iter: MergingIter<SortedPageIter<'a, Key<'a>, Value<'a>>>,
    last: Option<&'a [u8]>,
    limit: Option<&'a [u8]>,
}

impl<'a> MergingLeafPageIter<'a> {
    pub(super) fn new(iter: MergingPageIter<'a, Key<'a>, Value<'a>>) -> Self {
        Self {
            iter: iter.iter,
            last: None,
            limit: iter.limit,
        }
    }
}

impl<'a> Iterator for MergingLeafPageIter<'a> {
    type Item = (Key<'a>, Value<'a>);

    fn next(&mut self) -> Option<Self::Item> {
        // TODO: We should keep all versions visible at and after the safe LSN.
        for (k, v) in &mut self.iter {
            if let Some(last) = self.last {
                if k.raw == last {
                    continue;
                }
            }
            self.last = Some(k.raw);
            if let Some(limit) = self.limit {
                if k.raw >= limit {
                    return None;
                }
            }
            return Some((k, v));
        }
        None
    }
}

impl<'a> RewindableIterator for MergingLeafPageIter<'a> {
    fn rewind(&mut self) {
        self.iter.rewind();
    }
}

/// An iterator that merges multiple inner pages for consolidation.
pub(super) struct MergingInnerPageIter<'a> {
    iter: MergingIter<SortedPageIter<'a, &'a [u8], Index>>,
    last: Option<&'a [u8]>,
    limit: Option<&'a [u8]>,
}

impl<'a> MergingInnerPageIter<'a> {
    pub(super) fn new(iter: MergingPageIter<'a, &'a [u8], Index>) -> Self {
        Self {
            iter: iter.iter,
            last: None,
            limit: iter.limit,
        }
    }
}

impl<'a> Iterator for MergingInnerPageIter<'a> {
    type Item = (&'a [u8], Index);

    fn next(&mut self) -> Option<Self::Item> {
        for (start, index) in &mut self.iter {
            // Skip placeholders
            if index == NULL_INDEX {
                continue;
            }
            // Skip overwritten indexes
            if let Some(last) = self.last {
                if start == last {
                    continue;
                }
            }
            self.last = Some(start);
            if let Some(limit) = self.limit {
                if start >= limit {
                    return None;
                }
            }
            return Some((start, index));
        }
        None
    }
}

impl<'a> RewindableIterator for MergingInnerPageIter<'a> {
    fn rewind(&mut self) {
        self.iter.rewind();
    }
}
