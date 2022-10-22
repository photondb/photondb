use crate::{page::*, page_store::*};

pub(super) struct MergingPageIter<'a, V> {
    iter: MergingIter<SortedPageIter<'a, V>>,
    range_limit: Option<Key<'a>>,
}

impl<'a, V> MergingPageIter<'a, V> {
    pub(super) fn new(
        iter: MergingIter<SortedPageIter<'a, V>>,
        range_limit: Option<Key<'a>>,
    ) -> Self {
        Self { iter, range_limit }
    }
}

impl<'a, V> Iterator for MergingPageIter<'a, V> {
    type Item = (Key<'a>, V);

    fn next(&mut self) -> Option<Self::Item> {
        for (key, value) in &mut self.iter {
            if let Some(limit) = self.range_limit {
                if key >= limit {
                    return None;
                }
            }
            return Some((key, value));
        }
        None
    }
}

impl<'a, V> RewindableIterator for MergingPageIter<'a, V> {
    fn rewind(&mut self) {
        self.iter.rewind();
    }
}

/// An iterator that merges multiple leaf pages for consolidation.
pub(super) struct MergingLeafPageIter<'a> {
    iter: MergingPageIter<'a, Value<'a>>,
    last_raw: Option<&'a [u8]>,
}

impl<'a> MergingLeafPageIter<'a> {
    pub(super) fn new(iter: MergingPageIter<'a, Value<'a>>) -> Self {
        Self {
            iter,
            last_raw: None,
        }
    }
}

impl<'a> Iterator for MergingLeafPageIter<'a> {
    type Item = (Key<'a>, Value<'a>);

    fn next(&mut self) -> Option<Self::Item> {
        // TODO: We should keep all versions visible at and after the safe LSN.
        for (key, value) in &mut self.iter {
            if let Some(raw) = self.last_raw {
                if key.raw == raw {
                    continue;
                }
            }
            self.last_raw = Some(key.raw);
            return Some((key, value));
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
    iter: MergingPageIter<'a, Index>,
    last_raw: Option<&'a [u8]>,
}

impl<'a> MergingInnerPageIter<'a> {
    pub(super) fn new(iter: MergingPageIter<'a, Index>) -> Self {
        Self {
            iter,
            last_raw: None,
        }
    }
}

impl<'a> Iterator for MergingInnerPageIter<'a> {
    type Item = (Key<'a>, Index);

    fn next(&mut self) -> Option<Self::Item> {
        for (start, index) in &mut self.iter {
            // Skip placeholders
            if index.id == NAN_ID {
                continue;
            }
            // Skip overwritten keys.
            if let Some(raw) = self.last_raw {
                if start.raw == raw {
                    continue;
                }
            }
            self.last_raw = Some(start.raw);
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
