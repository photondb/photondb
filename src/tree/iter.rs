use crate::page::*;

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
    type Item = SortedItem<'a, V>;

    fn next(&mut self) -> Option<Self::Item> {
        for SortedItem(key, value) in &mut self.iter {
            if let Some(limit) = self.range_limit {
                if key >= limit {
                    return None;
                }
            }
            return Some(SortedItem(key, value));
        }
        None
    }
}

impl<'a, V> RewindableIterator for MergingPageIter<'a, V> {
    fn rewind(&mut self) {
        self.iter.rewind();
    }
}

pub(super) struct MergingLeafPageIter<'a> {
    iter: MergingPageIter<'a, Value<'a>>,
}

impl<'a> MergingLeafPageIter<'a> {
    pub(super) fn new(iter: MergingPageIter<'a, Value<'a>>) -> Self {
        Self { iter }
    }
}

impl<'a> Iterator for MergingLeafPageIter<'a> {
    type Item = SortedItem<'a, Value<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        for SortedItem(key, value) in &mut self.iter {
            todo!()
        }
        None
    }
}

impl<'a> RewindableIterator for MergingLeafPageIter<'a> {
    fn rewind(&mut self) {
        self.iter.rewind();
    }
}

pub(super) struct MergingInnerPageIter<'a> {
    iter: MergingPageIter<'a, Index>,
}

impl<'a> MergingInnerPageIter<'a> {
    pub(super) fn new(iter: MergingPageIter<'a, Index>) -> Self {
        Self { iter }
    }
}

impl<'a> Iterator for MergingInnerPageIter<'a> {
    type Item = SortedItem<'a, Index>;

    fn next(&mut self) -> Option<Self::Item> {
        for SortedItem(key, value) in &mut self.iter {
            todo!()
        }
        None
    }
}

impl<'a> RewindableIterator for MergingInnerPageIter<'a> {
    fn rewind(&mut self) {
        self.iter.rewind();
    }
}
