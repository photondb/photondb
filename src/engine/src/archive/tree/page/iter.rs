use std::{
    cmp::{Ord, Ordering, Reverse},
    collections::BinaryHeap,
};

use super::Compare;

pub trait ForwardIter {
    type Item;

    /// Returns the current item.
    fn current(&self) -> Option<&Self::Item>;

    /// Rewinds back to the first item.
    fn rewind(&mut self);

    /// Advances to the next item.
    fn next(&mut self);

    /// Skips the next `n` items.
    fn skip(&mut self, mut n: usize) {
        while self.current().is_some() && n > 0 {
            self.next();
            n -= 1;
        }
    }

    /// Skips all items until the end.
    fn skip_all(&mut self) {
        while self.current().is_some() {
            self.next();
        }
    }
}

impl<I: ForwardIter> ForwardIter for &mut I {
    type Item = I::Item;

    #[inline]
    fn current(&self) -> Option<&Self::Item> {
        (**self).current()
    }

    #[inline]
    fn rewind(&mut self) {
        (**self).rewind()
    }

    #[inline]
    fn next(&mut self) {
        (**self).next()
    }

    #[inline]
    fn skip(&mut self, n: usize) {
        (**self).skip(n)
    }

    #[inline]
    fn skip_all(&mut self) {
        (**self).skip_all()
    }
}

pub trait SeekableIter<T: ?Sized>: ForwardIter {
    /// Positions the next item at or after `target`.
    fn seek(&mut self, target: &T);
}

impl<I, T> SeekableIter<T> for &mut I
where
    I: SeekableIter<T>,
    T: ?Sized,
{
    #[inline]
    fn seek(&mut self, target: &T) {
        (**self).seek(target)
    }
}

/// A wrapper that turns a slice into a `SeekableIter`.
pub struct SliceIter<'a, I> {
    data: &'a [I],
    index: usize,
    current: Option<&'a I>,
}

impl<'a, I> SliceIter<'a, I> {
    pub fn new(data: &'a [I]) -> Self {
        SliceIter {
            data,
            index: data.len(),
            current: None,
        }
    }
}

impl<'a, I> ForwardIter for SliceIter<'a, I> {
    type Item = I;

    fn current(&self) -> Option<&I> {
        self.current
    }

    fn rewind(&mut self) {
        self.index = 0;
        self.current = self.data.get(0);
    }

    fn next(&mut self) {
        self.index += 1;
        self.current = self.data.get(self.index);
    }
}

impl<'a, I> SeekableIter<I> for SliceIter<'a, I>
where
    I: Ord,
{
    fn seek(&mut self, target: &I) {
        self.index = match self.data.binary_search_by(|item| item.cmp(target)) {
            Ok(i) => i,
            Err(i) => i,
        };
        self.current = self.data.get(self.index);
    }
}

impl<'a, I> From<&'a [I]> for SliceIter<'a, I> {
    fn from(data: &'a [I]) -> Self {
        Self::new(data)
    }
}

impl<'a, I, const N: usize> From<&'a [I; N]> for SliceIter<'a, I> {
    fn from(data: &'a [I; N]) -> Self {
        Self::new(data.as_slice())
    }
}

/// A wrapper that turns an option into a `ForwardIter`.
pub struct OptionIter<I> {
    next: Option<I>,
    current: Option<I>,
}

impl<I> OptionIter<I> {
    pub fn new(next: Option<I>) -> Self {
        OptionIter {
            next,
            current: None,
        }
    }
}

impl<I> ForwardIter for OptionIter<I> {
    type Item = I;

    fn current(&self) -> Option<&I> {
        self.current.as_ref()
    }

    fn rewind(&mut self) {
        if let Some(item) = self.next.take() {
            self.current = Some(item);
        }
    }

    fn next(&mut self) {
        if let Some(item) = self.current.take() {
            self.next = Some(item);
        }
    }
}

impl<I> From<I> for OptionIter<I> {
    fn from(item: I) -> Self {
        Self::new(Some(item))
    }
}

impl<I> From<Option<I>> for OptionIter<I> {
    fn from(next: Option<I>) -> Self {
        Self::new(next)
    }
}

pub struct BoundedIter<I> {
    iter: I,
    start: usize,
}

impl<I> BoundedIter<I>
where
    I: ForwardIter,
{
    pub fn new(iter: I, start: usize) -> Self {
        Self { iter, start }
    }
}

impl<I> ForwardIter for BoundedIter<I>
where
    I: ForwardIter,
{
    type Item = I::Item;

    #[inline]
    fn current(&self) -> Option<&Self::Item> {
        self.iter.current()
    }

    #[inline]
    fn rewind(&mut self) {
        self.iter.rewind();
        self.iter.skip(self.start);
    }

    #[inline]
    fn next(&mut self) {
        self.iter.next()
    }

    #[inline]
    fn skip(&mut self, n: usize) {
        self.iter.skip(n)
    }

    #[inline]
    fn skip_all(&mut self) {
        self.iter.skip_all()
    }
}

/// A wrapper to order iterators by their current items and ranks.
pub struct OrderedIter<I> {
    iter: I,
    rank: usize,
}

impl<I> OrderedIter<I> {
    pub fn new(iter: I, rank: usize) -> Self {
        Self { iter, rank }
    }
}

impl<I> Eq for OrderedIter<I>
where
    I: ForwardIter,
    I::Item: Compare<I::Item>,
{
}

impl<I> PartialEq for OrderedIter<I>
where
    I: ForwardIter,
    I::Item: Compare<I::Item>,
{
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl<I> Ord for OrderedIter<I>
where
    I: ForwardIter,
    I::Item: Compare<I::Item>,
{
    fn cmp(&self, other: &Self) -> Ordering {
        let mut ord = match (self.current(), other.current()) {
            (Some(a), Some(b)) => a.compare(b),
            (Some(_), None) => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (None, None) => Ordering::Equal,
        };
        if ord == Ordering::Equal {
            ord = self.rank.cmp(&other.rank);
        }
        ord
    }
}

impl<I> PartialOrd for OrderedIter<I>
where
    I: ForwardIter,
    I::Item: Compare<I::Item>,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<I> ForwardIter for OrderedIter<I>
where
    I: ForwardIter,
{
    type Item = I::Item;

    #[inline]
    fn current(&self) -> Option<&Self::Item> {
        self.iter.current()
    }

    #[inline]
    fn rewind(&mut self) {
        self.iter.rewind();
    }

    #[inline]
    fn next(&mut self) {
        self.iter.next()
    }

    #[inline]
    fn skip(&mut self, n: usize) {
        self.iter.skip(n)
    }

    #[inline]
    fn skip_all(&mut self) {
        self.iter.skip_all()
    }
}

impl<I, T> SeekableIter<T> for OrderedIter<I>
where
    I: SeekableIter<T>,
    T: ?Sized,
{
    #[inline]
    fn seek(&mut self, target: &T) {
        self.iter.seek(target)
    }
}

/// An iterator that merges entries from multiple iterators in ascending order.
pub struct MergingIter<I>
where
    I: ForwardIter + Ord,
{
    heap: BinaryHeap<Reverse<I>>,
    children: Vec<Reverse<I>>,
}

impl<I> MergingIter<I>
where
    I: ForwardIter + Ord,
{
    fn new(children: Vec<Reverse<I>>) -> Self {
        Self {
            heap: BinaryHeap::default(),
            children,
        }
    }

    fn reset<F>(&mut self, f: F)
    where
        F: Fn(&mut I),
    {
        let mut children = self.take_children();
        for iter in children.iter_mut() {
            f(&mut iter.0);
        }
        let mut heap = BinaryHeap::from(children);
        std::mem::swap(&mut self.heap, &mut heap);
    }

    fn take_children(&mut self) -> Vec<Reverse<I>> {
        if self.heap.is_empty() {
            std::mem::take(&mut self.children)
        } else {
            std::mem::take(&mut self.heap).into_vec()
        }
    }
}

impl<I> ForwardIter for MergingIter<I>
where
    I: ForwardIter + Ord,
{
    type Item = I::Item;

    fn current(&self) -> Option<&Self::Item> {
        self.heap.peek().and_then(|iter| iter.0.current())
    }

    fn rewind(&mut self) {
        self.reset(|iter| iter.rewind());
    }

    fn next(&mut self) {
        if let Some(mut iter) = self.heap.peek_mut() {
            iter.0.next();
        }
    }

    fn skip_all(&mut self) {
        self.reset(|iter| iter.skip_all());
    }
}

impl<I, T> SeekableIter<T> for MergingIter<I>
where
    I: SeekableIter<T> + Ord,
    T: ?Sized,
{
    fn seek(&mut self, target: &T) {
        self.reset(|iter| iter.seek(target));
    }
}

/// A builder to create `MergingIter`.
pub struct MergingIterBuilder<I> {
    children: Vec<Reverse<OrderedIter<I>>>,
}

impl<I> Default for MergingIterBuilder<I> {
    fn default() -> Self {
        Self {
            children: Vec::new(),
        }
    }
}

impl<I> MergingIterBuilder<I>
where
    I: ForwardIter,
    I::Item: Compare<I::Item>,
{
    pub fn with_len(len: usize) -> Self {
        Self {
            children: Vec::with_capacity(len),
        }
    }

    pub fn len(&self) -> usize {
        self.children.len()
    }

    pub fn add(&mut self, iter: I) {
        let rank = self.children.len();
        let iter = OrderedIter::new(iter, rank);
        self.children.push(Reverse(iter));
    }

    pub fn build(self) -> MergingIter<OrderedIter<I>> {
        MergingIter::new(self.children)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slice_iter() {
        let mut iter = SliceIter::from(&[1, 2]);
        for _ in 0..2 {
            assert_eq!(iter.current(), None);
            iter.rewind();
            assert_eq!(iter.current(), Some(&1));
            iter.next();
            assert_eq!(iter.current(), Some(&2));
            iter.next();
            assert_eq!(iter.current(), None);
        }

        iter.rewind();
        iter.skip_all();
        assert_eq!(iter.current(), None);
    }

    #[test]
    fn option_iter() {
        let mut iter = OptionIter::from(1);
        for _ in 0..2 {
            assert_eq!(iter.current(), None);
            iter.rewind();
            assert_eq!(iter.current(), Some(&1));
            iter.next();
            assert_eq!(iter.current(), None);
        }
    }

    #[test]
    fn bounded_iter() {
        let iter = SliceIter::from(&[1, 2, 3]);
        let mut iter = BoundedIter::new(iter, 1);
        for _ in 0..2 {
            assert_eq!(iter.current(), None);
            iter.rewind();
            assert_eq!(iter.current(), Some(&2));
            iter.next();
            assert_eq!(iter.current(), Some(&3));
            iter.next();
            assert_eq!(iter.current(), None);
        }
    }

    #[test]
    fn merging_iter() {
        let input_data = [[1, 3], [2, 4], [1, 8], [3, 7]];
        let sorted_data = [1, 1, 2, 3, 3, 4, 7, 8];

        let mut merger = MergingIterBuilder::default();
        for item in input_data.iter() {
            merger.add(SliceIter::from(item));
        }
        let mut iter = merger.build();

        for _ in 0..2 {
            assert_eq!(iter.current(), None);
            iter.rewind();
            for item in sorted_data.iter() {
                assert_eq!(iter.current(), Some(item));
                iter.next();
            }
            assert_eq!(iter.current(), None);
        }

        iter.seek(&0);
        assert_eq!(iter.current(), Some(&1));
        iter.next();
        assert_eq!(iter.current(), Some(&1));
        iter.seek(&9);
        assert_eq!(iter.current(), None);
        iter.seek(&1);
        assert_eq!(iter.current(), Some(&1));
        iter.seek(&3);
        assert_eq!(iter.current(), Some(&3));
        iter.next();
        assert_eq!(iter.current(), Some(&3));
        iter.seek(&5);
        assert_eq!(iter.current(), Some(&7));
        iter.next();
        assert_eq!(iter.current(), Some(&8));

        iter.rewind();
        iter.skip_all();
        assert_eq!(iter.current(), None);
    }
}
