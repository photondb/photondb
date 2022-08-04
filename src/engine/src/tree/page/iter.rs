use std::{
    cmp::{Ord, Ordering},
    collections::BinaryHeap,
    ops::{Deref, DerefMut},
};

use super::Comparable;

pub trait ForwardIter {
    type Item;

    /// Returns the last item.
    fn last(&self) -> Option<&Self::Item>;

    /// Advances to the next item and returns it.
    fn next(&mut self) -> Option<&Self::Item>;

    /// Skips the next `n` items.
    fn skip(&mut self, mut n: usize) {
        while n > 0 && self.next().is_some() {
            n -= 1;
        }
    }

    /// Skips all items to the end.
    fn skip_all(&mut self) {
        while self.next().is_some() {}
    }
}

impl<I: ForwardIter> ForwardIter for &mut I {
    type Item = I::Item;

    fn last(&self) -> Option<&Self::Item> {
        (**self).last()
    }

    fn next(&mut self) -> Option<&Self::Item> {
        (**self).next()
    }

    fn skip(&mut self, n: usize) {
        (**self).skip(n)
    }

    fn skip_all(&mut self) {
        (**self).skip_all()
    }
}

pub trait RewindableIter: ForwardIter {
    /// Positions the next item at the front.
    fn rewind(&mut self);
}

impl<I: RewindableIter> RewindableIter for &mut I {
    fn rewind(&mut self) {
        (**self).rewind()
    }
}

pub trait SeekableIter<T: ?Sized>: ForwardIter {
    /// Positions the next item at or after `target`.
    fn seek(&mut self, target: &T);
}

impl<T, I> SeekableIter<T> for &mut I
where
    T: ?Sized,
    I: SeekableIter<T>,
{
    fn seek(&mut self, target: &T) {
        (**self).seek(target)
    }
}

/// A wrapper that turns a slice into a `SeekableIter` and `RewindableIter`.
pub struct SliceIter<'a, I> {
    data: &'a [I],
    iter: std::slice::Iter<'a, I>,
    last: Option<&'a I>,
}

impl<'a, I> SliceIter<'a, I> {
    pub fn new(data: &'a [I]) -> Self {
        SliceIter {
            data,
            iter: data.iter(),
            last: None,
        }
    }
}

impl<'a, I> ForwardIter for SliceIter<'a, I> {
    type Item = I;

    fn last(&self) -> Option<&I> {
        self.last
    }

    fn next(&mut self) -> Option<&I> {
        self.last = self.iter.next();
        self.last
    }
}

impl<'a, I> RewindableIter for SliceIter<'a, I> {
    fn rewind(&mut self) {
        self.iter = self.data.iter();
        self.last = None;
    }
}

impl<'a, I> SeekableIter<I> for SliceIter<'a, I>
where
    I: Ord,
{
    fn seek(&mut self, target: &I) {
        let index = match self.data.binary_search_by(|item| item.cmp(target)) {
            Ok(i) => i,
            Err(i) => i,
        };
        self.iter = self.data[index..].iter();
        self.last = None;
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

/// A wrapper that turns an option into a `RewindableIter`.
pub struct OptionIter<I> {
    next: Option<I>,
    last: Option<I>,
}

impl<I> OptionIter<I> {
    pub fn new(next: Option<I>) -> Self {
        OptionIter { next, last: None }
    }
}

impl<I> ForwardIter for OptionIter<I> {
    type Item = I;

    fn last(&self) -> Option<&I> {
        self.last.as_ref()
    }

    fn next(&mut self) -> Option<&I> {
        if let Some(next) = self.next.take() {
            self.last = Some(next);
            self.last.as_ref()
        } else {
            None
        }
    }
}

impl<I> RewindableIter for OptionIter<I> {
    fn rewind(&mut self) {
        if let Some(last) = self.last.take() {
            self.next = Some(last);
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
    pub fn new(mut iter: I, start: usize) -> Self {
        iter.skip(start);
        Self { iter, start }
    }
}

impl<I> ForwardIter for BoundedIter<I>
where
    I: ForwardIter,
{
    type Item = I::Item;

    fn last(&self) -> Option<&Self::Item> {
        self.iter.last()
    }

    fn next(&mut self) -> Option<&Self::Item> {
        self.iter.next()
    }

    fn skip(&mut self, n: usize) {
        self.iter.skip(n)
    }

    fn skip_all(&mut self) {
        self.iter.skip_all()
    }
}

impl<I> RewindableIter for BoundedIter<I>
where
    I: RewindableIter,
{
    fn rewind(&mut self) {
        self.iter.rewind();
        self.iter.skip(self.start);
    }
}

/// A wrapper to sorts iterators by their last entries in reverse order.
struct ReverseIter<I> {
    iter: I,
    rank: usize,
}

impl<I> Deref for ReverseIter<I> {
    type Target = I;

    fn deref(&self) -> &Self::Target {
        &self.iter
    }
}

impl<I> DerefMut for ReverseIter<I> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.iter
    }
}

impl<I> Eq for ReverseIter<I>
where
    I: ForwardIter,
    I::Item: Comparable<I::Item>,
{
}

impl<I> PartialEq for ReverseIter<I>
where
    I: ForwardIter,
    I::Item: Comparable<I::Item>,
{
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl<I> Ord for ReverseIter<I>
where
    I: ForwardIter,
    I::Item: Comparable<I::Item>,
{
    fn cmp(&self, other: &Self) -> Ordering {
        let mut ord = match (self.last(), other.last()) {
            (Some(a), Some(b)) => b.compare(a),
            (Some(_), None) => Ordering::Greater,
            (None, Some(_)) => Ordering::Less,
            (None, None) => Ordering::Equal,
        };
        if ord == Ordering::Equal {
            ord = other.rank.cmp(&self.rank);
        }
        ord
    }
}

impl<I> PartialOrd for ReverseIter<I>
where
    I: ForwardIter,
    I::Item: Comparable<I::Item>,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// An iterator that merges entries from multiple iterators in ascending order.
pub struct MergingIter<I>
where
    I: ForwardIter,
    I::Item: Comparable<I::Item>,
{
    heap: BinaryHeap<ReverseIter<I>>,
    children: Vec<ReverseIter<I>>,
}

impl<I> MergingIter<I>
where
    I: ForwardIter,
    I::Item: Comparable<I::Item>,
{
    fn new(children: Vec<ReverseIter<I>>) -> Self {
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
            f(iter);
        }
        std::mem::swap(&mut self.children, &mut children);
    }

    fn init_heap(&mut self) {
        let mut children = std::mem::take(&mut self.children);
        for iter in children.iter_mut() {
            iter.next();
        }
        let mut heap = BinaryHeap::from(children);
        std::mem::swap(&mut self.heap, &mut heap);
    }

    fn take_children(&mut self) -> Vec<ReverseIter<I>> {
        if self.heap.is_empty() {
            std::mem::take(&mut self.children)
        } else {
            std::mem::take(&mut self.heap).into_vec()
        }
    }
}

impl<I> ForwardIter for MergingIter<I>
where
    I: ForwardIter,
    I::Item: Comparable<I::Item>,
{
    type Item = I::Item;

    fn last(&self) -> Option<&Self::Item> {
        self.heap.peek().and_then(|iter| iter.last())
    }

    fn next(&mut self) -> Option<&Self::Item> {
        if self.heap.is_empty() {
            self.init_heap();
        } else if let Some(mut iter) = self.heap.peek_mut() {
            iter.next();
        }
        self.heap.peek().and_then(|iter| iter.last())
    }

    fn skip_all(&mut self) {
        self.reset(|iter| iter.skip_all());
    }
}

impl<I> RewindableIter for MergingIter<I>
where
    I: RewindableIter,
    I::Item: Comparable<I::Item>,
{
    fn rewind(&mut self) {
        self.reset(|iter| iter.rewind());
    }
}

impl<T, I> SeekableIter<T> for MergingIter<I>
where
    T: ?Sized,
    I: SeekableIter<T>,
    I::Item: Comparable<I::Item>,
{
    fn seek(&mut self, target: &T) {
        self.reset(|iter| iter.seek(target));
    }
}

/// A builder to create `MergingIter`.
pub struct MergingIterBuilder<I> {
    children: Vec<ReverseIter<I>>,
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
    I::Item: Comparable<I::Item>,
{
    pub fn with_exact(len: usize) -> Self {
        let mut children = Vec::new();
        children.reserve_exact(len);
        Self { children }
    }

    pub fn add(&mut self, iter: I) {
        let rank = self.children.len();
        self.children.push(ReverseIter { iter, rank });
    }

    pub fn build(self) -> MergingIter<I> {
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
            assert_eq!(iter.last(), None);
            assert_eq!(iter.next(), Some(&1));
            assert_eq!(iter.last(), Some(&1));
            assert_eq!(iter.next(), Some(&2));
            assert_eq!(iter.last(), Some(&2));
            assert_eq!(iter.next(), None);
            iter.rewind();
        }

        iter.rewind();
        iter.skip_all();
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn option_iter() {
        let mut iter = OptionIter::from(1);
        for _ in 0..2 {
            assert_eq!(iter.last(), None);
            assert_eq!(iter.next(), Some(&1));
            assert_eq!(iter.last(), Some(&1));
            assert_eq!(iter.next(), None);
            iter.rewind();
        }
    }

    #[test]
    fn bounded_iter() {
        let iter = SliceIter::from(&[1, 2, 3]);
        let mut iter = BoundedIter::new(iter, 1);
        for _ in 0..2 {
            assert_eq!(iter.next(), Some(&2));
            assert_eq!(iter.next(), Some(&3));
            assert_eq!(iter.next(), None);
            iter.rewind();
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

        // Tests next() and rewind()
        for _ in 0..2 {
            assert_eq!(iter.last(), None);
            for item in sorted_data.iter() {
                assert_eq!(iter.next(), Some(item));
                assert_eq!(iter.last(), Some(item));
            }
            assert_eq!(iter.next(), None);
            iter.rewind();
        }

        // Tests seek()
        iter.seek(&0);
        assert_eq!(iter.next(), Some(&1));
        iter.seek(&9);
        assert_eq!(iter.next(), None);
        iter.seek(&1);
        assert_eq!(iter.next(), Some(&1));
        iter.seek(&3);
        assert_eq!(iter.next(), Some(&3));
        iter.seek(&5);
        assert_eq!(iter.next(), Some(&7));

        // Tests skip_all()
        iter.rewind();
        iter.skip_all();
        assert_eq!(iter.next(), None);
    }
}
