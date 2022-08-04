use std::{
    cmp::{Ord, Ordering},
    collections::BinaryHeap,
    ops::{Deref, DerefMut},
    slice,
};

pub trait Comparable<T> {
    fn compare(&self, other: &T) -> Ordering;

    fn eq(&self, other: &T) -> bool {
        self.compare(other) == Ordering::Equal
    }

    fn lt(&self, other: &T) -> bool {
        self.compare(other) == Ordering::Less
    }

    fn le(&self, other: &T) -> bool {
        self.compare(other) != Ordering::Greater
    }

    fn gt(&self, other: &T) -> bool {
        self.compare(other) == Ordering::Greater
    }

    fn ge(&self, other: &T) -> bool {
        self.compare(other) != Ordering::Less
    }
}

impl Comparable<u64> for u64 {
    fn compare(&self, other: &u64) -> Ordering {
        self.cmp(other)
    }
}

impl Comparable<&[u8]> for &[u8] {
    fn compare(&self, other: &&[u8]) -> Ordering {
        self.cmp(other)
    }
}

pub trait ForwardIter {
    type Item;

    /// Returns the last item.
    fn last(&self) -> Option<&Self::Item>;

    /// Advances to the next item and returns it.
    fn next(&mut self) -> Option<&Self::Item>;

    /// Skips all items to the end.
    fn skip_all(&mut self) {
        while self.next().is_some() {}
    }
}

pub trait SeekableIter: ForwardIter {
    /// Positions the next item at or after `target`.
    fn seek<T>(&mut self, target: &T)
    where
        T: Comparable<Self::Item>;
}

pub trait RewindableIter: ForwardIter {
    /// Positions the next item at the front.
    fn rewind(&mut self);
}

/// A wrapper that turns a slice into a `SeekableIter` and `RewindableIter`.
pub struct SliceIter<'a, I> {
    data: &'a [I],
    iter: slice::Iter<'a, I>,
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

impl<'a, I> ForwardIter for SliceIter<'a, I>
where
    I: Ord,
{
    type Item = I;

    fn last(&self) -> Option<&I> {
        self.last
    }

    fn next(&mut self) -> Option<&I> {
        self.last = self.iter.next();
        self.last
    }
}

impl<'a, I> SeekableIter for SliceIter<'a, I>
where
    I: Ord,
{
    fn seek<T>(&mut self, target: &T)
    where
        T: Comparable<I>,
    {
        let index = match self
            .data
            .binary_search_by(|item| target.compare(item).reverse())
        {
            Ok(i) => i,
            Err(i) => i,
        };
        self.iter = self.data[index..].iter();
        self.last = None;
    }
}

impl<'a, I> RewindableIter for SliceIter<'a, I>
where
    I: Ord,
{
    fn rewind(&mut self) {
        self.iter = self.data.iter();
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

impl<I> ForwardIter for OptionIter<I>
where
    I: Ord,
{
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

impl<I> RewindableIter for OptionIter<I>
where
    I: Ord,
{
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
    I::Item: Ord,
{
}

impl<I> PartialEq for ReverseIter<I>
where
    I: ForwardIter,
    I::Item: Ord,
{
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl<I> Ord for ReverseIter<I>
where
    I: ForwardIter,
    I::Item: Ord,
{
    fn cmp(&self, other: &Self) -> Ordering {
        let mut ord = match (self.last(), other.last()) {
            (Some(a), Some(b)) => b.cmp(&a),
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
    I::Item: Ord,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// An iterator that merges entries from multiple iterators in ascending order.
pub struct MergingIter<I>
where
    I: ForwardIter,
    I::Item: Ord,
{
    heap: BinaryHeap<ReverseIter<I>>,
    children: Vec<ReverseIter<I>>,
}

impl<I> MergingIter<I>
where
    I: ForwardIter,
    I::Item: Ord,
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
    I::Item: Ord,
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
        self.last()
    }

    fn skip_all(&mut self) {
        self.reset(|iter| iter.skip_all());
    }
}

impl<I> SeekableIter for MergingIter<I>
where
    I: SeekableIter,
    I::Item: Ord,
{
    fn seek<T>(&mut self, target: &T)
    where
        T: Comparable<I::Item>,
    {
        self.reset(|iter| iter.seek(target));
    }
}

impl<I> RewindableIter for MergingIter<I>
where
    I: RewindableIter,
    I::Item: Ord,
{
    fn rewind(&mut self) {
        self.reset(|iter| iter.rewind());
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
    I::Item: Ord,
{
    pub fn len(&self) -> usize {
        self.children.len()
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
