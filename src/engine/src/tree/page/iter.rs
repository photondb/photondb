use std::{
    cmp::{Ord, Ordering},
    collections::BinaryHeap,
    fmt::Debug,
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
    type Key: Ord;
    type Value;

    /// Returns the last entry.
    fn last(&self) -> Option<&(Self::Key, Self::Value)>;

    /// Advances to the next entry and returns it.
    fn next(&mut self) -> Option<&(Self::Key, Self::Value)>;

    /// Skips all entries.
    fn skip_all(&mut self) {
        while let Some(_) = self.next() {}
    }
}

pub trait SeekableIter: ForwardIter {
    /// Positions the next entry at or after `target`.
    fn seek<T>(&mut self, target: &T)
    where
        T: Comparable<Self::Key>;
}

pub trait RewindableIter: ForwardIter {
    /// Positions the next entry at the front.
    fn rewind(&mut self);
}

/// Extends `ForwardIter` with a method to print entries.
pub trait PrintableIter: ForwardIter {
    fn print(&mut self);
}

impl<I> PrintableIter for I
where
    I: ForwardIter,
    I::Key: Debug,
    I::Value: Debug,
{
    fn print(&mut self) {
        while let Some(ent) = self.next() {
            println!("{:?}", ent);
        }
    }
}

/// A wrapper that turns a slice into a `SeekableIter` and `RewindableIter`.
pub struct SliceIter<'a, K, V> {
    data: &'a [(K, V)],
    iter: slice::Iter<'a, (K, V)>,
    last: Option<&'a (K, V)>,
}

impl<'a, K, V> SliceIter<'a, K, V> {
    pub fn new(data: &'a [(K, V)]) -> Self {
        SliceIter {
            data,
            iter: data.iter(),
            last: None,
        }
    }
}

impl<'a, K, V> ForwardIter for SliceIter<'a, K, V>
where
    K: Ord,
{
    type Key = K;
    type Value = V;

    fn last(&self) -> Option<&(K, V)> {
        self.last
    }

    fn next(&mut self) -> Option<&(K, V)> {
        self.last = self.iter.next();
        self.last
    }
}

impl<'a, K, V> SeekableIter for SliceIter<'a, K, V>
where
    K: Ord,
{
    fn seek<T>(&mut self, target: &T)
    where
        T: Comparable<K>,
    {
        let index = match self
            .data
            .binary_search_by(|(k, _)| target.compare(k).reverse())
        {
            Ok(i) => i,
            Err(i) => i,
        };
        self.iter = self.data[index..].iter();
        self.last = None;
    }
}

impl<'a, K, V> RewindableIter for SliceIter<'a, K, V>
where
    K: Ord,
{
    fn rewind(&mut self) {
        self.iter = self.data.iter();
        self.last = None;
    }
}

impl<'a, K, V> From<&'a [(K, V)]> for SliceIter<'a, K, V> {
    fn from(data: &'a [(K, V)]) -> Self {
        Self::new(data)
    }
}

impl<'a, K, V, const N: usize> From<&'a [(K, V); N]> for SliceIter<'a, K, V> {
    fn from(data: &'a [(K, V); N]) -> Self {
        Self::new(data.as_slice())
    }
}

/// A wrapper that turns an option into a `RewindableIter`.
pub struct OptionIter<K, V> {
    next: Option<(K, V)>,
    last: Option<(K, V)>,
}

impl<K, V> OptionIter<K, V> {
    pub fn new(next: Option<(K, V)>) -> Self {
        OptionIter { next, last: None }
    }
}

impl<K, V> ForwardIter for OptionIter<K, V>
where
    K: Ord,
{
    type Key = K;
    type Value = V;

    fn last(&self) -> Option<&(K, V)> {
        self.last.as_ref()
    }

    fn next(&mut self) -> Option<&(K, V)> {
        if let Some(next) = self.next.take() {
            self.last = Some(next);
            self.last.as_ref()
        } else {
            None
        }
    }
}

impl<K, V> RewindableIter for OptionIter<K, V>
where
    K: Ord,
{
    fn rewind(&mut self) {
        if let Some(last) = self.last.take() {
            self.next = Some(last);
        }
    }
}

impl<K, V> From<(K, V)> for OptionIter<K, V> {
    fn from(item: (K, V)) -> Self {
        Self::new(Some(item))
    }
}

impl<K, V> From<Option<(K, V)>> for OptionIter<K, V> {
    fn from(next: Option<(K, V)>) -> Self {
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

impl<I> Eq for ReverseIter<I> where I: ForwardIter {}

impl<I> PartialEq for ReverseIter<I>
where
    I: ForwardIter,
{
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl<I> Ord for ReverseIter<I>
where
    I: ForwardIter,
{
    fn cmp(&self, other: &Self) -> Ordering {
        let ord = match (self.last(), other.last()) {
            (Some(a), Some(b)) => b.0.cmp(&a.0),
            (Some(_), None) => Ordering::Greater,
            (None, Some(_)) => Ordering::Less,
            (None, None) => Ordering::Equal,
        };
        if ord != Ordering::Equal {
            ord
        } else {
            other.rank.cmp(&self.rank)
        }
    }
}

impl<I> PartialOrd for ReverseIter<I>
where
    I: ForwardIter,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// An iterator that merges entries from multiple iterators in ascending order.
pub struct MergingIter<I>
where
    I: ForwardIter,
{
    heap: BinaryHeap<ReverseIter<I>>,
    children: Vec<ReverseIter<I>>,
}

impl<I> MergingIter<I>
where
    I: ForwardIter,
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
        if !self.heap.is_empty() {
            std::mem::take(&mut self.heap).into_vec()
        } else {
            std::mem::take(&mut self.children)
        }
    }
}

impl<I> ForwardIter for MergingIter<I>
where
    I: ForwardIter,
{
    type Key = I::Key;
    type Value = I::Value;

    fn last(&self) -> Option<&(Self::Key, Self::Value)> {
        self.heap.peek().and_then(|iter| iter.last())
    }

    fn next(&mut self) -> Option<&(Self::Key, Self::Value)> {
        if let Some(mut iter) = self.heap.pop() {
            iter.next();
            self.heap.push(iter);
        } else {
            self.init_heap();
        }
        self.last()
    }
}

impl<I> SeekableIter for MergingIter<I>
where
    I: SeekableIter,
{
    fn seek<T>(&mut self, target: &T)
    where
        T: Comparable<I::Key>,
    {
        self.reset(|iter| iter.seek(target));
    }
}

impl<I> RewindableIter for MergingIter<I>
where
    I: RewindableIter,
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
{
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
        let mut iter = SliceIter::from(&[(1, 2), (3, 4)]);
        for _ in 0..2 {
            assert_eq!(iter.last(), None);
            assert_eq!(iter.next(), Some(&(1, 2)));
            assert_eq!(iter.last(), Some(&(1, 2)));
            assert_eq!(iter.next(), Some(&(3, 4)));
            assert_eq!(iter.last(), Some(&(3, 4)));
            assert_eq!(iter.next(), None);
            iter.rewind();
        }
    }

    #[test]
    fn option_iter() {
        let mut iter = OptionIter::from((1, 2));
        for _ in 0..2 {
            assert_eq!(iter.last(), None);
            assert_eq!(iter.next(), Some(&(1, 2)));
            assert_eq!(iter.last(), Some(&(1, 2)));
            assert_eq!(iter.next(), None);
            iter.rewind();
        }
    }

    #[test]
    fn merging_iter() {
        let data = [
            [(1, 0), (3, 0)],
            [(2, 0), (4, 0)],
            [(1, 0), (8, 0)],
            [(3, 0), (7, 0)],
        ];
        let sorted_data = [
            (1, 0),
            (1, 0),
            (2, 0),
            (3, 0),
            (3, 0),
            (4, 0),
            (7, 0),
            (8, 0),
        ];

        let mut merger = MergingIterBuilder::default();
        for item in data.iter() {
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
        assert_eq!(iter.next(), Some(&(1, 0)));
        iter.seek(&9);
        assert_eq!(iter.next(), None);
        iter.seek(&1);
        assert_eq!(iter.next(), Some(&(1, 0)));
        iter.seek(&3);
        assert_eq!(iter.next(), Some(&(3, 0)));
        iter.seek(&5);
        assert_eq!(iter.next(), Some(&(7, 0)));
    }
}
