use std::{
    cmp::{Ord, Ordering},
    collections::BinaryHeap,
    ops::{Deref, DerefMut},
    slice,
};

pub trait ForwardIter {
    type Key;
    type Value;

    /// Returns the last entry.
    fn last(&self) -> Option<&(Self::Key, Self::Value)>;

    /// Advances to the next entry and returns it.
    fn next(&mut self) -> Option<&(Self::Key, Self::Value)>;
}

pub trait SeekableIter: ForwardIter {
    /// Positions the next entry at or after the target.
    fn seek(&mut self, target: &Self::Key);
}

pub trait RewindableIter: ForwardIter {
    /// Positions the next entry at the beginning.
    fn rewind(&mut self);
}

/// A wrapper that turns a slice into a `RewindableIter`.
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

impl<'a, K, V> ForwardIter for SliceIter<'a, K, V> {
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

impl<'a, K, V> RewindableIter for SliceIter<'a, K, V> {
    fn rewind(&mut self) {
        self.iter = self.data.iter();
        self.last = None;
    }
}

impl<'a, K, V> From<&'a [(K, V)]> for SliceIter<'a, K, V> {
    fn from(entries: &'a [(K, V)]) -> Self {
        Self::new(entries)
    }
}

impl<'a, K, V, const N: usize> From<&'a [(K, V); N]> for SliceIter<'a, K, V> {
    fn from(entries: &'a [(K, V); N]) -> Self {
        Self::new(entries.as_slice())
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

impl<K, V> ForwardIter for OptionIter<K, V> {
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

impl<K, V> RewindableIter for OptionIter<K, V> {
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

/// A wrapper to sorts iterators by their current entries in reverse order.
struct ReverseIter<I>(I);

impl<I> Deref for ReverseIter<I> {
    type Target = I;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<I> DerefMut for ReverseIter<I> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<I> Eq for ReverseIter<I>
where
    I: ForwardIter,
    I::Key: Ord,
{
}

impl<I> PartialEq for ReverseIter<I>
where
    I: ForwardIter,
    I::Key: Ord,
{
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl<I> Ord for ReverseIter<I>
where
    I: ForwardIter,
    I::Key: Ord,
{
    fn cmp(&self, other: &Self) -> Ordering {
        match (self.last(), other.last()) {
            (Some(a), Some(b)) => b.0.cmp(&a.0),
            (Some(_), None) => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (None, None) => Ordering::Equal,
        }
    }
}

impl<I> PartialOrd for ReverseIter<I>
where
    I: ForwardIter,
    I::Key: Ord,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// A iterator that merges entries from multiple iterators in ascending order.
pub struct MergingIter<I>
where
    I: ForwardIter,
    I::Key: Ord,
{
    heap: BinaryHeap<ReverseIter<I>>,
    children: Vec<ReverseIter<I>>,
}

impl<I> MergingIter<I>
where
    I: ForwardIter,
    I::Key: Ord,
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
        std::mem::take(&mut self.heap).into_vec()
    }
}

impl<I> ForwardIter for MergingIter<I>
where
    I: ForwardIter,
    I::Key: Ord,
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
    I::Key: Ord,
{
    fn seek(&mut self, target: &Self::Key) {
        self.reset(|iter| iter.seek(target));
    }
}

impl<I> RewindableIter for MergingIter<I>
where
    I: RewindableIter,
    I::Key: Ord,
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
    I::Key: Ord,
{
    pub fn add(&mut self, child: I) {
        self.children.push(ReverseIter(child));
    }

    pub fn build(self) -> MergingIter<I> {
        MergingIter::new(self.children)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn slice_iter() {
        let mut iter = SliceIter::from(&[(1, 2), (3, 4)]);
        assert_eq!(iter.next(), Some(&(1, 2)));
        assert_eq!(iter.last(), Some(&(1, 2)));
        assert_eq!(iter.next(), Some(&(3, 4)));
        assert_eq!(iter.last(), Some(&(3, 4)));
        assert_eq!(iter.next(), None);
        iter.rewind();
        assert_eq!(iter.next(), Some(&(1, 2)));
        assert_eq!(iter.next(), Some(&(3, 4)));
        assert_eq!(iter.next(), None);
    }
}
