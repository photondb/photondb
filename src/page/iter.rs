use std::{
    cmp::{Ordering, Reverse},
    collections::BinaryHeap,
    iter::Iterator,
    mem,
};

/// An extension of [`Iterator`] that can seek to a target.
pub(crate) trait SeekableIterator<T>: Iterator {
    /// Positions the iterator at the first item that is at or after `target`.
    fn seek(&mut self, target: &T);
}

/// An extension of [`Iterator`] that can rewind back to the beginning.
pub(crate) trait RewindableIterator: Iterator {
    /// Positions the iterator at the first item.
    fn rewind(&mut self);
}

#[derive(Clone, Debug, Default)]
pub(crate) struct ItemIter<T> {
    next: Option<T>,
    item: Option<T>,
}

impl<T: Clone> ItemIter<T> {
    pub(crate) fn new(item: T) -> Self {
        Self {
            next: Some(item.clone()),
            item: Some(item),
        }
    }
}

impl<T: Clone> Iterator for ItemIter<T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        self.next.take()
    }
}

impl<T: Clone> RewindableIterator for ItemIter<T> {
    fn rewind(&mut self) {
        self.next = self.item.clone();
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct SliceIter<'a, T> {
    data: &'a [T],
    next: usize,
}

impl<'a, T> SliceIter<'a, T> {
    pub(crate) fn new(data: &'a [T]) -> Self {
        Self { data, next: 0 }
    }
}

impl<'a, T: Clone> Iterator for SliceIter<'a, T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(item) = self.data.get(self.next) {
            self.next += 1;
            Some(item.clone())
        } else {
            None
        }
    }
}

impl<'a, T: Clone + Ord> SeekableIterator<T> for SliceIter<'a, T> {
    fn seek(&mut self, target: &T) {
        self.next = match self.data.binary_search(target) {
            Ok(i) => i,
            Err(i) => i,
        }
    }
}

impl<'a, T: Clone> RewindableIterator for SliceIter<'a, T> {
    fn rewind(&mut self) {
        self.next = 0;
    }
}

/// A wrapper to order an [`Iterator`] by its next item and rank.
#[derive(Debug)]
pub(crate) struct OrderedIter<I>
where
    I: Iterator,
{
    iter: I,
    rank: usize,
    next: Option<I::Item>,
}

impl<I> OrderedIter<I>
where
    I: Iterator,
{
    fn new(iter: I, rank: usize) -> Self {
        Self {
            iter,
            rank,
            next: None,
        }
    }

    fn init(&mut self) {
        self.next = self.iter.next();
    }
}

impl<I, K, V> Eq for OrderedIter<I>
where
    I: Iterator<Item = (K, V)>,
    K: Ord,
{
}

impl<I, K, V> PartialEq for OrderedIter<I>
where
    I: Iterator<Item = (K, V)>,
    K: Ord,
{
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl<I, K, V> Ord for OrderedIter<I>
where
    I: Iterator<Item = (K, V)>,
    K: Ord,
{
    fn cmp(&self, other: &Self) -> Ordering {
        let mut ord = match (&self.next, &other.next) {
            (Some(a), Some(b)) => a.0.cmp(&b.0),
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

impl<I, K, V> PartialOrd for OrderedIter<I>
where
    I: Iterator<Item = (K, V)>,
    K: Ord,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<I> Iterator for OrderedIter<I>
where
    I: Iterator,
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.next.take();
        self.next = self.iter.next();
        next
    }
}

impl<I, T> SeekableIterator<T> for OrderedIter<I>
where
    I: SeekableIterator<T>,
{
    fn seek(&mut self, target: &T) {
        self.iter.seek(target);
        self.next = self.iter.next();
    }
}

impl<I> RewindableIterator for OrderedIter<I>
where
    I: RewindableIterator,
{
    fn rewind(&mut self) {
        self.iter.rewind();
        self.next = self.iter.next();
    }
}

/// An iterator that merges multiple ordered iterators into one.
#[derive(Default)]
pub(crate) struct MergingIter<I>
where
    I: Iterator,
    OrderedIter<I>: Iterator + Ord,
{
    heap: BinaryHeap<Reverse<OrderedIter<I>>>,
}

impl<I> MergingIter<I>
where
    I: Iterator,
    OrderedIter<I>: Iterator + Ord,
{
    fn init(mut vec: Vec<Reverse<OrderedIter<I>>>) -> Self {
        for iter in vec.iter_mut() {
            iter.0.init();
        }
        Self { heap: vec.into() }
    }

    fn for_each<F>(&mut self, f: F)
    where
        F: Fn(&mut Reverse<OrderedIter<I>>),
    {
        let mut vec = mem::take(&mut self.heap).into_vec();
        for iter in vec.iter_mut() {
            f(iter)
        }
        let mut heap = BinaryHeap::from(vec);
        mem::swap(&mut self.heap, &mut heap);
    }
}

impl<I> Iterator for MergingIter<I>
where
    I: Iterator,
    OrderedIter<I>: Iterator<Item = I::Item> + Ord,
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(mut iter) = self.heap.peek_mut() {
            iter.0.next()
        } else {
            None
        }
    }
}

impl<I, T> SeekableIterator<T> for MergingIter<I>
where
    I: Iterator,
    OrderedIter<I>: SeekableIterator<T, Item = I::Item> + Ord,
{
    fn seek(&mut self, target: &T) {
        self.for_each(|iter| iter.0.seek(target))
    }
}

impl<I> RewindableIterator for MergingIter<I>
where
    I: Iterator,
    OrderedIter<I>: RewindableIterator<Item = I::Item> + Ord,
{
    fn rewind(&mut self) {
        self.for_each(|iter| iter.0.rewind());
    }
}

/// Builds a [`MergingIter`] from multiple iterators.
#[derive(Default)]
pub(crate) struct MergingIterBuilder<I>
where
    I: Iterator,
{
    iters: Vec<Reverse<OrderedIter<I>>>,
}

impl<I, K, V> MergingIterBuilder<I>
where
    I: Iterator<Item = (K, V)>,
    K: Ord,
{
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            iters: Vec::with_capacity(capacity),
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.iters.len()
    }

    pub(crate) fn add(&mut self, iter: I) {
        let rank = self.iters.len();
        self.iters.push(Reverse(OrderedIter::new(iter, rank)));
    }

    /// Creates a [`MergingIter`] from the specified iterators.
    pub(crate) fn build(self) -> MergingIter<I> {
        MergingIter::init(self.iters)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn item_iter() {
        let mut iter = ItemIter::new(1);
        for _ in 0..2 {
            assert_eq!(iter.next(), Some(1));
            assert_eq!(iter.next(), None);
            iter.rewind();
        }
    }

    #[test]
    fn slice_iter() {
        let mut iter = SliceIter::new(&[1, 2]);
        for _ in 0..2 {
            assert_eq!(iter.next(), Some(1));
            assert_eq!(iter.next(), Some(2));
            assert_eq!(iter.next(), None);
            iter.rewind();
        }
    }

    #[test]
    fn merging_iter() {
        let input = [
            vec![(1, "a"), (3, "a"), (7, "a")],
            vec![(2, "b"), (4, "b")],
            vec![(1, "c"), (2, "c"), (8, "c")],
            vec![(3, "d"), (7, "d")],
        ];
        let output = [
            (1, "a"),
            (1, "c"),
            (2, "b"),
            (2, "c"),
            (3, "a"),
            (3, "d"),
            (4, "b"),
            (7, "a"),
            (7, "d"),
            (8, "c"),
        ];

        let mut builder = MergingIterBuilder::default();
        for data in input.iter() {
            builder.add(SliceIter::new(data));
        }
        let mut iter = builder.build();

        for _ in 0..2 {
            for item in output.iter().copied() {
                assert_eq!(iter.next(), Some(item));
            }
            assert_eq!(iter.next(), None);
            iter.rewind();
        }

        iter.seek(&(0, ""));
        assert_eq!(iter.next(), Some((1, "a")));
        assert_eq!(iter.next(), Some((1, "c")));
        iter.seek(&(9, ""));
        assert_eq!(iter.next(), None);
        iter.seek(&(1, ""));
        assert_eq!(iter.next(), Some((1, "a")));
        iter.seek(&(3, ""));
        assert_eq!(iter.next(), Some((3, "a")));
        assert_eq!(iter.next(), Some((3, "d")));
        iter.seek(&(5, ""));
        assert_eq!(iter.next(), Some((7, "a")));
        assert_eq!(iter.next(), Some((7, "d")));
        assert_eq!(iter.next(), Some((8, "c")));
    }
}
