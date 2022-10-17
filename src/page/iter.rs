use std::{
    cmp::{Ordering, Reverse},
    collections::BinaryHeap,
    iter::Iterator,
    mem,
};

pub(crate) trait SeekableIter: Iterator {
    /// Positions the iterator at the first item that is greater than or equal to `target`.
    fn seek(&mut self);
}

pub(crate) trait RewindableIter: Iterator {
    /// Positions the iterator at the first item.
    fn rewind(&mut self);
}

pub(crate) struct ItemIter<'a, T> {
    next: Option<&'a T>,
    item: Option<&'a T>,
}

impl<'a, T> ItemIter<'a, T> {
    pub(crate) fn new(item: &'a T) -> Self {
        Self {
            next: Some(item),
            item: Some(item),
        }
    }
}

impl<'a, T> Iterator for ItemIter<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        self.next.take()
    }
}

impl<'a, T> RewindableIter for ItemIter<'a, T> {
    fn rewind(&mut self) {
        self.next = self.item;
    }
}

pub(crate) struct SliceIter<'a, T> {
    data: &'a [T],
    next: usize,
}

impl<'a, T> SliceIter<'a, T> {
    pub(crate) fn new(data: &'a [T]) -> Self {
        Self { data, next: 0 }
    }
}

impl<'a, T> Iterator for SliceIter<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(item) = self.data.get(self.next) {
            self.next += 1;
            Some(item)
        } else {
            None
        }
    }
}

impl<'a, T> RewindableIter for SliceIter<'a, T> {
    fn rewind(&mut self) {
        self.next = 0;
    }
}

struct OrderedIter<I>
where
    I: Iterator,
{
    iter: I,
    next: Option<I::Item>,
}

impl<I> OrderedIter<I>
where
    I: Iterator,
{
    fn new(iter: I) -> Self {
        Self { iter, next: None }
    }
}

impl<I> Eq for OrderedIter<I>
where
    I: Iterator,
    I::Item: Ord,
{
}

impl<I> PartialEq for OrderedIter<I>
where
    I: Iterator,
    I::Item: Ord,
{
    fn eq(&self, other: &Self) -> bool {
        self.next == other.next
    }
}

impl<I> Ord for OrderedIter<I>
where
    I: Iterator,
    I::Item: Ord,
{
    fn cmp(&self, other: &Self) -> Ordering {
        self.next.cmp(&other.next)
    }
}

impl<I> PartialOrd for OrderedIter<I>
where
    I: Iterator,
    I::Item: Ord,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.next.partial_cmp(&other.next)
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

impl<I> SeekableIter for OrderedIter<I>
where
    I: SeekableIter,
{
    fn seek(&mut self) {
        self.iter.seek();
        self.next = self.iter.next();
    }
}

impl<I> RewindableIter for OrderedIter<I>
where
    I: RewindableIter,
{
    fn rewind(&mut self) {
        self.iter.rewind();
        self.next = self.iter.next();
    }
}

pub(crate) struct MergingIter<I>
where
    I: Iterator,
    I::Item: Ord,
{
    heap: BinaryHeap<Reverse<OrderedIter<I>>>,
}

impl<I> MergingIter<I>
where
    I: Iterator,
    I::Item: Ord,
{
    fn init(mut iters: Vec<Reverse<OrderedIter<I>>>) -> Self {
        for iter in iters.iter_mut() {
            iter.0.next();
        }
        Self { heap: iters.into() }
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
    I::Item: Ord,
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

impl<I> SeekableIter for MergingIter<I>
where
    I: SeekableIter,
    I::Item: Ord,
{
    fn seek(&mut self) {
        self.for_each(|iter| iter.0.seek());
    }
}

impl<I> RewindableIter for MergingIter<I>
where
    I: RewindableIter,
    I::Item: Ord,
{
    fn rewind(&mut self) {
        self.for_each(|iter| iter.0.rewind());
    }
}

pub(crate) struct MergingIterBuilder<I>
where
    I: Iterator,
    I::Item: Ord,
{
    iters: Vec<Reverse<OrderedIter<I>>>,
}

impl<I> MergingIterBuilder<I>
where
    I: Iterator,
    I::Item: Ord,
{
    pub(crate) fn new() -> Self {
        Self { iters: Vec::new() }
    }

    pub(crate) fn add(mut self, iter: I) {
        self.iters.push(Reverse(OrderedIter::new(iter)));
    }

    pub(crate) fn build(self) -> MergingIter<I> {
        MergingIter::init(self.iters)
    }
}
