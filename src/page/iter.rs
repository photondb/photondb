use std::{
    cmp::{Ordering, Reverse},
    collections::BinaryHeap,
    mem,
};

pub(crate) trait ForwardIter {
    type Item;

    /// Positions the iterator at the first item.
    ///
    /// [`Self::next`] will return the first item.
    fn init(&mut self);

    /// Advances the iterator to the next item.
    fn next(&mut self) -> Option<Self::Item>;
}

pub(crate) trait SeekableIter: ForwardIter {
    /// Positions the iterator at the first item that is greater than or equal to `target`.
    fn seek(&mut self);
}

struct OrderedIter<I>
where
    I: ForwardIter,
{
    iter: I,
    next: Option<I::Item>,
}

impl<I> OrderedIter<I>
where
    I: ForwardIter,
{
    fn new(iter: I) -> Self {
        Self { iter, next: None }
    }
}

impl<I> Eq for OrderedIter<I>
where
    I: ForwardIter,
    I::Item: Ord,
{
}

impl<I> PartialEq for OrderedIter<I>
where
    I: ForwardIter,
    I::Item: Ord,
{
    fn eq(&self, other: &Self) -> bool {
        self.next == other.next
    }
}

impl<I> Ord for OrderedIter<I>
where
    I: ForwardIter,
    I::Item: Ord,
{
    fn cmp(&self, other: &Self) -> Ordering {
        self.next.cmp(&other.next)
    }
}

impl<I> PartialOrd for OrderedIter<I>
where
    I: ForwardIter,
    I::Item: Ord,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.next.partial_cmp(&other.next)
    }
}

impl<I> ForwardIter for OrderedIter<I>
where
    I: ForwardIter,
{
    type Item = I::Item;

    fn init(&mut self) {
        self.iter.init();
        self.next = self.iter.next();
    }

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

pub(crate) struct MergingIter<I>
where
    I: ForwardIter,
    I::Item: Ord,
{
    heap: BinaryHeap<Reverse<OrderedIter<I>>>,
}

impl<I> MergingIter<I>
where
    I: ForwardIter,
    I::Item: Ord,
{
    fn new(iters: Vec<Reverse<OrderedIter<I>>>) -> Self {
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

impl<I> ForwardIter for MergingIter<I>
where
    I: ForwardIter,
    I::Item: Ord,
{
    type Item = I::Item;

    fn init(&mut self) {
        self.for_each(|iter| iter.0.init());
    }

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

pub(crate) struct MergingIterBuilder<I>
where
    I: ForwardIter,
    I::Item: Ord,
{
    iters: Vec<Reverse<OrderedIter<I>>>,
}

impl<I> MergingIterBuilder<I>
where
    I: ForwardIter,
    I::Item: Ord,
{
    pub(crate) fn new() -> Self {
        Self { iters: Vec::new() }
    }

    pub(crate) fn add(mut self, iter: I) {
        self.iters.push(Reverse(OrderedIter::new(iter)));
    }

    pub(crate) fn build(self) -> MergingIter<I> {
        MergingIter::new(self.iters)
    }
}
