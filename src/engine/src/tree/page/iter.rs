use std::{
    cmp::{Ord, Ordering},
    collections::BinaryHeap,
};

pub trait PageIter {
    type Key: Ord;
    type Value;

    fn len(&self) -> usize;

    fn peek(&self) -> Option<(Self::Key, Self::Value)>;

    fn next(&mut self) -> Option<(Self::Key, Self::Value)>;

    fn seek(&mut self, target: Self::Key) -> Option<(Self::Key, Self::Value)>;
}

struct ReverseIter<I>(I);

impl<I: PageIter> Eq for ReverseIter<I> {}

impl<I: PageIter> PartialEq for ReverseIter<I> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl<I: PageIter> Ord for ReverseIter<I> {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self.0.peek(), other.0.peek()) {
            (Some(a), Some(b)) => b.0.cmp(&a.0),
            (Some(_), None) => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (None, None) => Ordering::Equal,
        }
    }
}

impl<I: PageIter> PartialOrd for ReverseIter<I> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

pub struct MergeIter<I> {
    heap: BinaryHeap<ReverseIter<I>>,
}

impl<I> MergeIter<I>
where
    I: PageIter,
{
    fn new(children: Vec<ReverseIter<I>>) -> Self {
        Self {
            heap: children.into(),
        }
    }

    pub fn next(&mut self) -> Option<(I::Key, I::Value)> {
        if let Some(mut iter) = self.heap.pop() {
            let next = iter.0.next();
            self.heap.push(iter);
            next
        } else {
            None
        }
    }
}

pub struct MergeIterBuilder<I> {
    children: Vec<ReverseIter<I>>,
}

impl<I> Default for MergeIterBuilder<I> {
    fn default() -> Self {
        Self {
            children: Vec::new(),
        }
    }
}

impl<I> MergeIterBuilder<I>
where
    I: PageIter,
{
    pub fn add(&mut self, child: I) {
        self.children.push(ReverseIter(child));
    }

    pub fn build(self) -> MergeIter<I> {
        MergeIter::new(self.children)
    }
}
