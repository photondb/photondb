use std::{
    cmp::{Ord, Ordering},
    collections::BinaryHeap,
};

pub trait PageIter {
    type Key: Ord;
    type Value;

    // Returns the current entry.
    fn peek(&self) -> Option<&(Self::Key, Self::Value)>;

    // Advances to the next entry.
    fn next(&mut self) -> Option<&(Self::Key, Self::Value)>;

    // Positions at the first entry that is no less than the target.
    fn seek(&mut self, target: &Self::Key);

    // Positions at the first entry.
    fn rewind(&mut self);
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

pub struct MergeIter<I>
where
    I: PageIter,
{
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
}

impl<I> PageIter for MergeIter<I>
where
    I: PageIter,
{
    type Key = I::Key;
    type Value = I::Value;

    fn peek(&self) -> Option<&(I::Key, I::Value)> {
        self.heap.peek().and_then(|iter| iter.0.peek())
    }

    fn next(&mut self) -> Option<&(I::Key, I::Value)> {
        if let Some(mut iter) = self.heap.pop() {
            iter.0.next();
            self.heap.push(iter);
        }
        self.peek()
    }

    fn seek(&mut self, target: &I::Key) {
        let mut children = Vec::from(std::mem::take(&mut self.heap));
        for iter in children.iter_mut() {
            iter.0.seek(target);
        }
        std::mem::swap(&mut self.heap, &mut BinaryHeap::from(children));
    }

    fn rewind(&mut self) {
        let mut children = Vec::from(std::mem::take(&mut self.heap));
        for iter in children.iter_mut() {
            iter.0.rewind();
        }
        std::mem::swap(&mut self.heap, &mut BinaryHeap::from(children));
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
