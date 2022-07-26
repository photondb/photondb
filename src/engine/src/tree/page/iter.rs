use std::{
    cmp::{Ord, Ordering},
    collections::BinaryHeap,
    ops::{Deref, DerefMut},
};

pub trait ForwardIter {
    type Item;

    fn next(&mut self) -> Option<&Self::Item>;
}

pub trait SequentialIter: ForwardIter {
    // Positions the next entry at the beginning.
    fn rewind(&mut self);

    // Returns the current entry.
    fn current(&self) -> Option<&Self::Item>;
}

pub trait RandomAccessIter: SequentialIter {
    type Target;

    // Positions the next entry at or past the target.
    fn seek(&mut self, target: &Self::Target);
}

pub struct OptionIter<'a, I> {
    next: Option<&'a I>,
    current: Option<&'a I>,
}

impl<'a, I> From<&'a I> for OptionIter<'a, I> {
    fn from(item: &'a I) -> Self {
        Some(item).into()
    }
}

impl<'a, I> From<Option<&'a I>> for OptionIter<'a, I> {
    fn from(next: Option<&'a I>) -> Self {
        Self {
            next,
            current: None,
        }
    }
}

impl<'a, I> ForwardIter for OptionIter<'a, I> {
    type Item = I;

    fn next(&mut self) -> Option<&Self::Item> {
        if let Some(next) = self.next.take() {
            self.current = Some(next);
            self.current
        } else {
            None
        }
    }
}

impl<'a, I: 'a> SequentialIter for OptionIter<'a, I> {
    fn rewind(&mut self) {
        if let Some(current) = self.current.take() {
            self.next = Some(current);
        }
    }

    fn current(&self) -> Option<&Self::Item> {
        self.current
    }
}

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
    I: SequentialIter,
    I::Item: Ord,
{
    pub fn add(&mut self, child: I) {
        self.children.push(ReverseIter(child));
    }

    pub fn build(self) -> MergingIter<I> {
        MergingIter::new(self.children)
    }
}

pub struct MergingIter<I>
where
    I: SequentialIter,
    I::Item: Ord,
{
    heap: BinaryHeap<ReverseIter<I>>,
    children: Vec<ReverseIter<I>>,
}

impl<I> MergingIter<I>
where
    I: SequentialIter,
    I::Item: Ord,
{
    fn new(children: Vec<ReverseIter<I>>) -> Self {
        Self {
            heap: BinaryHeap::default(),
            children,
        }
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
    I: SequentialIter,
    I::Item: Ord,
{
    type Item = I::Item;

    fn next(&mut self) -> Option<&Self::Item> {
        if let Some(mut iter) = self.heap.pop() {
            iter.next();
            self.heap.push(iter);
        } else {
            self.init_heap();
        }
        self.current()
    }
}

impl<I> SequentialIter for MergingIter<I>
where
    I: SequentialIter,
    I::Item: Ord,
{
    fn rewind(&mut self) {
        let mut children = self.take_children();
        for iter in children.iter_mut() {
            iter.rewind();
        }
        std::mem::swap(&mut self.children, &mut children);
    }

    fn current(&self) -> Option<&Self::Item> {
        self.heap.peek().and_then(|iter| iter.current())
    }
}

impl<I> RandomAccessIter for MergingIter<I>
where
    I: RandomAccessIter,
    I::Item: Ord,
{
    type Target = I::Target;

    fn seek(&mut self, target: &Self::Target) {
        let mut children = self.take_children();
        for iter in children.iter_mut() {
            iter.seek(target);
        }
        std::mem::swap(&mut self.children, &mut children);
    }
}

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
    I: SequentialIter,
    I::Item: Ord,
{
}

impl<I> PartialEq for ReverseIter<I>
where
    I: SequentialIter,
    I::Item: Ord,
{
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl<I> Ord for ReverseIter<I>
where
    I: SequentialIter,
    I::Item: Ord,
{
    fn cmp(&self, other: &Self) -> Ordering {
        match (self.current(), other.current()) {
            (Some(a), Some(b)) => b.cmp(a),
            (Some(_), None) => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (None, None) => Ordering::Equal,
        }
    }
}

impl<I> PartialOrd for ReverseIter<I>
where
    I: SequentialIter,
    I::Item: Ord,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
