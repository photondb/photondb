use std::{
    cmp::{Ord, Ordering},
    collections::BinaryHeap,
    ops::{Deref, DerefMut},
};

pub trait ForwardIter {
    type Key;
    type Value;

    /// Returns the current entry.
    fn current(&self) -> Option<&(Self::Key, Self::Value)>;

    /// Advances to the next entry and returns it.
    fn next(&mut self) -> Option<&(Self::Key, Self::Value)>;
}

pub trait SeekableIter: ForwardIter {
    /// Positions the next entry at or past the target.
    fn seek(&mut self, target: &Self::Key);
}

pub trait RewindableIter: ForwardIter {
    /// Positions the next entry at the beginning.
    fn rewind(&mut self);
}

/// A wrapper that turns an option into a `RewindableIter`.
pub struct OptionIter<K, V> {
    next: Option<(K, V)>,
    current: Option<(K, V)>,
}

impl<K, V> From<(K, V)> for OptionIter<K, V> {
    fn from(item: (K, V)) -> Self {
        Some(item).into()
    }
}

impl<K, V> From<Option<(K, V)>> for OptionIter<K, V> {
    fn from(next: Option<(K, V)>) -> Self {
        Self {
            next,
            current: None,
        }
    }
}

impl<K, V> ForwardIter for OptionIter<K, V> {
    type Key = K;
    type Value = V;

    fn current(&self) -> Option<&(K, V)> {
        self.current.as_ref()
    }

    fn next(&mut self) -> Option<&(K, V)> {
        if let Some(next) = self.next.take() {
            self.current = Some(next);
            self.current.as_ref()
        } else {
            None
        }
    }
}

impl<K, V> RewindableIter for OptionIter<K, V> {
    fn rewind(&mut self) {
        if let Some(current) = self.current.take() {
            self.next = Some(current);
        }
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
        match (self.current(), other.current()) {
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

    fn current(&self) -> Option<&(Self::Key, Self::Value)> {
        self.heap.peek().and_then(|iter| iter.current())
    }

    fn next(&mut self) -> Option<&(Self::Key, Self::Value)> {
        if let Some(mut iter) = self.heap.pop() {
            iter.next();
            self.heap.push(iter);
        } else {
            self.init_heap();
        }
        self.current()
    }
}

impl<I> SeekableIter for MergingIter<I>
where
    I: SeekableIter,
    I::Key: Ord,
{
    fn seek(&mut self, target: &Self::Key) {
        let mut children = self.take_children();
        for iter in children.iter_mut() {
            iter.seek(target);
        }
        std::mem::swap(&mut self.children, &mut children);
    }
}

impl<I> RewindableIter for MergingIter<I>
where
    I: RewindableIter,
    I::Key: Ord,
{
    fn rewind(&mut self) {
        let mut children = self.take_children();
        for iter in children.iter_mut() {
            iter.rewind();
        }
        std::mem::swap(&mut self.children, &mut children);
    }
}

/// A builder to construct `MergingIter`.
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
