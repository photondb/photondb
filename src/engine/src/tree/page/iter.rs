use std::{
    cmp::{Ord, Ordering},
    collections::BinaryHeap,
    ops::{Deref, DerefMut},
};

pub trait PageIter {
    type Key: Ord;
    type Value;

    // Returns the current entry.
    fn current(&self) -> Option<&(Self::Key, Self::Value)>;

    // Advances to the next entry and returns it.
    fn next(&mut self) -> Option<&(Self::Key, Self::Value)>;

    // Positions the next entry at or past the target.
    fn seek(&mut self, target: &Self::Key);

    // Positions the next entry at the first one.
    fn rewind(&mut self);
}

pub struct SingleIter<K, V> {
    next: Option<(K, V)>,
    current: Option<(K, V)>,
}

impl<K, V> From<(K, V)> for SingleIter<K, V> {
    fn from(next: (K, V)) -> Self {
        Some(next).into()
    }
}

impl<K, V> From<Option<(K, V)>> for SingleIter<K, V> {
    fn from(next: Option<(K, V)>) -> Self {
        Self {
            next,
            current: None,
        }
    }
}

impl<K: Ord, V> PageIter for SingleIter<K, V> {
    type Key = K;
    type Value = V;

    fn current(&self) -> Option<&(Self::Key, Self::Value)> {
        self.current.as_ref()
    }

    fn next(&mut self) -> Option<&(Self::Key, Self::Value)> {
        if let Some(v) = self.next.take() {
            self.current = Some(v);
        }
        self.current.as_ref()
    }

    fn seek(&mut self, target: &Self::Key) {
        todo!()
    }

    fn rewind(&mut self) {
        if let Some(v) = self.current.take() {
            self.next = Some(v);
        }
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

impl<I: PageIter> Eq for ReverseIter<I> {}

impl<I: PageIter> PartialEq for ReverseIter<I> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl<I: PageIter> Ord for ReverseIter<I> {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self.current(), other.current()) {
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

pub struct MergingIter<I: PageIter> {
    heap: BinaryHeap<ReverseIter<I>>,
    children: Vec<ReverseIter<I>>,
}

impl<I: PageIter> MergingIter<I> {
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

impl<I: PageIter> PageIter for MergingIter<I> {
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

    fn seek(&mut self, target: &Self::Key) {
        let mut children = self.take_children();
        for iter in children.iter_mut() {
            iter.seek(target);
        }
        std::mem::swap(&mut self.children, &mut children);
    }

    fn rewind(&mut self) {
        let mut children = self.take_children();
        for iter in children.iter_mut() {
            iter.rewind();
        }
        std::mem::swap(&mut self.children, &mut children);
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

impl<I: PageIter> MergingIterBuilder<I> {
    pub fn add(&mut self, child: I) {
        self.children.push(ReverseIter(child));
    }

    pub fn build(self) -> MergingIter<I> {
        MergingIter::new(self.children)
    }
}
