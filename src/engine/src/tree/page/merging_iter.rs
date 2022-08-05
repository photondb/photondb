use std::{
    cmp::Ordering,
    ops::{Deref, DerefMut},
};

use super::*;

struct OrderedIter<I> {
    iter: I,
    rank: usize,
}

impl<I> Deref for OrderedIter<I> {
    type Target = I;

    fn deref(&self) -> &Self::Target {
        &self.iter
    }
}

impl<I> DerefMut for OrderedIter<I> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.iter
    }
}

impl<I> Eq for OrderedIter<I>
where
    I: ForwardIter,
    I::Item: Comparable<I::Item>,
{
}

impl<I> PartialEq for OrderedIter<I>
where
    I: ForwardIter,
    I::Item: Comparable<I::Item>,
{
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl<I> Ord for OrderedIter<I>
where
    I: ForwardIter,
    I::Item: Comparable<I::Item>,
{
    fn cmp(&self, other: &Self) -> Ordering {
        let mut ord = match (self.last(), other.last()) {
            (Some(a), Some(b)) => a.compare(b),
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

impl<I> PartialOrd for OrderedIter<I>
where
    I: ForwardIter,
    I::Item: Comparable<I::Item>,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// An iterator that merges entries from multiple iterators in ascending order.
pub struct MergingIter<I> {
    heap: MinHeap<OrderedIter<I>>,
    should_rebuild: bool,
}

impl<I> MergingIter<I>
where
    I: ForwardIter,
    I::Item: Comparable<I::Item>,
{
    pub fn new(children: Vec<I>) -> Self {
        let children = children
            .into_iter()
            .enumerate()
            .map(|(rank, iter)| OrderedIter { iter, rank })
            .collect();
        Self {
            heap: MinHeap::new(children),
            should_rebuild: true,
        }
    }

    fn rebuild(&mut self) {
        self.heap.for_each(|iter| {
            iter.next();
        });
        self.heap.rebuild();
        self.should_rebuild = false;
    }

    fn for_each<F>(&mut self, f: F)
    where
        F: Fn(&mut OrderedIter<I>),
    {
        self.heap.for_each(f);
        self.should_rebuild = true;
    }
}

impl<I> ForwardIter for MergingIter<I>
where
    I: ForwardIter,
    I::Item: Comparable<I::Item>,
{
    type Item = I::Item;

    fn last(&self) -> Option<&Self::Item> {
        self.heap.peek().and_then(|iter| iter.last())
    }

    fn next(&mut self) -> Option<&Self::Item> {
        if self.should_rebuild {
            self.rebuild();
        } else {
            self.heap.for_peek(|iter| {
                iter.next();
            });
        }
        self.heap.peek().and_then(|iter| iter.last())
    }

    fn skip_all(&mut self) {
        self.for_each(|iter| iter.skip_all());
    }
}

impl<I> RewindableIter for MergingIter<I>
where
    I: RewindableIter,
    I::Item: Comparable<I::Item>,
{
    fn rewind(&mut self) {
        self.for_each(|iter| iter.rewind());
    }
}

impl<T, I> SeekableIter<T> for MergingIter<I>
where
    T: ?Sized,
    I: SeekableIter<T>,
    I::Item: Comparable<I::Item>,
{
    fn seek(&mut self, target: &T) {
        self.for_each(|iter| iter.seek(target));
    }
}

struct MinHeap<T> {
    index: Vec<usize>,
    children: Vec<T>,
}

impl<T: Ord> MinHeap<T> {
    fn new(children: Vec<T>) -> Self {
        Self {
            index: (0..children.len()).collect(),
            children,
        }
    }

    fn rebuild(&mut self) {
        let mut n = self.len() / 2;
        while n > 0 {
            n -= 1;
            unsafe { self.sift_down(n) };
        }
    }

    fn len(&self) -> usize {
        self.index.len()
    }

    fn peek(&self) -> Option<&T> {
        self.index
            .get(0)
            .map(|&i| unsafe { self.children.get_unchecked(i) })
    }

    fn for_peek<F>(&mut self, f: F)
    where
        F: Fn(&mut T),
    {
        if let Some(child) = self
            .index
            .get(0)
            .map(|&i| unsafe { self.children.get_unchecked_mut(i) })
        {
            f(child);
            unsafe {
                self.sift_down(0);
            }
        }
    }

    fn for_each<F>(&mut self, f: F)
    where
        F: Fn(&mut T),
    {
        for child in self.children.iter_mut() {
            f(child);
        }
    }

    unsafe fn child(&self, i: usize) -> &T {
        self.children.get_unchecked(i)
    }

    unsafe fn index(&self, i: usize) -> (usize, usize) {
        (i, *self.index.get_unchecked(i))
    }

    unsafe fn set_index(&mut self, i: usize, v: usize) {
        *self.index.get_unchecked_mut(i) = v;
    }

    unsafe fn sift_down(&mut self, pos: usize) {
        self.sift_down_range(pos, self.len());
    }

    unsafe fn sift_down_range(&mut self, pos: usize, end: usize) {
        let mut saved = self.index(pos);
        let mut child = self.index(pos * 2 + 1);
        while child.0 <= end.saturating_sub(2) {
            let right = self.index(child.0 + 1);
            if self.child(right.1) <= self.child(child.1) {
                child = right;
            }
            if self.child(saved.1) <= self.child(child.1) {
                // This is the place for the saved item.
                self.set_index(saved.0, saved.1);
                return;
            }
            // Moves the child up.
            self.set_index(saved.0, child.1);
            saved.0 = child.0;
            child = self.index(child.0 * 2 + 1);
        }
        // Handles the last child.
        if child.0 == end.saturating_sub(1) && self.child(saved.1) > self.child(child.1) {
            self.set_index(saved.0, child.1);
            saved.0 = child.0;
        }
        self.set_index(saved.0, saved.1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merging_iter() {
        let input_data = [[1, 3], [2, 4], [1, 8], [3, 7]];
        let sorted_data = [1, 1, 2, 3, 3, 4, 7, 8];

        let children = input_data.iter().map(SliceIter::from).collect();
        let mut iter = MergingIter::new(children);

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
        assert_eq!(iter.next(), Some(&1));
        iter.seek(&9);
        assert_eq!(iter.next(), None);
        iter.seek(&1);
        assert_eq!(iter.next(), Some(&1));
        iter.seek(&3);
        assert_eq!(iter.next(), Some(&3));
        iter.seek(&5);
        assert_eq!(iter.next(), Some(&7));

        // Tests skip_all()
        iter.rewind();
        iter.skip_all();
        assert_eq!(iter.next(), None);
    }
}
