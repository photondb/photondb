use std::iter::Iterator;

pub trait PageIter: Iterator {
    type Item;

    fn seek(&mut self, key: &[u8]);
}

pub struct MergeIter<I>
where
    I: PageIter,
{
    children: Vec<I>,
}

pub struct MergeIterBuilder<I>
where
    I: PageIter,
{
    children: Vec<I>,
}

impl<I> MergeIterBuilder<I>
where
    I: PageIter,
{
    pub fn add(&mut self, child: I) {
        self.children.push(child);
    }

    pub fn build(self) -> MergeIter<I> {
        MergeIter {
            children: self.children,
        }
    }
}
