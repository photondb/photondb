use std::iter::Iterator;

pub trait PageIter: Iterator {}

impl<T: Iterator> PageIter for T {}

pub struct MergeIter<I>
where
    I: PageIter,
{
    children: Vec<I>,
}

impl<I> Iterator for MergeIter<I>
where
    I: PageIter,
{
    type Item = <I as Iterator>::Item;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

pub struct MergeIterBuilder<I>
where
    I: PageIter,
{
    children: Vec<I>,
}

impl<I> Default for MergeIterBuilder<I>
where
    I: PageIter,
{
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
        self.children.push(child);
    }

    pub fn build(self) -> MergeIter<I> {
        MergeIter {
            children: self.children,
        }
    }
}
