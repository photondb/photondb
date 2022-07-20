pub trait PageIter {
    type Item;

    fn len(&self) -> usize;

    fn get(&self, n: usize) -> Option<Self::Item>;

    fn next(&mut self) -> Option<Self::Item>;
}

pub struct MergeIter<I>
where
    I: PageIter,
{
    children: Vec<I>,
}

impl<I> PageIter for MergeIter<I>
where
    I: PageIter,
{
    type Item = I::Item;

    fn len(&self) -> usize {
        todo!()
    }

    fn get(&self, n: usize) -> Option<Self::Item> {
        todo!()
    }

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
