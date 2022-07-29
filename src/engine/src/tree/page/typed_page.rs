use super::{DataPageRef, Decodable, PageKind, PagePtr, SplitPageRef};

pub enum TypedPageRef<'a, K, V> {
    Data(DataPageRef<'a, K, V>),
    Split(SplitPageRef<'a>),
}

impl<'a, K, V> TypedPageRef<'a, K, V>
where
    K: Decodable + Ord,
    V: Decodable,
{
    pub unsafe fn cast(base: PagePtr) -> Self {
        match base.kind() {
            PageKind::Data => Self::Data(DataPageRef::new(base)),
            PageKind::Split => Self::Split(SplitPageRef::new(base)),
        }
    }
}
