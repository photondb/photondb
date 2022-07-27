use super::{Decodable, PageKind, PageRef, SortedPageRef};

pub enum TypedPageRef<'a, K, V> {
    Data(SortedPageRef<'a, K, V>),
    Split,
}

impl<'a, K, V> TypedPageRef<'a, K, V>
where
    K: Decodable + Ord,
    V: Decodable,
{
    pub unsafe fn cast(base: PageRef<'a>) -> Self {
        match base.kind() {
            PageKind::Data => Self::Data(SortedPageRef::new(base)),
            PageKind::Split => Self::Split,
        }
    }
}
