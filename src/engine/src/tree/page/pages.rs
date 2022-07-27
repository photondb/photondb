use super::{Decodable, PageKind, PageRef, SortedPageRef};

pub enum TypedPageRef<'a, K, V> {
    Data(SortedPageRef<'a, K, V>),
    Split,
}

impl<'a, K, V> From<PageRef<'a>> for TypedPageRef<'a, K, V>
where
    K: Decodable + Ord,
    V: Decodable,
{
    fn from(base: PageRef<'a>) -> Self {
        match base.kind() {
            PageKind::Data => Self::Data(unsafe { SortedPageRef::new(base) }),
            PageKind::Split => Self::Split,
        }
    }
}
