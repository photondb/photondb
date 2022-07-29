use super::{DataPageRef, Decodable, PageKind, PagePtr, SplitPageRef};

/// A page reference with a specific type.
pub enum TypedPageRef<'a, K, V> {
    Data(DataPageRef<'a, K, V>),
    Split(SplitPageRef<'a>),
}

impl<'a, K, V> TypedPageRef<'a, K, V>
where
    K: Decodable + Ord,
    V: Decodable,
{
    /// Creates a typed reference from a `PagePtr`.
    ///
    /// # Safety
    ///
    /// This function is unsafe because it does not check that the page is of the correct type.
    pub unsafe fn cast(base: PagePtr) -> Self {
        match base.kind() {
            PageKind::Data => Self::Data(DataPageRef::new(base)),
            PageKind::Split => Self::Split(SplitPageRef::new(base)),
        }
    }
}
