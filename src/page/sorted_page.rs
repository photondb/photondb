use super::{PageBuilder, PageKind, PageTier};

pub(super) struct SortedPageBuilder {
    base: PageBuilder,
}

impl SortedPageBuilder {
    pub(super) fn new(tier: PageTier, kind: PageKind) -> Self {
        Self {
            base: PageBuilder::new(tier, kind),
        }
    }
}
