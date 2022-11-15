//! A mod contains jobs used by `page_store`.

pub(crate) mod cleanup;
pub(crate) mod flush;
pub(crate) mod reclaim;
pub(crate) use reclaim::RewritePage;
