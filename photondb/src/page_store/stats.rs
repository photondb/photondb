use crate::util::atomic::Counter;

/// Statistics of page store.
#[derive(Clone)]
pub struct StoreStats {
    /// Statistics of page cache.
    pub page_cache: CacheStats,
}

/// Statistics of pagee cache.
#[derive(Default, Clone, Debug)]
pub struct CacheStats {
    pub lookup_hit: u64,
    pub lookup_miss: u64,
    pub insert: u64,
    pub active_evit: u64,
}

impl CacheStats {
    pub fn sub(&self, o: &CacheStats) -> CacheStats {
        CacheStats {
            lookup_hit: self.lookup_hit - o.lookup_hit,
            lookup_miss: self.lookup_miss - o.lookup_hit,
            insert: self.insert - o.insert,
            active_evit: self.active_evit - o.active_evit,
        }
    }
}

#[derive(Default)]
pub(super) struct AtomicCacheStats {
    pub(super) lookup_hit: Counter,
    pub(super) lookup_miss: Counter,
    pub(super) insert: Counter,
    pub(super) active_evit: Counter,
}

impl AtomicCacheStats {
    pub(super) fn snapshot(&self) -> CacheStats {
        CacheStats {
            lookup_hit: self.lookup_hit.get(),
            lookup_miss: self.lookup_miss.get(),
            insert: self.insert.get(),
            active_evit: self.active_evit.get(),
        }
    }
}
