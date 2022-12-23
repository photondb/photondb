use std::{
    fmt,
    ops::{Deref, DerefMut},
    ptr,
    sync::atomic::{AtomicU8, Ordering},
};

use ::std::sync::{
    atomic::{AtomicU32, AtomicU64},
    Arc,
};

use super::{stats::CacheStats, CacheOption};
use crate::{
    page_store::{Error, Result},
    util::atomic::Counter,
};

pub(crate) mod clock;
pub(crate) mod lru;

pub(crate) trait Cache<T: Clone>: Sized {
    fn insert(
        self: &Arc<Self>,
        key: u64,
        value: Option<T>,
        charge: usize,
        option: CacheOption,
    ) -> Result<Option<CacheEntry<T, Self>>>;

    fn lookup(self: &Arc<Self>, key: u64) -> Option<CacheEntry<T, Self>>;

    fn release(&self, h: &Handle<T>, cache_token: CacheToken) -> bool;

    fn erase(self: &Arc<Self>, key: u64);

    fn stats(self: &Arc<Self>) -> CacheStats;
}

pub(crate) struct CacheEntry<T, C>
where
    T: Clone,
    C: Cache<T>,
{
    handle: Handle<T>,
    cache: Arc<C>,
    token: CacheToken,
}

pub(crate) const CACHE_AS_HOT: u8 = 0;
pub(crate) const CACHE_AS_COLD: u8 = 1;
pub(crate) const CACHE_DISCARD: u8 = 2;

#[derive(Clone)]
pub(crate) struct CacheToken {
    returning_behavior: Arc<AtomicU8>,
}

impl Default for CacheToken {
    fn default() -> Self {
        Self {
            returning_behavior: Arc::new(AtomicU8::new(CACHE_AS_HOT)),
        }
    }
}

impl CacheToken {
    pub(crate) fn new(behavior: u8) -> Self {
        Self {
            returning_behavior: Arc::new(AtomicU8::new(behavior)),
        }
    }

    pub(crate) fn returning_behavior_match(&self, behavior: u8) -> bool {
        self.returning_behavior.load(Ordering::Relaxed) == behavior
    }

    #[allow(dead_code)]
    pub(crate) fn invalidate_from_cache(&self) {
        self.returning_behavior
            .store(CACHE_DISCARD, Ordering::Relaxed);
    }

    #[allow(dead_code)]
    pub(crate) fn return_cache_as_cold(&self) {
        self.returning_behavior
            .store(CACHE_AS_COLD, Ordering::Relaxed);
    }
}

unsafe impl<T, C> Send for CacheEntry<T, C>
where
    T: Clone,
    C: Cache<T>,
{
}

unsafe impl<T, C> Sync for CacheEntry<T, C>
where
    T: Clone,
    C: Cache<T>,
{
}

impl<T, C> Drop for CacheEntry<T, C>
where
    T: Clone,
    C: Cache<T>,
{
    fn drop(&mut self) {
        self.cache.release(&self.handle, self.token.clone());
    }
}

impl<T, C> CacheEntry<T, C>
where
    T: Clone,
    C: Cache<T>,
{
    #[allow(dead_code)]
    pub(crate) fn key(&self) -> u64 {
        match self.handle {
            Handle::Clock(h) => unsafe { (*h).key },
            Handle::Lru(h) => unsafe { (*h).key },
        }
    }

    pub(crate) fn value(&self) -> &T {
        match self.handle {
            Handle::Clock(h) => unsafe { (*h).value.as_ref().unwrap() },
            Handle::Lru(h) => unsafe { (*h).value.as_ref().unwrap() },
        }
    }

    pub(crate) fn cache_token(&self) -> CacheToken {
        self.token.clone()
    }
}

#[repr(align(64))]
pub(crate) enum Handle<T: Clone> {
    Clock(*mut ClockHandle<T>),
    Lru(*mut LRUHandle<T>),
}

pub(crate) struct ClockHandle<T: Clone> {
    key: u64,
    hash: u32, // TODO: cmp with bijective hash algorithm to save hash into key.
    value: Option<T>,
    charge: usize,

    meta: AtomicU64,
    displacements: AtomicU32,
    detached: bool,
}

#[repr(align(64))]
pub(crate) struct LRUHandle<T: Clone> {
    key: u64,
    hash: u32,
    value: Option<T>,
    charge: usize,

    next_hash: *mut LRUHandle<T>,
    next_linked: *mut LRUHandle<T>,
    prev_linked: *mut LRUHandle<T>,
    refs: u32,
    flags: u8,
    detached: bool,
}

impl<T: Clone> Default for ClockHandle<T> {
    fn default() -> Self {
        Self {
            key: Default::default(),
            hash: Default::default(),
            charge: Default::default(),
            meta: Default::default(),
            displacements: Default::default(),
            detached: Default::default(),

            value: None,
        }
    }
}

impl<T: Clone> Default for LRUHandle<T> {
    fn default() -> Self {
        Self {
            key: Default::default(),
            hash: Default::default(),
            charge: Default::default(),
            detached: Default::default(),

            next_hash: ptr::null_mut(),
            next_linked: ptr::null_mut(),
            prev_linked: ptr::null_mut(),
            refs: 0,
            flags: 0,

            value: None,
        }
    }
}

impl<T: Clone> ClockHandle<T> {
    fn insert(&mut self, key: u64, val: Option<T>, hash: u32, charge: usize) {
        self.key = key;
        self.value = val;
        self.hash = hash;
        self.charge = charge;
    }
}

impl<T: Clone> LRUHandle<T> {
    fn is_in_cache(&self) -> bool {
        self.flags & FLAG_IN_CACHE > 0
    }

    fn set_in_cache(&mut self, in_cache: bool) {
        if in_cache {
            self.flags |= FLAG_IN_CACHE
        } else {
            self.flags &= !FLAG_IN_CACHE
        }
    }

    fn add_ref(&mut self) {
        self.refs += 1;
    }

    fn unref(&mut self) -> bool {
        assert!(self.refs > 0);
        self.refs -= 1;
        self.refs == 0
    }

    fn has_refs(&self) -> bool {
        self.refs > 0
    }
}

const FLAG_IN_CACHE: u8 = 1;

#[derive(Default)]
struct AtomicCacheStats {
    lookup_hit: CachePadded<Counter>,
    lookup_miss: CachePadded<Counter>,
    insert: CachePadded<Counter>,
    active_evict: CachePadded<Counter>,
    passive_evict: CachePadded<Counter>,
}

impl AtomicCacheStats {
    pub(super) fn snapshot(&self) -> CacheStats {
        CacheStats {
            lookup_hit: self.lookup_hit.get(),
            lookup_miss: self.lookup_miss.get(),
            insert: self.insert.get(),
            active_evict: self.active_evict.get(),
            passive_evict: self.passive_evict.get(),
            recommendation: vec![],
        }
    }
}

#[cfg_attr(any(target_arch = "x86_64", target_arch = "aarch64"), repr(align(128)))]
#[cfg_attr(
    not(any(target_arch = "x86_64", target_arch = "aarch64")),
    repr(align(64))
)]
#[derive(Default, PartialEq, Eq)]
struct CachePadded<T> {
    value: T,
}

#[allow(unsafe_code)]
unsafe impl<T: Send> Send for CachePadded<T> {}

#[allow(unsafe_code)]
unsafe impl<T: Sync> Sync for CachePadded<T> {}

impl<T> CachePadded<T> {
    const fn new(t: T) -> CachePadded<T> {
        CachePadded::<T> { value: t }
    }
}

impl<T> Deref for CachePadded<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.value
    }
}

impl<T> DerefMut for CachePadded<T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.value
    }
}

impl<T: fmt::Debug> fmt::Debug for CachePadded<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CachePadded")
            .field("value", &self.value)
            .finish()
    }
}

impl<T> From<T> for CachePadded<T> {
    fn from(t: T) -> Self {
        CachePadded::new(t)
    }
}

#[cfg(test)]
mod tests {
    use ::std::thread;

    use super::*;

    #[test]
    fn test_lru_base_op() {
        use super::lru::*;

        let c = Arc::new(LRUCache::new(2, -1));

        let h = c
            .insert(1, Some(vec![1]), 1, CacheOption::default())
            .unwrap()
            .unwrap();
        drop(h);
        let h = c.lookup(1).unwrap();
        assert_eq!(h.key(), 1);
        drop(h);

        let h = c
            .insert(2, Some(vec![2]), 1, CacheOption::default())
            .unwrap()
            .unwrap();
        drop(h);

        let h = c
            .insert(3, Some(vec![3]), 1, CacheOption::default())
            .unwrap()
            .unwrap();
        drop(h);
        let h = c.lookup(3).unwrap();
        assert_eq!(h.key(), 3);
        drop(h);

        let h = c.lookup(1);
        assert!(h.is_none());

        c.erase(3);
        let h = c.lookup(3);
        assert!(h.is_none());
    }

    #[test]
    fn test_base_cache_op() {
        use super::clock::*;
        let c = Arc::new(ClockCache::new(2, 1, -1, false, false));

        let t1 = {
            let c = c.clone();
            thread::spawn(move || {
                for i in 1..=3 {
                    let v = c
                        .insert(i, Some(vec![i]), 1, CacheOption::default())
                        .unwrap()
                        .unwrap();
                    assert_eq!(v.key(), i);
                    drop(v);
                }
            })
        };
        t1.join().unwrap();

        let v = c.lookup(3).unwrap();
        assert_eq!(v.key(), 3);
        drop(v);
        assert!(c.lookup(1).is_none());
        assert!(c.lookup(2).is_none());

        let v = c
            .insert(4, Some(vec![4]), 1, CacheOption::default())
            .unwrap()
            .unwrap();
        assert_eq!(v.key(), 4);
        drop(v);

        let t2 = {
            let c = c.clone();
            thread::spawn(move || {
                let v = c.lookup(3).unwrap();
                assert_eq!(v.key(), 3);
                drop(v)
            })
        };
        let t3 = {
            let c = c.clone();
            thread::spawn(move || {
                let v = c.lookup(4).unwrap();
                assert_eq!(v.key(), 4);
                drop(v);
            })
        };
        t3.join().unwrap();
        t2.join().unwrap();

        c.erase(4);
        assert!(c.lookup(4).is_none());
    }
}
