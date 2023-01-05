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
use bitflags::bitflags;

use super::{page_txn::CachePriority, stats::CacheStats, CacheOption};
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

    fn erase_file_pages(self: &std::sync::Arc<Self>, file_id: u32);

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

pub(crate) const CACHE_AS_RECENT: u8 = 0;
pub(crate) const CACHE_AS_OLD: u8 = 1;
pub(crate) const CACHE_DISCARD: u8 = 2;

#[derive(Clone)]
pub(crate) struct CacheToken {
    returning_behavior: Arc<AtomicU8>,
}

impl Default for CacheToken {
    fn default() -> Self {
        Self {
            returning_behavior: Arc::new(AtomicU8::new(CACHE_AS_RECENT)),
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
    pub(crate) fn return_cache_as_old(&self) {
        self.returning_behavior
            .store(CACHE_AS_OLD, Ordering::Relaxed);
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
            Handle::Lru(h) => unsafe { (*h).key.into() },
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

#[derive(Default, PartialEq, Debug, Clone, Copy)]
pub(crate) struct Key(u64);

impl From<u64> for Key {
    fn from(n: u64) -> Self {
        Self(n)
    }
}

impl From<Key> for u64 {
    fn from(k: Key) -> Self {
        k.0
    }
}

impl Key {
    pub(crate) fn file_id(&self) -> u32 {
        (self.0 >> 32) as u32
    }
}

#[repr(align(64))]
pub(crate) struct LRUHandle<T: Clone> {
    key: Key,
    hash: u32,
    value: Option<T>,
    charge: usize,

    page_link: HandleLink<T>,
    file_link: HandleLink<T>,

    refs: u32,
    flags: CacheFlags,
}

bitflags! {
/// Cache Option.
pub(crate) struct CacheFlags: u8 {
    const DEFAULT = 0b00000000;

    const IN_CACHE = 0b00000001;

    const DETACHED = 0b00000010;

    const LOW_PRI = 0b00000100;

    const BOTTOM_PRI = 0b00001000;

    const IN_LOW_PRI = 0b00010000;

    const IN_HIGH_PRI = 0b00100000;
}
}

impl Default for CacheFlags {
    fn default() -> Self {
        CacheFlags::DEFAULT
    }
}

struct HandleLink<T: Clone> {
    next: *mut LRUHandle<T>,
    prev: *mut LRUHandle<T>,
}

impl<T: Clone> Default for HandleLink<T> {
    fn default() -> Self {
        Self {
            next: ptr::null_mut(),
            prev: ptr::null_mut(),
        }
    }
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
            page_link: Default::default(),
            file_link: Default::default(),
            refs: 0,
            flags: Default::default(),

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
    #[inline]
    fn is_in_cache(&self) -> bool {
        self.flags.contains(CacheFlags::IN_CACHE)
    }

    #[inline]
    fn set_in_cache(&mut self, in_cache: bool) {
        self.flags.set(CacheFlags::IN_CACHE, in_cache)
    }

    #[inline]
    fn is_detached(&self) -> bool {
        self.flags.contains(CacheFlags::DETACHED)
    }

    #[inline]
    fn set_detached(&mut self, detached: bool) {
        self.flags.set(CacheFlags::DETACHED, detached)
    }

    #[inline]
    fn priority(&self) -> CachePriority {
        if self.flags.contains(CacheFlags::BOTTOM_PRI) {
            CachePriority::Bottom
        } else if self.flags.contains(CacheFlags::LOW_PRI) {
            CachePriority::Low
        } else {
            CachePriority::High
        }
    }

    #[inline]
    fn set_priority(&mut self, pri: CachePriority) {
        match pri {
            CachePriority::High => {
                self.flags.set(CacheFlags::LOW_PRI, false);
                self.flags.set(CacheFlags::BOTTOM_PRI, false);
            }
            CachePriority::Low => {
                self.flags.set(CacheFlags::LOW_PRI, true);
                self.flags.set(CacheFlags::BOTTOM_PRI, false);
            }
            CachePriority::Bottom => {
                self.flags.set(CacheFlags::LOW_PRI, false);
                self.flags.set(CacheFlags::BOTTOM_PRI, true);
            }
        }
    }

    #[inline]
    fn set_in_cache_priority(&mut self, pri: CachePriority) {
        match pri {
            CachePriority::High => {
                self.flags.set(CacheFlags::IN_HIGH_PRI, true);
                self.flags.set(CacheFlags::IN_LOW_PRI, false);
            }
            CachePriority::Low => {
                self.flags.set(CacheFlags::IN_HIGH_PRI, false);
                self.flags.set(CacheFlags::IN_LOW_PRI, true);
            }
            CachePriority::Bottom => {
                self.flags.set(CacheFlags::IN_HIGH_PRI, false);
                self.flags.set(CacheFlags::IN_LOW_PRI, false);
            }
        }
    }

    #[inline]
    fn in_cache_priority(&self) -> CachePriority {
        if self.flags.contains(CacheFlags::IN_HIGH_PRI) {
            CachePriority::High
        } else if self.flags.contains(CacheFlags::IN_LOW_PRI) {
            CachePriority::Low
        } else {
            CachePriority::Bottom
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
    use crate::page_store::CacheOption;

    #[test]
    fn test_lru_base_op() {
        use super::lru::*;

        let c = Arc::new(LRUCache::new(2, -1, 0.0, 0.0));

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
    fn test_lru_pri_op() {
        use super::lru::*;

        let c = Arc::new(LRUCache::new(10, -1, 0.5, 0.2));

        // fill 1-5 as high, 6-7 as low, 8-10 as bottom.
        for n in 1..=10 {
            let p = if n <= 5 {
                CachePriority::High
            } else if n <= 7 {
                CachePriority::Low
            } else {
                CachePriority::Bottom
            };
            let h = c
                .insert(n, Some(vec![n]), 1, CacheOption::default().set_priority(p))
                .unwrap()
                .unwrap();
            drop(h);
        }

        // access low and bottom's values.
        for n in 6..=10 {
            let h = c.lookup(n).unwrap();
            drop(h);
        }

        // fill addition high element over capacity.
        let h = c
            .insert(
                11,
                Some(vec![11]),
                1,
                CacheOption::default().set_priority(CachePriority::High),
            )
            .unwrap()
            .unwrap();
        drop(h);

        // 1-5 should not be evict.
        for n in 1..=5 {
            let h = c.lookup(n).unwrap();
            drop(h);
        }

        // bottom oldest value should be evict first.
        let h = c.lookup(8);
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
