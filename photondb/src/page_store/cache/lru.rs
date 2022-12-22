use std::{
    ptr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use parking_lot::Mutex;

use super::{AtomicCacheStats, Cache, CacheEntry, Handle, LRUHandle};
use crate::page_store::{stats::CacheStats, AccessHint, Result};

pub(crate) struct LRUCache<T: Clone> {
    shards: Vec<Mutex<LRUCacheShard<T>>>,
    shard_mask: u32,
    stats: Vec<Arc<AtomicCacheStats>>,
}

struct LRUCacheShard<T: Clone> {
    head: Box<LRUHandlePtr<T>>,
    table: LRUCacheHandleTable<T>,
    capacity: usize,

    lru_usage: Arc<AtomicUsize>,
    usage: Arc<AtomicUsize>,

    stats: Arc<AtomicCacheStats>,
}

struct LRUCacheHandleTable<T: Clone> {
    data: Vec<LRUHandlePtr<T>>,
    len: usize,
}

#[derive(Clone)]
struct LRUHandlePtr<T: Clone> {
    ptr: *mut LRUHandle<T>,
}

impl<T: Clone> LRUHandlePtr<T> {
    fn as_ref(&self) -> &LRUHandle<T> {
        unsafe { &(*self.ptr) }
    }

    fn mut_ptr(&self) -> *mut LRUHandle<T> {
        self.ptr
    }
}

unsafe impl<T: Clone> Send for LRUHandlePtr<T> {}

unsafe impl<T: Clone> Sync for LRUHandlePtr<T> {}

impl<T: Clone> LRUCache<T> {
    #[allow(dead_code)]
    pub(crate) fn new(capacity: usize, num_shard_bits: i32) -> Self {
        assert!(num_shard_bits < 20);
        let num_shard_bits = if num_shard_bits >= 0 {
            num_shard_bits as u32
        } else {
            const MIN_SHARD_SIZE: usize = 32 << 20;
            let mut num_shard_bits = 0;
            let mut num_shards = capacity / MIN_SHARD_SIZE;
            loop {
                num_shards >>= 1;
                if num_shards == 0 {
                    break;
                }
                num_shard_bits += 1;
                if num_shard_bits >= 6 {
                    break;
                }
            }
            num_shard_bits
        };
        let num_shards = 1u32 << num_shard_bits;
        let per_shard_cap = (capacity + (num_shards as usize - 1)) / num_shards as usize;
        let shard_mask = num_shards - 1;
        let mut shards = Vec::with_capacity(num_shards as usize);
        let mut stats = Vec::with_capacity(num_shards as usize);
        for _ in 0..num_shards {
            let shard = LRUCacheShard::new(per_shard_cap);
            stats.push(shard.stats.clone());
            shards.push(Mutex::new(shard));
        }
        Self {
            shards,
            shard_mask,
            stats,
        }
    }

    #[inline]
    fn shard(&self, hash: u32) -> u32 {
        self.shard_mask & hash
    }

    #[inline]
    fn hash_key(key: u64) -> u32 {
        const FIBONACCI_MAGIC_NUMBER_64BIT: u64 = 11400714819323198485;
        (key.wrapping_mul(FIBONACCI_MAGIC_NUMBER_64BIT) >> 32) as u32 // TODO: cmp with hash for seprated file_id, offset and other algorithm(siphash, fnv, xxhash?).
    }
}

impl<T: Clone> Cache<T> for LRUCache<T> {
    fn insert(
        self: &std::sync::Arc<Self>,
        key: u64,
        value: Option<T>,
        charge: usize,
    ) -> Result<Option<CacheEntry<T, Self>>> {
        let hash = Self::hash_key(key);
        let idx = self.shard(hash);
        let shard = &self.shards[idx as usize];
        let mut shard = shard.lock();
        unsafe { shard.insert(key, hash, value, charge) }.map(|ptr| {
            if ptr.is_null() {
                None
            } else {
                Some(CacheEntry {
                    handle: Handle::Lru(ptr),
                    cache: self.clone(),
                    hint: None,
                })
            }
        })
    }

    fn detach(
        self: &std::sync::Arc<Self>,
        key: u64,
        value: Option<T>,
        charge: usize,
    ) -> Result<Option<CacheEntry<T, Self>>> {
        let hash = Self::hash_key(key);
        let h = Box::new(LRUHandle {
            key,
            hash,
            value,
            charge,
            detached: true,
            ..Default::default()
        });
        let handle = Box::into_raw(h);
        Ok(Some(CacheEntry {
            handle: Handle::Lru(handle),
            cache: self.clone(),
            hint: None,
        }))
    }

    fn lookup(
        self: &std::sync::Arc<Self>,
        key: u64,
        hint: crate::page_store::AccessHint,
    ) -> Option<CacheEntry<T, Self>> {
        let hash = Self::hash_key(key);
        let idx = self.shard(hash);
        let shard = &self.shards[idx as usize];
        let mut shard = shard.lock();
        unsafe {
            let ptr = shard.lookup(hash, key);
            if ptr.is_null() {
                return None;
            }
            let entry = CacheEntry {
                handle: Handle::Lru(ptr),
                cache: self.clone(),
                hint: Some(hint),
            };
            Some(entry)
        }
    }

    fn release(&self, h: &Handle<T>, hint: &Option<AccessHint>) -> bool {
        unsafe {
            if let Handle::Lru(lh) = *h {
                let hash = (*lh).hash;
                let idx = self.shard(hash);
                let mut shard = self.shards[idx as usize].lock();
                shard.release(lh, hint);
                true
            } else {
                unreachable!()
            }
        }
    }

    fn erase(self: &std::sync::Arc<Self>, key: u64) {
        unsafe {
            let hash = Self::hash_key(key);
            let idx = self.shard(hash);
            let mut shard = self.shards[idx as usize].lock();
            shard.erase(hash, key);
        }
    }

    fn stats(self: &std::sync::Arc<Self>) -> crate::page_store::stats::CacheStats {
        let mut summary = CacheStats::default();
        for s in &self.stats {
            let shard_stats = s.snapshot();
            summary = summary.add(&shard_stats);
        }
        summary
    }
}

impl<T: Clone> LRUCacheShard<T> {
    pub(crate) fn new(capacity: usize) -> Self {
        let mut linked = Box::new(LRUHandle::default());
        linked.next_linked = linked.as_mut();
        linked.prev_linked = linked.as_mut();
        let ptr = Box::into_raw(linked);
        let head = Box::new(LRUHandlePtr { ptr });
        Self {
            head,
            table: LRUCacheHandleTable::new(),
            capacity,
            lru_usage: Default::default(),
            usage: Default::default(),
            stats: Default::default(),
        }
    }

    unsafe fn insert(
        &mut self,
        key: u64,
        hash: u32,
        value: Option<T>,
        charge: usize,
    ) -> Result<*mut LRUHandle<T>> {
        self.evict_from_lru(charge);
        let h = Box::new(LRUHandle {
            key,
            value,
            hash,
            charge,
            detached: false,
            ..Default::default()
        });
        let lhd = Box::into_raw(h);
        let old = self.table.insert(lhd)?;
        if !old.is_null() {
            self.try_remove_cache_handle(old);
        }
        self.stats.insert.inc();
        self.usage.fetch_add(charge, Ordering::Relaxed);
        (*lhd).add_ref();
        Ok(lhd)
    }

    unsafe fn release(&mut self, h: *mut LRUHandle<T>, hint: &Option<AccessHint>) {
        assert!(!h.is_null());
        if (*h).detached {
            drop(Box::from_raw(h));
            return;
        }

        // The handle should not be in lru before calling this method.
        let last_reference = (*h).unref();
        // If the handle is still referenced by someone else, do nothing and return.
        if !last_reference {
            return;
        }

        // Keep the handle in lru list if it is still in the cache and the cache is not
        // over-sized.
        if (*h).is_in_cache() {
            if self.usage.load(Ordering::Relaxed) <= self.capacity {
                let as_recent =
                    hint.is_none() || (*hint.as_ref().unwrap() != AccessHint::READ_THEN_REPLACE);
                self.lru_insert(h, as_recent);
                return;
            }

            // Remove the handle from table.
            self.table.remove((*h).key, (*h).hash);
        }

        self.clear_handle(h)
    }

    unsafe fn lookup(&mut self, hash: u32, key: u64) -> *mut LRUHandle<T> {
        let e = self.table.lookup(hash, key);
        if !e.is_null() {
            if !(*e).has_refs() {
                self.lru_remove(e);
            }
            self.stats.lookup_hit.inc();
            (*e).add_ref();
        } else {
            self.stats.lookup_miss.inc();
        }
        e
    }

    unsafe fn erase(&mut self, hash: u32, key: u64) {
        let h = self.table.remove(key, hash);
        if !h.is_null() {
            self.try_remove_cache_handle(h)
        }
        self.stats.active_evict.inc();
    }

    unsafe fn try_remove_cache_handle(&mut self, h: *mut LRUHandle<T>) {
        assert!(!h.is_null());
        if !(*h).has_refs() {
            self.lru_remove(h);
            self.clear_handle(h);
        }
    }

    unsafe fn lru_insert(&mut self, e: *mut LRUHandle<T>, as_recent: bool) {
        assert!(!e.is_null());
        if as_recent {
            (*e).next_linked = self.head.mut_ptr();
            (*e).prev_linked = self.head.as_ref().as_ref().prev_linked;
        } else {
            (*e).prev_linked = self.head.mut_ptr();
            (*e).next_linked = self.head.as_ref().as_ref().next_linked;
        }
        (*(*e).prev_linked).next_linked = e;
        (*(*e).next_linked).prev_linked = e;
        self.lru_usage.fetch_add((*e).charge, Ordering::Relaxed);
    }

    unsafe fn lru_remove(&mut self, e: *mut LRUHandle<T>) {
        assert!(!e.is_null());

        (*(*e).next_linked).prev_linked = (*e).prev_linked;
        (*(*e).prev_linked).next_linked = (*e).next_linked;
        (*e).prev_linked = ptr::null_mut();
        (*e).next_linked = ptr::null_mut();
        self.lru_usage.fetch_sub((*e).charge, Ordering::Relaxed);
    }

    unsafe fn evict_from_lru(&mut self, charge: usize) {
        while self.usage.load(Ordering::Relaxed) + charge > self.capacity
            && !std::ptr::eq((*self.head.ptr).next_linked, self.head.ptr)
        {
            let old_ptr = (*self.head.ptr).next_linked;
            self.table.remove((*old_ptr).key, (*old_ptr).hash);
            self.lru_remove(old_ptr);
            self.clear_handle(old_ptr);
            self.stats.passive_evict.inc();
        }
    }

    unsafe fn clear_handle(&mut self, lh: *mut LRUHandle<T>) {
        assert!(!lh.is_null());
        assert!(!(*lh).is_in_cache());
        assert!(!(*lh).has_refs());
        self.usage.fetch_sub((*lh).charge, Ordering::Relaxed);
        drop(Box::from_raw(lh));
    }
}

impl<T: Clone> LRUCacheHandleTable<T> {
    pub(crate) fn new() -> Self {
        Self {
            data: vec![
                LRUHandlePtr {
                    ptr: ptr::null_mut()
                };
                16
            ],
            len: 0,
        }
    }

    unsafe fn find_pointer(&self, idx: usize, key: u64) -> (*mut LRUHandle<T>, *mut LRUHandle<T>) {
        let mut ptr = self.data[idx].mut_ptr();
        let mut prev = ptr::null_mut();
        while !ptr.is_null() && (*ptr).key != key {
            prev = ptr;
            ptr = (*ptr).next_hash;
        }
        (prev, ptr)
    }

    unsafe fn insert(&mut self, proto: *mut LRUHandle<T>) -> Result<*mut LRUHandle<T>> {
        assert!(!proto.is_null());
        assert!(!(*proto).is_in_cache());
        (*proto).set_in_cache(true);
        assert!(self.data.len().is_power_of_two());
        let idx = ((*proto).hash as usize) & (self.data.len() - 1);
        let (mut prev, ptr) = self.find_pointer(idx, (*proto).key);
        if prev.is_null() {
            self.data[idx] = LRUHandlePtr { ptr: proto };
        } else {
            (*prev).next_hash = proto;
        }

        if !ptr.is_null() {
            assert_eq!((*ptr).key, (*proto).key);
            assert!((*ptr).is_in_cache());
            (*ptr).set_in_cache(false);
            (*proto).next_hash = (*ptr).next_hash;
            return Ok(ptr);
        }

        (*proto).next_hash = ptr;

        self.len += 1;
        if self.len > self.data.len() {
            self.resize();
        }

        Ok(ptr::null_mut())
    }

    unsafe fn lookup(&self, hash: u32, key: u64) -> *mut LRUHandle<T> {
        assert!(self.data.len().is_power_of_two());
        let idx = (hash as usize) & (self.data.len() - 1);
        let (_, ptr) = self.find_pointer(idx, key);
        ptr
    }

    unsafe fn resize(&mut self) {
        let mut l = std::cmp::max(16, self.data.len());
        let next_capacity = self.len * 3 / 2;
        while l < next_capacity {
            l <<= 1;
        }
        let mut count = 0;
        let mut new_list = vec![
            LRUHandlePtr {
                ptr: ptr::null_mut()
            };
            l
        ];
        for head in self.data.drain(..) {
            let mut handle = head.mut_ptr();
            while !handle.is_null() {
                let idx = (*handle).hash as usize & (l - 1);
                let next = (*handle).next_hash;
                (*handle).next_hash = new_list[idx].mut_ptr();
                new_list[idx] = LRUHandlePtr { ptr: handle };
                handle = next;
                count += 1;
            }
        }
        assert_eq!(count, self.len);
        self.data = new_list;
    }

    unsafe fn remove(&mut self, key: u64, hash: u32) -> *mut LRUHandle<T> {
        assert!(self.data.len().is_power_of_two());
        let idx = (hash as usize) & (self.data.len() - 1);

        let (mut prev, ptr) = self.find_pointer(idx, key);
        if ptr.is_null() {
            return ptr::null_mut();
        }
        assert!((*ptr).is_in_cache());
        (*ptr).set_in_cache(false);
        if prev.is_null() {
            self.data[idx] = LRUHandlePtr {
                ptr: (*ptr).next_hash,
            };
        } else {
            (*prev).next_hash = (*ptr).next_hash;
        }
        self.len -= 1;
        ptr
    }
}
