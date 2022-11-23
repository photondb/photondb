use ::std::sync::{
    atomic::{AtomicU32, AtomicU64},
    Arc,
};

use crate::*;

pub(crate) trait Cache<T: Clone>: Sized {
    fn insert(
        self: &Arc<Self>,
        key: u64,
        value: Option<T>,
        charge: usize,
    ) -> Result<Option<CacheEntry<T, Self>>>;

    fn lookup(self: &Arc<Self>, key: u64) -> Option<CacheEntry<T, Self>>;

    fn release(&self, h: *mut Handle<T>) -> bool;

    fn erase(&self, key: u64);
}

pub(crate) struct CacheEntry<T, C>
where
    T: Clone,
    C: Cache<T>,
{
    handle: *mut Handle<T>,
    cache: Arc<C>,
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
        self.cache.release(self.handle);
    }
}

impl<T, C> CacheEntry<T, C>
where
    T: Clone,
    C: Cache<T>,
{
    #[allow(dead_code)]
    pub(crate) fn key(&self) -> u64 {
        unsafe { (*self.handle).key }
    }

    pub(crate) fn value(&self) -> &T {
        unsafe { (*self.handle).value.as_ref().unwrap() }
    }
}

pub(crate) struct Handle<T: Clone> {
    key: u64,
    hash: u32, // TODO: cmp with bijective hash algorithm to save hash into key.
    value: Option<T>,
    charge: usize,

    meta: AtomicU64,
    displacements: AtomicU32,
    detached: bool,
}

impl<T: Clone> Default for Handle<T> {
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

impl<T: Clone> Handle<T> {
    fn insert(&mut self, key: u64, value: Option<T>, hash: u32, charge: usize) {
        self.key = key;
        self.value = value;
        self.hash = hash;
        self.charge = charge;
    }
}

const COUNTER_NUM_BITS: u8 = 30;
const COUNTER_MASK: u64 = (1u64 << COUNTER_NUM_BITS) - 1;

const ACQUIRE_COUNTER_SHIFT: u8 = 0;
const ACQUIRE_INCREMENT: u64 = 1u64 << ACQUIRE_COUNTER_SHIFT;
const RELEASE_COUNTER_SHIFT: u8 = COUNTER_NUM_BITS;
const RELEASE_INCREMENT: u64 = 1u64 << RELEASE_COUNTER_SHIFT;

// For reading or updating the state marker in meta word
const STATE_SHIFT: u8 = 2u8 * COUNTER_NUM_BITS;

// Bits contribution to state marker.
// Occupied means any state other than empty
const STATE_OCCUPIED_BIT: u8 = 0b100;
// Shareable means the entry is reference counted (visible or invisible)
// (only set if also occupied)
const STATE_SHAREABLE_BIT: u8 = 0b010;
// Visible is only set if also shareable
const STATE_VISIBLE_BIT: u8 = 0b001;

// Complete state markers (not shifted into full word)
const STATE_EMPTY: u8 = 0b000;
const STATE_CONSTRUCTION: u8 = STATE_OCCUPIED_BIT;
const STATE_INVISIBLE: u8 = STATE_OCCUPIED_BIT | STATE_SHAREABLE_BIT;
const STATE_VISIBLE: u8 = STATE_OCCUPIED_BIT | STATE_SHAREABLE_BIT | STATE_VISIBLE_BIT;

// Constants for initializing the countdown clock. (Countdown clock is only
// in effect with zero refs, acquire counter == release counter, and in that
// case the countdown clock == both of those counters.)
const HIGH_COUNT_DOWN: u8 = 3;
const LOW_COUNT_DOWN: u8 = 2;
#[allow(dead_code)]
const BOTTOM_COUNT_DOWN: u8 = 1;
// During clock update, treat any countdown clock value greater than this
// value the same as this value.
const MAX_COUNT_DOWN: u8 = HIGH_COUNT_DOWN;

pub(crate) mod clock {

    use ::std::{
        mem,
        ptr::{self, null_mut},
        sync::{atomic::*, Arc},
    };

    use super::*;

    const LOAD_FACTOR: f64 = 0.7;
    const STRICT_LOAD_FACTOR: f64 = 0.84;

    pub(crate) struct ClockCache<T: Clone> {
        shards: Vec<ClockCacheShard<T>>, // TODO: align to cacheline.
        shard_mask: u32,
    }

    struct ClockCacheShard<T: Clone> {
        table: ClockCacheHandleTable<T>,
        capacity: usize,
    }

    struct ClockHandlePtr<T: Clone> {
        ptr: *mut Handle<T>,
    }

    impl<T: Clone> ClockHandlePtr<T> {
        fn as_ref(&self) -> &Handle<T> {
            unsafe { &(*self.ptr) }
        }

        fn mut_ptr(&self) -> *mut Handle<T> {
            self.ptr
        }
    }

    impl<T: Clone> Drop for ClockHandlePtr<T> {
        fn drop(&mut self) {
            unsafe {
                drop(Box::from_raw(self.ptr));
            }
        }
    }

    unsafe impl<T: Clone> Send for ClockHandlePtr<T> {}

    unsafe impl<T: Clone> Sync for ClockHandlePtr<T> {}

    #[derive(Default)]
    struct ClockCacheHandleTable<T: Clone> {
        length_bits: u64,
        length_bits_mask: u32,
        occupancy_limit: u32,
        strict_capacity_limit: bool,

        handles: Vec<ClockHandlePtr<T>>,

        occupancy: AtomicU32,
        usage: AtomicUsize,
        detached_usage: AtomicUsize,
        clock_pointer: AtomicU64,
    }

    impl<T: Clone> ClockCacheHandleTable<T> {
        fn new(length_bits: u64, strict_capacity_limit: bool) -> Self {
            let length_bits_mask = ((1 << length_bits) - 1) as u32;
            let occupancy_limit = ((1 << length_bits) as f64 * STRICT_LOAD_FACTOR) as u32;
            let mut handles = Vec::with_capacity(1 << length_bits);
            for _ in 0..handles.capacity() {
                let h = Box::new(Handle::default());
                handles.push(ClockHandlePtr {
                    ptr: Box::into_raw(h),
                });
            }
            let usage = AtomicUsize::new(handles.len() * mem::size_of::<Handle<T>>());
            Self {
                length_bits,
                length_bits_mask,
                occupancy_limit,
                strict_capacity_limit,
                handles,
                occupancy: Default::default(),
                usage,
                detached_usage: Default::default(),
                clock_pointer: Default::default(),
            }
        }

        fn lookup(&self, key: u64, hash: u32) -> *mut Handle<T> {
            let (slot, _) = self.find_slot(
                hash,
                |hp| {
                    let h = hp.as_ref();
                    let mut old_meta = h.meta.fetch_add(ACQUIRE_INCREMENT, Ordering::Acquire);
                    if (old_meta >> STATE_SHIFT) as u8 == STATE_VISIBLE {
                        if h.key == key {
                            return true;
                        } else {
                            old_meta = h.meta.fetch_sub(ACQUIRE_INCREMENT, Ordering::Release);
                        }
                    } else if (old_meta >> STATE_SHIFT) as u8 == STATE_INVISIBLE {
                        old_meta = h.meta.fetch_sub(ACQUIRE_INCREMENT, Ordering::Release);
                    } else {
                    }
                    _ = old_meta;
                    false
                },
                |hp| hp.as_ref().displacements.load(Ordering::Relaxed) == 0,
                |_h| {},
            );
            let Some(slot) = slot else {
                return null_mut();
            };
            let Some(h) = self.handles.get(slot) else {
                return null_mut();
            };
            h.mut_ptr()
        }

        fn insert(&self, proto: Handle<T>, capacity: usize) -> Result<*mut Handle<T>> {
            // Add occupany ahead, revert if not real occupy.
            let old_occupancy = self.occupancy.fetch_add(1, Ordering::Acquire);
            // Whether we over-committed and need an eviction to make up for it
            let need_evict_for_occupancy = old_occupancy > self.occupancy_limit;
            // Usage/capacity handling is somewhat different depending on
            // `strict_capacity_limit`.
            let mut use_detached_insert = false;
            let total_charge = proto.charge;
            if self.strict_capacity_limit {
                let r = self.charge_usage_maybe_evict_strict(
                    total_charge,
                    capacity,
                    need_evict_for_occupancy,
                );
                if let Err(err) = r {
                    self.occupancy.fetch_sub(1, Ordering::Relaxed);
                    return Err(err);
                }
            } else {
                let success = self.charge_usage_maybe_evict_non_strict(
                    total_charge,
                    capacity,
                    need_evict_for_occupancy,
                );
                if !success {
                    self.occupancy.fetch_sub(1, Ordering::Relaxed);
                    self.usage.fetch_add(total_charge, Ordering::Relaxed);
                    use_detached_insert = true;
                }
            }

            if !use_detached_insert {
                let initial_countdown = LOW_COUNT_DOWN;
                let (slot, _) = self.find_slot(
                    proto.hash,
                    |hp| {
                        let h = hp.as_ref();
                        // Optimistically transition the slot from "empty" to
                        // "under construction" (no effect on other states)
                        let old_meta = h
                            .meta
                            .fetch_or((STATE_OCCUPIED_BIT as u64) << STATE_SHIFT, Ordering::AcqRel);
                        let old_state = (old_meta >> STATE_SHIFT) as u8;

                        if old_state == STATE_EMPTY {
                            // Inserting into an available slot.
                            unsafe {
                                (*hp.mut_ptr()).insert(
                                    proto.key,
                                    proto.value.clone(),
                                    proto.hash,
                                    proto.charge,
                                );
                            }

                            // Transition from "under construction" state to "visible" state
                            let mut new_meta = (STATE_VISIBLE as u64) << STATE_SHIFT;

                            // Maybe with an outstanding reference
                            new_meta |= (initial_countdown as u64) << ACQUIRE_COUNTER_SHIFT;
                            new_meta |= (initial_countdown as u64 - 1) << RELEASE_COUNTER_SHIFT;

                            let old_meta = h.meta.swap(new_meta, Ordering::Release);
                            assert!((old_meta >> STATE_SHIFT) as u8 == STATE_CONSTRUCTION);

                            true
                        } else if old_state != STATE_INVISIBLE {
                            // Slot not usable / touchable now
                            false
                        } else {
                            // Existing, visible entry, which might be a match.
                            // But first, we need to acquire a ref to read it. In fact, number
                            // of refs for initial countdown, so
                            // that we boost the clock state if
                            // this is a match.
                            let mut old_meta = h.meta.fetch_add(
                                initial_countdown as u64 * ACQUIRE_INCREMENT,
                                Ordering::AcqRel,
                            );
                            if (old_meta >> STATE_SHIFT) as u8 == STATE_VISIBLE {
                                if h.key == proto.key {
                                    old_meta = h.meta.fetch_add(
                                        initial_countdown as u64 * RELEASE_INCREMENT,
                                        Ordering::AcqRel,
                                    );
                                    Self::correct_near_overflow(old_meta, &h.meta);
                                    use_detached_insert = true;
                                    return true;
                                } else {
                                    old_meta = h.meta.fetch_sub(
                                        initial_countdown as u64 * ACQUIRE_INCREMENT,
                                        Ordering::AcqRel,
                                    );
                                }
                            } else if (old_meta >> STATE_SHIFT) as u8 == STATE_INVISIBLE {
                                old_meta = h.meta.fetch_sub(
                                    initial_countdown as u64 * ACQUIRE_INCREMENT,
                                    Ordering::AcqRel,
                                );
                            } else {
                                // For other states, incrementing the
                                // acquire
                                // counter has no effect
                                // so we don't need to undo it.
                                // Slot not usable / touchable now.
                            }
                            _ = old_meta;
                            false
                        }
                    },
                    |_h| false,
                    |hp| {
                        hp.as_ref().displacements.fetch_add(1, Ordering::Relaxed);
                    },
                );
                if slot.is_none() {
                    assert!(self.table_size() < 256);
                    use_detached_insert = true;
                }
                if !use_detached_insert {
                    // Successfully inserted
                    let h = self.handles.get(slot.unwrap()).unwrap();
                    return Ok(h.mut_ptr());
                }

                if let Some(slot) = slot {
                    let h = self.handles.get(slot).unwrap();
                    self.rollback(proto.hash, h.mut_ptr());
                }
                self.occupancy.fetch_sub(1, Ordering::Relaxed);
            }

            assert!(use_detached_insert);
            let h = self.detached_insert(&proto);

            Ok(h)
        }

        fn charge_usage_maybe_evict_non_strict(
            &self,
            total_charge: usize,
            capacity: usize,
            need_evict_for_occupancy: bool,
        ) -> bool {
            let old_usage = self.usage.load(Ordering::Relaxed);
            let mut need_evict_charge =
                if old_usage + total_charge <= capacity || total_charge > old_usage {
                    0
                } else {
                    let mut need_charge = total_charge;
                    if old_usage > capacity {
                        need_charge += (capacity / 1024).min(total_charge) + 1;
                    }
                    need_charge
                };
            if need_evict_for_occupancy && need_evict_charge == 0 {
                need_evict_charge = 1;
            }
            let evicted_charge = if need_evict_charge > 0 {
                let (evicted_charge, evicted_count) = self.evit(need_evict_charge);
                if need_evict_for_occupancy && evicted_count == 0 {
                    assert!(evicted_charge == 0);
                    return false;
                } else {
                    self.occupancy
                        .fetch_sub(evicted_count as u32, Ordering::Release);
                }
                evicted_charge
            } else {
                0
            };
            if total_charge > evicted_charge {
                self.usage
                    .fetch_add(total_charge - evicted_charge, Ordering::Relaxed);
            } else {
                self.usage
                    .fetch_sub(evicted_charge - total_charge, Ordering::Relaxed);
            }
            assert!(self.usage.load(Ordering::Relaxed) < usize::MAX / 2);
            true
        }

        fn charge_usage_maybe_evict_strict(
            &self,
            total_charge: usize,
            capacity: usize,
            need_evict_for_occupancy: bool,
        ) -> Result<()> {
            if total_charge > capacity {
                return Err(Error::MemoryLimit);
            }
            // Grab any available capacity, and free up any more required.
            let old_usage = self.usage.load(Ordering::Relaxed);
            let new_usage = if old_usage != capacity {
                let mut new_usage;
                loop {
                    new_usage = capacity.min(old_usage + total_charge);
                    if self
                        .usage
                        .compare_exchange_weak(
                            old_usage,
                            new_usage,
                            Ordering::Release,
                            Ordering::Relaxed,
                        )
                        .is_ok()
                    {
                        break;
                    }
                }
                new_usage
            } else {
                old_usage
            };
            let need_evict_charge = old_usage + total_charge - new_usage;
            let mut request_evict_charge = need_evict_charge;
            if need_evict_for_occupancy && request_evict_charge == 0 {
                request_evict_charge = 1;
            }
            if request_evict_charge > 0 {
                let (evicted_charge, evicted_count) = self.evit(request_evict_charge);
                self.occupancy
                    .fetch_sub(evicted_count as u32, Ordering::Release);
                if evicted_charge > need_evict_charge {
                    assert!(evicted_count > 0);
                    // // Evicted more than enough, maitain usage.
                    self.usage
                        .fetch_sub(evicted_charge - need_evict_charge, Ordering::Relaxed);
                } else if evicted_charge < need_evict_charge
                    || (need_evict_for_occupancy && evicted_count == 0)
                {
                    // Roll back to old_usage - evicted
                    self.usage
                        .fetch_sub(evicted_charge + (new_usage - old_usage), Ordering::Relaxed);
                    return Err(Error::MemoryLimit); // TODO: detail cause.
                }
                assert!(evicted_count > 0)
            }
            Ok(())
        }

        fn detached_insert(&self, proto: &Handle<T>) -> *mut Handle<T> {
            let h = Box::new(Handle {
                key: proto.key,
                value: proto.value.clone(),
                hash: proto.hash,
                charge: proto.charge,
                detached: true,
                meta: Default::default(),
                displacements: Default::default(),
            });
            let mut meta = (STATE_INVISIBLE as u64) << STATE_SHIFT;
            meta |= 1 << ACQUIRE_COUNTER_SHIFT;
            h.meta.store(meta, Ordering::Release);
            self.detached_usage
                .fetch_add(proto.charge, Ordering::Relaxed);
            Box::into_raw(h)
        }

        fn release(&self, hp: *mut Handle<T>) -> bool {
            if hp.is_null() {
                return false;
            }
            let erase_if_last_ref = false; //TODO: pass by param.
            let h = unsafe { &(*hp) };
            let mut old_meta = h.meta.fetch_add(RELEASE_INCREMENT, Ordering::Release);
            assert!((old_meta >> STATE_SHIFT) as u8 & STATE_SHAREABLE_BIT > 0);
            assert!(
                ((old_meta >> ACQUIRE_COUNTER_SHIFT) & COUNTER_MASK)
                    != ((old_meta >> RELEASE_COUNTER_SHIFT) & COUNTER_MASK),
            );
            if erase_if_last_ref || (old_meta >> STATE_SHIFT) as u8 == STATE_INVISIBLE {
                old_meta += RELEASE_INCREMENT as u64;
                loop {
                    if Self::ref_count(old_meta) != 0 {
                        Self::correct_near_overflow(old_meta, &h.meta);
                        return false;
                    }
                    if old_meta & ((STATE_SHAREABLE_BIT as u64) << STATE_SHIFT) == 0 {
                        // Someone else took ownership
                        return false;
                    }
                    if h.meta
                        .compare_exchange_weak(
                            old_meta,
                            (STATE_CONSTRUCTION as u64) << STATE_SHIFT,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        break;
                    }
                }
                let total_charge = h.charge;
                if h.detached {
                    unsafe {
                        drop(Box::from_raw(hp));
                    }
                    self.detached_usage
                        .fetch_sub(total_charge, Ordering::Relaxed);
                    self.usage.fetch_sub(total_charge, Ordering::Relaxed);
                } else {
                    Self::free_data_mark_empty(h);
                    self.rollback(h.hash, hp);
                    self.reclaim_entry_usage(total_charge);
                }
                true
            } else {
                Self::correct_near_overflow(old_meta, &h.meta);
                false
            }
        }

        fn erase(&self, key: u64, hash: u32) {
            self.find_slot(
                hash,
                |hp| {
                    let h = hp.as_ref();
                    let mut old_meta = h.meta.fetch_add(ACQUIRE_INCREMENT, Ordering::Acquire);
                    // let mut old_meta = h.meta.fetch_add(ACQUIRE_INCREMENT, Ordering::Acquire);
                    // Check if it's an entry visible to lookups
                    if (old_meta >> STATE_SHIFT) as u8 == STATE_VISIBLE {
                        if h.key == key {
                            old_meta = h.meta.fetch_and(
                                !((STATE_VISIBLE_BIT as u64) << STATE_SHIFT),
                                Ordering::AcqRel,
                            );
                            old_meta &= !((STATE_VISIBLE_BIT as u64) << STATE_SHIFT);
                            loop {
                                let refcount = Self::ref_count(old_meta);
                                assert!(refcount > 0);
                                if refcount > 1 {
                                    // Not last ref at some point in time during this Erase call
                                    // Pretend we never took the reference
                                    h.meta.fetch_sub(ACQUIRE_INCREMENT, Ordering::Release);
                                    break;
                                } else if h
                                    .meta
                                    .compare_exchange_weak(
                                        old_meta,
                                        (STATE_CONSTRUCTION as u64) << STATE_SHIFT,
                                        Ordering::AcqRel,
                                        Ordering::Acquire,
                                    )
                                    .is_ok()
                                {
                                    assert!(hash == h.hash);
                                    let total_charge = h.charge;
                                    Self::free_data_mark_empty(h);
                                    self.reclaim_entry_usage(total_charge);
                                    self.rollback(hash, hp.mut_ptr());
                                    break;
                                }
                            }
                        } else {
                            h.meta.fetch_sub(ACQUIRE_INCREMENT, Ordering::Release);
                        }
                    } else if (old_meta >> STATE_SHIFT) as u8 == STATE_INVISIBLE {
                        h.meta.fetch_sub(ACQUIRE_INCREMENT, Ordering::Release);
                    } else {
                    }
                    false
                },
                |hp| hp.as_ref().displacements.load(Ordering::Relaxed) == 0,
                |_hp| {},
            );
        }

        #[inline]
        fn reclaim_entry_usage(&self, totol_charge: usize) {
            let old_occupancy = self.occupancy.fetch_sub(1, Ordering::Release);
            assert!(old_occupancy > 0);
            let old_usage = self.usage.fetch_sub(totol_charge, Ordering::Relaxed);
            assert!(old_usage > 0);
        }

        fn ref_count(meta: u64) -> u64 {
            ((meta >> ACQUIRE_COUNTER_SHIFT) & COUNTER_MASK)
                - ((meta >> RELEASE_COUNTER_SHIFT) & COUNTER_MASK)
        }

        const fn table_size(&self) -> u64 {
            1u64 << self.length_bits
        }

        fn correct_near_overflow(old_meta: u64, meta: &AtomicU64) {
            const COUNTER_TO_BIT: u64 = 1u64 << (COUNTER_NUM_BITS - 1);
            const CLEAR_BITS: u64 = (COUNTER_TO_BIT << ACQUIRE_COUNTER_SHIFT)
                | (COUNTER_TO_BIT << RELEASE_COUNTER_SHIFT);
            const CHECK_BITS: u64 =
                (COUNTER_TO_BIT | (MAX_COUNT_DOWN as u64 + 1)) << RELEASE_COUNTER_SHIFT;

            if old_meta & CHECK_BITS > 0 {
                meta.fetch_and(!CLEAR_BITS, Ordering::Relaxed);
            }
        }

        fn find_slot(
            &self,
            hash: u32,
            mut match_fn: impl FnMut(&ClockHandlePtr<T>) -> bool,
            abort_fn: impl Fn(&ClockHandlePtr<T>) -> bool,
            update_fn: impl Fn(&ClockHandlePtr<T>),
        ) -> (Option<usize>, u32) {
            let mut probe = 0;
            let base = self.mod_table_size(Self::remix1(hash));
            let increment = Self::remix2(hash) | 1;
            let mut current = self.mod_table_size(base + probe * increment);
            while probe <= self.length_bits_mask {
                let h = self.handles.get(current as usize).unwrap();
                if match_fn(h) {
                    probe += 1;
                    return (Some(current as usize), probe);
                }
                if abort_fn(h) {
                    return (None, probe);
                }
                probe += 1;
                update_fn(h);
                current = self.mod_table_size(current + increment);
            }
            (None, probe)
        }

        fn evit(
            &self,
            requested_charge: usize,
        ) -> (
            usize, /* evicted_charge */
            usize, /* evicted count */
        ) {
            assert!(requested_charge > 0);
            let mut evicted_charge = 0;
            let mut evicted_count = 0;

            const STEP_SIZE: u64 = 4;
            let mut old_clock_pointer = self.clock_pointer.fetch_add(STEP_SIZE, Ordering::Relaxed);
            let max_clock_pointer =
                old_clock_pointer + ((MAX_COUNT_DOWN as u64) << self.length_bits);
            loop {
                for i in 0..STEP_SIZE {
                    let idx = self.mod_table_size((old_clock_pointer + i) as u32);
                    let hp = self.handles.get(idx as usize).unwrap();
                    let evicting = self.clock_update(hp);
                    if evicting {
                        // evicted_charge
                        let h = hp.as_ref();
                        self.rollback(h.hash, hp.mut_ptr());
                        evicted_charge += h.charge;
                        evicted_count += 1;
                        Self::free_data_mark_empty(h);
                    }
                }

                if evicted_charge >= requested_charge {
                    return (evicted_charge, evicted_count);
                }

                if old_clock_pointer >= max_clock_pointer {
                    return (evicted_charge, evicted_count);
                }

                old_clock_pointer = self.clock_pointer.fetch_add(STEP_SIZE, Ordering::Relaxed);
            }
        }

        fn free_data_mark_empty(h: &Handle<T>) {
            let meta = h.meta.swap(0, Ordering::Release);
            assert!((meta >> (STATE_SHIFT as u64)) as u8 == STATE_CONSTRUCTION);
        }

        fn clock_update(&self, hp: &ClockHandlePtr<T>) -> bool {
            let h = hp.as_ref();
            let meta = h.meta.load(Ordering::Relaxed);
            let acquire_count = (meta >> ACQUIRE_COUNTER_SHIFT) & COUNTER_MASK;
            let release_count = (meta >> RELEASE_COUNTER_SHIFT) & COUNTER_MASK;
            if acquire_count != release_count {
                // Only clock update entries with no outstanding refs
                return false;
            }
            if ((meta >> STATE_SHIFT) & STATE_SHAREABLE_BIT as u64) == 0 {
                // Only clock update Shareable entries
                return false;
            }
            if ((meta >> STATE_SHIFT) as u8 == STATE_INVISIBLE) && acquire_count > 0 {
                // Decrement clock
                let new_count = (acquire_count - 1).min(MAX_COUNT_DOWN as u64 - 1);
                // Compare-exchange in the decremented clock info, but
                // not aggressively
                let new_meta = ((STATE_VISIBLE as u64) << STATE_SHIFT as u64)
                    | (new_count << RELEASE_COUNTER_SHIFT)
                    | (new_count << ACQUIRE_COUNTER_SHIFT);
                let _ =
                    h.meta
                        .compare_exchange(meta, new_meta, Ordering::Release, Ordering::Relaxed);
                return false;
            }
            h.meta
                .compare_exchange(
                    meta,
                    (STATE_CONSTRUCTION as u64) << STATE_SHIFT as u64,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok()
        }

        fn rollback(&self, hash: u32, h: *mut Handle<T>) {
            let mut current = self.mod_table_size(Self::remix1(hash));
            let increment = Self::remix2(hash) | 1;
            loop {
                if ptr::eq(self.handles.get(current as usize).unwrap().mut_ptr(), h) {
                    break;
                }
                let hh = self.handles.get(current as usize).unwrap();
                hh.as_ref().displacements.fetch_sub(1, Ordering::Relaxed);
                current = self.mod_table_size(current + increment);
            }
        }

        const fn mod_table_size(&self, x: u32) -> u32 {
            x & self.length_bits_mask
        }

        const fn remix1(hash: u32) -> u32 {
            (((hash as u64) * 0xbc9f1d35) >> 29) as u32
        }

        const fn remix2(hash: u32) -> u32 {
            (((hash as u64) * 0x7a2bb9d5) >> 29) as u32
        }
    }

    impl<T: Clone> Drop for ClockCacheHandleTable<T> {
        fn drop(&mut self) {
            for hp in &self.handles {
                let h = hp.as_ref();
                let meta = h.meta.load(Ordering::Relaxed);
                let ref_cnt = Self::ref_count(meta);
                let state = (meta >> STATE_SHIFT) as u8;
                match state {
                    STATE_EMPTY => {}
                    STATE_INVISIBLE => {
                        assert_eq!(ref_cnt, 0);
                        self.rollback(h.hash, hp.mut_ptr());
                        self.reclaim_entry_usage(h.charge);
                    }
                    STATE_VISIBLE => {
                        assert_eq!(ref_cnt, 0);
                        self.rollback(h.hash, hp.mut_ptr());
                        self.reclaim_entry_usage(h.charge);
                    }
                    _ => unreachable!(),
                }
            }
            assert!(
                self.usage.load(Ordering::Relaxed) == 0
                    || self.usage.load(Ordering::Relaxed)
                        == self.handles.len() * mem::size_of::<Handle<T>>()
            );
            assert_eq!(self.occupancy.load(Ordering::Relaxed), 0);
        }
    }

    impl<T: Clone> ClockCache<T> {
        pub(crate) fn new(
            capacity: usize,
            est_value_size: usize,
            num_shard_bits: i32,
            strict_capacity_limit: bool,
        ) -> Self {
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
                    if num_shard_bits >= 6 {
                        num_shard_bits += 1;
                        // No more than 6.
                        break;
                    }
                }
                num_shard_bits
            };
            let num_shards = 1u32 << num_shard_bits;
            let per_shard_cap = (capacity + (num_shards as usize - 1)) / num_shards as usize;
            let shard_mask = num_shards - 1;
            let mut shards = Vec::with_capacity(num_shards as usize);
            for _ in 0..num_shards {
                shards.push(ClockCacheShard::new(
                    per_shard_cap,
                    est_value_size,
                    strict_capacity_limit,
                ))
            }
            Self { shards, shard_mask }
        }
    }

    impl<T: Clone> ClockCacheShard<T> {
        fn new(capacity: usize, est_value_size: usize, strict_capacity_limit: bool) -> Self {
            let hash_bits = Self::hash_bits(capacity, est_value_size);
            let data = ClockCacheHandleTable::new(hash_bits, strict_capacity_limit);
            Self {
                table: data,
                capacity,
            }
        }

        fn hash_bits(capacity: usize, est_value_size: usize) -> u64 {
            let mut average_slot_charge = est_value_size as f64 * LOAD_FACTOR;
            average_slot_charge += mem::size_of::<Handle<T>>() as f64;
            let num_slots = (capacity as f64 / average_slot_charge + 0.999999) as u64;
            let mut hash_bits = ((num_slots << 1) as f64 - 1.).log2().floor().min(32.) as u64;
            while hash_bits > 0 && mem::size_of::<Handle<T>>() << hash_bits > capacity {
                hash_bits -= 1;
            }
            hash_bits
        }

        fn insert(
            &self,
            key: u64,
            hash: u32,
            value: Option<T>,
            charge: usize,
        ) -> Result<*mut Handle<T>> {
            let h = Handle {
                key,
                value,
                hash,
                charge,
                meta: Default::default(),
                displacements: Default::default(),
                detached: false,
            };
            self.table.insert(h, self.capacity)
        }

        fn lookup(&self, key: u64, hash: u32) -> *mut Handle<T> {
            self.table.lookup(key, hash)
        }

        fn release(&self, h: *mut Handle<T>) -> bool {
            self.table.release(h)
        }

        fn erase(&self, key: u64, hash: u32) {
            self.table.erase(key, hash)
        }
    }

    impl<T: Clone> Cache<T> for ClockCache<T> {
        fn insert(
            self: &Arc<Self>,
            key: u64,
            value: Option<T>,
            charge: usize,
        ) -> Result<Option<CacheEntry<T, Self>>> {
            let hash = Self::hash_key(key);
            let idx = self.shard(hash);
            let shard = &self.shards[idx as usize];
            shard.insert(key, hash, value, charge).map(|ptr| {
                if ptr.is_null() {
                    None
                } else {
                    Some(CacheEntry {
                        handle: ptr,
                        cache: self.clone(),
                    })
                }
            })
        }

        fn lookup(self: &Arc<Self>, key: u64) -> Option<CacheEntry<T, Self>> {
            let hash = Self::hash_key(key);
            let idx = self.shard(hash);
            let shard = &self.shards[idx as usize];
            let ptr = shard.lookup(key, hash);
            if ptr.is_null() {
                None
            } else {
                Some(CacheEntry {
                    handle: ptr,
                    cache: self.clone(),
                })
            }
        }

        fn release(&self, h: *mut Handle<T>) -> bool {
            let hash = unsafe { (*h).hash };
            let idx = self.shard(hash);
            let shard = &self.shards[idx as usize];
            shard.release(h)
        }

        fn erase(&self, key: u64) {
            let hash = Self::hash_key(key);
            let idx = self.shard(hash);
            let shard = &self.shards[idx as usize];
            shard.erase(key, hash)
        }
    }

    impl<T: Clone> ClockCache<T> {
        #[inline]
        fn shard(&self, hash: u32) -> u32 {
            self.shard_mask & hash
        }

        #[inline]
        fn hash_key(key: u64) -> u32 {
            // Fibonacci hash https://probablydance.com/2018/06/16/fibonacci-hashing-the-optimization-that-the-world-forgot-or-a-better-alternative-to-integer-modulo/
            const FIBONNACI_MAGIC_NUMBER_64BIT: u64 = 11400714819323198485;
            (key.wrapping_mul(FIBONNACI_MAGIC_NUMBER_64BIT) >> 32) as u32 // TODO: cmp with hash for seprated file_id, offset and other algorithm(siphash, fnv, xxhash?).
        }
    }
}
#[cfg(test)]
mod tests {
    use ::std::thread;

    use super::*;

    #[test]
    fn test_base_cache_op() {
        use super::clock::*;
        let c = Arc::new(ClockCache::new(258, 1, -1, false));

        let t1 = {
            let c = c.clone();
            thread::spawn(move || {
                for i in 1..=3 {
                    let v = c.insert(i, Some(vec![i]), 1).unwrap().unwrap();
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

        let v = c.insert(4, Some(vec![4]), 1).unwrap().unwrap();
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
