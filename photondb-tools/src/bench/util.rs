use std::{
    cell::Ref,
    collections::{hash_map, HashMap},
    io::Write,
    sync::{Arc, Mutex},
    task::{Poll, Waker},
    time::{self, Duration, Instant},
};

use chrono::Utc;
use futures::Future;
use hdrhistogram::Histogram;
use photondb::StoreStats;
use rand::{distributions::Uniform, prelude::Distribution, rngs::SmallRng, RngCore, SeedableRng};

use super::{store::Store, Args, BenchmarkType, ValueSizeDistributionType};

pub(super) enum GenMode {
    Random,
    Sequence,
}

pub(super) struct KeyGenerator {
    key_per_prefix: u64,
    prefix_size: u64,
    key_size: u64,
    key_nums: u64,
    state: Option<KeyGeneratorState>,
}

pub enum KeyGeneratorState {
    Random { rng: SmallRng },
    Sequence { last: u64 },
}

impl KeyGenerator {
    pub(super) fn new(mode: GenMode, key_size: u64, key_nums: u64, seed: u64) -> Self {
        let state = Some(match mode {
            GenMode::Random => KeyGeneratorState::Random {
                rng: SmallRng::seed_from_u64(seed),
            },
            GenMode::Sequence => KeyGeneratorState::Sequence { last: 0 },
        });
        let key_per_prefix = 0;
        let prefix_size = 0;
        Self {
            state,
            key_per_prefix,
            prefix_size,
            key_size,
            key_nums,
        }
    }

    pub(super) fn generate_key(&mut self, buf: &mut [u8]) {
        let rand_num = match self.state.as_mut().unwrap() {
            KeyGeneratorState::Random { rng } => rng.next_u64() % self.key_nums,
            KeyGeneratorState::Sequence { last } => {
                *last = last.saturating_add(1);
                *last
            }
        };
        self.generate_from_num(rand_num, buf)
    }

    fn generate_from_num(&self, val: u64, buf: &mut [u8]) {
        let mut pos = 0;
        if self.key_per_prefix > 0 {
            let prefix_cnt = self.key_nums / self.key_per_prefix;
            let prefix = val % prefix_cnt;
            let fill_size = self.prefix_size.min(8) as usize;
            buf[pos as usize..fill_size].copy_from_slice(&prefix.to_le_bytes()[..fill_size]);
            if self.prefix_size > 8 {
                buf[(pos + 8) as usize..(self.prefix_size - 8) as usize].fill(0);
            }
            pos += self.prefix_size;
        }
        let fill_size = (self.key_size - pos).min(8);
        let vals = val.to_be_bytes();
        buf[pos as usize..(pos + fill_size) as usize].copy_from_slice(&vals[..fill_size as usize]);
        pos += fill_size;
        if pos < self.key_size {
            buf[pos as usize..self.key_size as usize].fill(0)
        }
    }
}

pub(super) struct ValueGenerator {
    distribution_type: ValueSizeDistributionType,
    value_size: u64,

    data: Vec<u8>,
    pos: usize,
}

impl ValueGenerator {
    pub(super) fn new(distribution_type: ValueSizeDistributionType, value_size: u64) -> Self {
        let mut rng = SmallRng::seed_from_u64(301);
        let mut data = Vec::new();
        while data.len() < value_size as usize {
            let d = {
                let size = 100;
                let mut rand_str = Vec::with_capacity(size);
                let range = Uniform::new(0, 95);
                while rand_str.len() < size {
                    rand_str.push(b' ' + (range.sample(&mut rng) as u8));
                    // ' ' to '~'
                }
                rand_str
            };
            data.extend_from_slice(&d);
        }
        Self {
            data,
            pos: 0,
            distribution_type,
            value_size,
        }
    }

    pub(super) fn generate_value(&mut self) -> &[u8] {
        let require_len = match self.distribution_type {
            ValueSizeDistributionType::Fixed => self.value_size as usize,
            ValueSizeDistributionType::Uniform => unimplemented!(),
        };
        if self.pos + require_len > self.data.len() {
            self.pos = 0;
        }
        self.pos += require_len;
        &self.data[self.pos - require_len..self.pos]
    }
}

#[derive(Clone)]
pub(crate) struct Stats<S: Store> {
    tid: u32,

    table: Option<S>,

    config: Arc<Args>,

    start: time::Instant,
    finish: Option<time::Instant>,
    total_sec: u64,

    done_cnt: u64,
    err_cnt: u64,
    bytes: u64,

    last_op_finish: Option<time::Instant>,
    last_report_finish: Option<time::Instant>,
    last_report_done_cnt: u64,
    next_report_cnt: u64,

    last_store_stats: Option<StoreStats>,

    hist: HashMap<OpType, hdrhistogram::Histogram<u64>>,

    msg: String,
}

impl<S: Store> Stats<S> {
    pub(super) fn start(tid: u32, config: Arc<Args>, table: Option<S>) -> Self {
        let next_report_cnt = config.stats_interval;
        Self {
            tid,
            config,
            table,
            start: Instant::now(),
            next_report_cnt,

            total_sec: 0,
            finish: None,
            done_cnt: 0,
            err_cnt: 0,
            bytes: 0,
            last_op_finish: None,
            last_report_finish: None,
            last_report_done_cnt: 0,
            last_store_stats: None,
            hist: HashMap::new(),
            msg: "".to_string(),
        }
    }

    pub(super) fn finish_operation(&mut self, typ: OpType, done: u64, err: u64, bytes: u64) {
        let now = Instant::now();
        let op_elapsed = now.duration_since(*self.last_op_finish.as_ref().unwrap_or(&self.start));
        self.done_cnt += done;
        self.err_cnt += err;
        self.bytes += bytes;
        self.last_op_finish = Some(now);

        if self.config.hist {
            let t = op_elapsed.as_micros() as u64;
            self.hist
                .entry(typ)
                .or_insert_with(|| {
                    hdrhistogram::Histogram::new_with_bounds(1, 60 * 60 * 1000, 2).unwrap()
                })
                .record(t)
                .expect("duration should be in range");
        }

        if self.done_cnt >= self.next_report_cnt {
            if self.config.stats_interval > 0 {
                let report_elapsed =
                    now.duration_since(*self.last_report_finish.as_ref().unwrap_or(&self.start));
                if self.config.stats_interval_sec == 0
                    || report_elapsed.as_secs() >= self.config.stats_interval_sec
                {
                    let start_elapsed = now.duration_since(self.start);
                    println!(
                        "{} ... thread: {}, ({}, {}) ops and ({}, {}) ops/sec in ({}, {})",
                        Utc::now().to_rfc3339(),
                        self.tid,
                        self.last_report_done_cnt,
                        self.done_cnt,
                        (self.done_cnt.saturating_sub(self.last_report_done_cnt) as f64
                            / report_elapsed.as_secs_f64()),
                        (self.done_cnt as f64 / start_elapsed.as_secs_f64()),
                        report_elapsed.as_millis(),
                        start_elapsed.as_millis(),
                    );
                }
                self.last_report_done_cnt = self.done_cnt;
                self.next_report_cnt = self.last_report_done_cnt + self.config.stats_interval;
                self.last_report_finish = Some(now);
            } else {
                self.next_report_cnt += if self.next_report_cnt < 1000 {
                    100
                } else if self.next_report_cnt < 10000 {
                    500
                } else if self.next_report_cnt < 50000 {
                    5000
                } else if self.next_report_cnt < 100000 {
                    10000
                } else if self.next_report_cnt < 500000 {
                    50000
                } else {
                    100000
                };
                print!("... finished {:30} ops\r", self.done_cnt);
            }
            std::io::stdout().flush().unwrap();
        }
    }

    pub(super) fn add_msg(&mut self, msg: &str) {
        if msg.is_empty() {
            return;
        }
        self.msg.push(' ');
        self.msg.push_str(msg);
    }

    pub(super) fn stop(&mut self) {
        let now = Instant::now();
        let elapsed = now.duration_since(self.start);
        self.finish = Some(now);
        self.total_sec = elapsed.as_secs();
    }

    pub(super) fn merge(&mut self, o: Ref<Stats<S>>) {
        self.done_cnt += o.done_cnt;
        self.bytes += o.bytes;
        self.total_sec += o.total_sec;
        if self.start < o.start {
            self.start = o.start
        }
        if self.finish > o.finish {
            self.finish = o.finish
        }
        if !o.msg.is_empty() {
            self.msg = o.msg.to_owned();
        }
        if self.config.hist {
            for (typ, histo) in &o.hist {
                match self.hist.entry(typ.to_owned()) {
                    hash_map::Entry::Occupied(mut ent) => {
                        let h1 = ent.get_mut();
                        h1.add(histo).unwrap();
                    }
                    hash_map::Entry::Vacant(ent) => {
                        ent.insert(histo.to_owned());
                    }
                }
            }
        }
    }

    pub(super) fn report(&self, bench: BenchmarkType) {
        if self.finish.is_none() {
            return;
        }
        let elapsed = self.finish.as_ref().unwrap().duration_since(self.start);
        let bytes_rate = ((self.bytes / 1024 / 1024) as f64) / elapsed.as_secs_f64();
        println!(
            "{:12?} : {:11.3} ms/op {} ops/sec, {} sec, {} ops; {} MiB/s {}",
            bench,
            (self.total_sec * 1000000) as f64 / (self.done_cnt as f64),
            (self.done_cnt as f64) / (elapsed.as_secs_f64()),
            elapsed.as_secs_f64(),
            self.done_cnt,
            bytes_rate,
            self.msg,
        );
        if self.config.hist {
            display_hist(&self.hist);
        }
        if self.config.db_stats {
            self.display_db_stats();
        }
    }

    fn display_db_stats(&self) {
        let Some(table) = &self.table else {
		    return;
	    };
        let Some((tree_stats, store_stats)) = table.stats() else {
		    return;
	    };

        macro_rules! display_txn_stats {
		    ($expression:expr, $name:ident) => {
			    {
				    let s = $expression;
				    println!("TableStats_{}: read: {}, write: {}, split_page: {}, reconcile_page: {}, consolidate_page: {}",
				    stringify!($name), s.read, s.write, s.split_page, s.reconcile_page, s.consolidate_page)
			    }
		    }
	    }

        display_txn_stats!(tree_stats.conflict, conflict);
        display_txn_stats!(tree_stats.success, success);

        let page_cache_stats = if let Some(last_store_stats) = &self.last_store_stats {
            store_stats.page_cache.sub(&last_store_stats.page_cache)
        } else {
            store_stats.page_cache
        };
        println!(
            "CacheStats: lookup_hit: {}, lookup_miss: {}, hit_rate: {}%, insert: {}, evit: {}",
            page_cache_stats.lookup_hit,
            page_cache_stats.lookup_miss,
            (page_cache_stats.lookup_hit as f64) * 100.
                / (page_cache_stats.lookup_hit + page_cache_stats.lookup_miss) as f64,
            page_cache_stats.insert,
            page_cache_stats.active_evit,
        )
    }
}

fn display_hist(hists: &HashMap<OpType, Histogram<u64>>) {
    for (op, hist) in hists {
        println!(
            "Percentiles_{:12?} : P50: {} ms, P75: {} ms, P99: {} ms, P99.9: {} ms, P99.99: {} ms",
            op,
            hist.value_at_quantile(0.50),
            hist.value_at_quantile(0.75),
            hist.value_at_quantile(0.99),
            hist.value_at_quantile(0.999),
            hist.value_at_quantile(0.9999),
        )
    }
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub enum OpType {
    Write,
    Read,
    Update,
}

#[derive(Clone)]
pub struct Barrier {
    core: Arc<Mutex<BarrierCore>>,
}

pub struct BarrierCore {
    wakers: HashMap<usize, Waker>,
    done: bool,
    count: u64,
}

impl Barrier {
    pub(super) fn new(count: u64) -> Self {
        assert!(count > 0);
        let core = Arc::new(Mutex::new(BarrierCore {
            wakers: HashMap::with_capacity(count as usize),
            done: false,
            count,
        }));
        Self { core }
    }
}

impl Future for Barrier {
    type Output = ();

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let this = self.get_mut();
        let mut core = this.core.lock().unwrap();
        if core.done {
            Poll::Ready(())
        } else {
            let id = this as *const Barrier as usize;
            if core.wakers.len() == (core.count - 1) as usize {
                core.done = true;
                for w in core.wakers.values() {
                    w.wake_by_ref();
                }
                Poll::Ready(())
            } else {
                core.wakers.insert(id, cx.waker().clone());
                Poll::Pending
            }
        }
    }
}

pub(super) struct Until {
    want_cnt: u64,
    done_cnt: u64,
    duration: Option<Duration>,
    start: Instant,
}

impl Until {
    pub(super) fn new(cnt: u64, duration_sec: u64) -> Self {
        let duration = if duration_sec > 0 {
            Some(Duration::from_secs(duration_sec))
        } else {
            None
        };
        let start = Instant::now();
        Self {
            want_cnt: cnt,
            done_cnt: 0,
            duration,
            start,
        }
    }
}

impl Iterator for Until {
    type Item = ();

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(duration) = self.duration {
            self.done_cnt += 1;
            if self.done_cnt % 1000 == 0 && self.start.elapsed() >= duration {
                return None;
            }
            Some(())
        } else if self.done_cnt < self.want_cnt {
            self.done_cnt += 1;
            Some(())
        } else {
            None
        }
    }
}
