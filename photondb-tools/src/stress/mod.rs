//! A tool used to perform stress testing.

use std::{
    cmp::Reverse,
    collections::{BinaryHeap, HashMap},
    path::PathBuf,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicU8, Ordering},
        Arc, Mutex,
    },
    task::{Context, Poll, Waker},
    time::{Duration, Instant},
};

use clap::Parser;
use futures::Future;
use log::{debug, error, info, trace};
use photondb::{
    env::{self, Env},
    Table, TableOptions,
};
use rand::{
    rngs::{OsRng, SmallRng},
    Rng, RngCore, SeedableRng,
};

use crate::Result;

const PREFIX_SIZE: usize = 3;

#[derive(Parser, Debug, Clone)]
#[clap(about = "Start stress testing")]
pub(crate) struct Args {
    /// Sets the path of db to test
    #[clap(long, required = true)]
    db: PathBuf,

    /// Sets the key size
    #[clap(long, default_value_t = 10)]
    key_size: usize,

    /// Sets the value size
    #[clap(long, default_value_t = 100)]
    value_size: usize,

    /// Sets the random seed
    #[clap(long)]
    seed: Option<u64>,

    /// How long we are running for, in seconds
    #[clap(long, default_value_t = 600)]
    runtime_seconds: u64,

    /// Destory the existsing DB before running the test
    #[clap(long, default_value_t = true, action = clap::ArgAction::Set)]
    destory_db: bool,

    /// How offten are we going to mutate the prefix
    #[clap(long, default_value_t = 1.0)]
    prefix_mutate_period_seconds: f64,

    /// How likely are we to mutate the first char every period
    #[clap(long, default_value_t = 0.1)]
    first_char_mutate_probability: f64,

    /// How likely are we to mutate the second char every period
    #[clap(long, default_value_t = 0.2)]
    second_char_mutate_probability: f64,

    /// How likely are we to mutate the third char every period
    #[clap(long, default_value_t = 0.5)]
    third_char_mutate_probability: f64,

    /// The space watermark which the DB needed to reclaim
    #[clap(long, default_value_t = 10 << 30)]
    space_used_high: u64,
}

struct Job
where
    Self: Send + Sync,
{
    stop: AtomicBool,
    args: Args,
    table: Table,
    timer: Timer,

    key_prefix: [AtomicU8; PREFIX_SIZE],
}

#[derive(Clone)]
struct Timer {
    baseline: Instant,
    inner: Arc<Mutex<TimerCore>>,
}

struct TimerCore {
    close: bool,
    next_id: u64,
    heap: BinaryHeap<Reverse<TimerEvent>>,
    waiters: HashMap<u64, Option<Waker>>,
}

#[derive(PartialEq, Eq, PartialOrd, Ord)]
struct TimerEvent {
    deadline: u64,
    timer_id: u64,
}

struct Sleep {
    timer: Timer,
    timer_id: u64,
}

impl Timer {
    fn close(&self) {
        let mut timer = self.inner.lock().expect("Poisoned");
        timer.close = true;
        for waker in std::mem::take(&mut timer.waiters).into_values().flatten() {
            waker.wake();
        }
    }

    fn sleep(&self, duration: Duration) -> Sleep {
        let deadline = self.timestamp() + duration.as_millis() as u64;
        let timer_id = {
            let mut timer = self.inner.lock().expect("Poisoned");
            let timer_id = timer.next_id;
            timer.next_id += 1;
            timer.waiters.insert(timer_id, None);
            timer.heap.push(Reverse(TimerEvent { deadline, timer_id }));
            timer_id
        };
        Sleep {
            timer: self.clone(),
            timer_id,
        }
    }

    fn poll_elapsed(&self, timer_id: u64, cx: &mut Context<'_>) -> Poll<()> {
        let mut timer = self.inner.lock().expect("Poisoned");
        if timer.close {
            return Poll::Ready(());
        }
        match timer.waiters.get_mut(&timer_id) {
            Some(waker) => {
                *waker = Some(cx.waker().clone());
                Poll::Pending
            }
            None => Poll::Ready(()),
        }
    }

    fn next_round(&self) {
        let mut wakers = vec![];

        {
            let mut timer = self.inner.lock().expect("Poisoned");
            let now = self.timestamp();
            while let Some(Reverse(TimerEvent {
                timer_id,
                deadline: timeout_ms,
            })) = timer.heap.peek()
            {
                if now < *timeout_ms {
                    break;
                }

                let timer_id = *timer_id;
                if let Some(waker) = timer.waiters.remove(&timer_id).flatten() {
                    wakers.push(waker);
                }
                timer.heap.pop();
            }
        }

        for waker in wakers {
            waker.wake();
        }
    }

    /// The timestamp epoch since `ChannelTimer::baseline`.
    fn timestamp(&self) -> u64 {
        Instant::now()
            .saturating_duration_since(self.baseline)
            .as_millis() as u64
    }
}

impl Default for Timer {
    fn default() -> Self {
        Timer {
            baseline: Instant::now(),
            inner: Arc::new(Mutex::new(TimerCore {
                next_id: 0,
                close: false,
                heap: BinaryHeap::default(),
                waiters: HashMap::default(),
            })),
        }
    }
}

impl Future for Sleep {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let me = self.get_mut();
        me.timer.poll_elapsed(me.timer_id, cx)
    }
}

pub(crate) async fn run(args: Args) -> Result<()> {
    if args.key_size <= PREFIX_SIZE {
        error!(
            "Key size must large than {PREFIX_SIZE}, but get {}",
            args.key_size
        );
        std::process::exit(-1);
    }

    if args.destory_db && args.db.exists() {
        if let Err(err) = std::fs::remove_dir_all(&args.db) {
            error!("Destory DB {}: {err:?}", args.db.display());
            std::process::abort();
        }
    }

    let env = env::Photon;
    let mut options = TableOptions::default();
    options.page_store.space_used_high = args.space_used_high;
    let table = env
        .spawn_background(Table::open(args.db.clone(), options))
        .await?;
    debug!("Open DB {} success", args.db.display());

    let base_seed = args.seed.unwrap_or_else(|| OsRng.next_u64());
    info!("Spawn tasks with base seed {base_seed}");

    let key_prefix = [
        AtomicU8::new(b'a'),
        AtomicU8::new(b'a'),
        AtomicU8::new(b'a'),
    ];
    let job = Arc::new(Job {
        stop: AtomicBool::new(false),
        args: args.clone(),
        table,
        key_prefix,
        timer: Timer::default(),
    });
    let handles = vec![
        spawn_write_task(&env, job.clone(), base_seed),
        spawn_mutate_task(&env, job.clone(), base_seed.wrapping_add(1)),
        spawn_read_task(&env, job.clone(), base_seed.wrapping_add(1)),
    ];

    let mut elapsed = 0;
    while args.runtime_seconds == 0 || elapsed <= args.runtime_seconds * 1000 {
        std::thread::sleep(Duration::from_millis(1));
        job.timer.next_round();
        elapsed += 1;
    }

    info!("Now past {} seconds, exit ...", args.runtime_seconds);
    job.timer.close();
    job.stop.store(true, Ordering::SeqCst);
    env.spawn_background(async move {
        for handle in handles {
            handle.await;
        }
    })
    .await;

    Ok(())
}

#[inline]
fn spawn_write_task<E: Env>(env: &E, job: Arc<Job>, seed: u64) -> E::JoinHandle<()> {
    env.spawn_background(write_task(job, seed))
}

#[inline]
fn spawn_mutate_task<E: Env>(env: &E, job: Arc<Job>, seed: u64) -> E::JoinHandle<()> {
    env.spawn_background(mutate_task(job, seed))
}

#[inline]
fn spawn_read_task<E: Env>(env: &E, job: Arc<Job>, seed: u64) -> E::JoinHandle<()> {
    env.spawn_background(read_task(job, seed))
}

async fn write_task(job: Arc<Job>, seed: u64) {
    let mut rng = SmallRng::seed_from_u64(seed);
    while !job.stop.load(Ordering::Relaxed) {
        let mut key = vec![0u8; job.args.key_size];
        let mut value = vec![0u8; job.args.value_size];
        assert!(key.len() > PREFIX_SIZE);
        key[0] = job.key_prefix[0].load(Ordering::Relaxed);
        key[1] = job.key_prefix[1].load(Ordering::Relaxed);
        key[2] = job.key_prefix[2].load(Ordering::Relaxed);
        fill_bytes(&mut rng, &mut key[3..]);
        fill_bytes(&mut rng, value.as_mut_slice());
        if let Err(err) = job.table.put(&key, 0, &value).await {
            error!("Write to DB: {err:?}");
            std::process::abort();
        }
        photonio::task::yield_now().await;
    }
}

async fn mutate_task(job: Arc<Job>, seed: u64) {
    let duration = Duration::from_millis((job.args.prefix_mutate_period_seconds * 1000.0) as u64);
    let mut rng = SmallRng::seed_from_u64(seed);
    while !job.stop.load(Ordering::Relaxed) {
        job.timer.sleep(duration).await;
        if rng.gen::<f64>() < job.args.first_char_mutate_probability {
            job.key_prefix[0].store(rng.gen_range(b'a'..=b'z'), Ordering::Relaxed);
        }
        if rng.gen::<f64>() < job.args.second_char_mutate_probability {
            job.key_prefix[1].store(rng.gen_range(b'a'..=b'z'), Ordering::Relaxed);
        }
        if rng.gen::<f64>() < job.args.third_char_mutate_probability {
            job.key_prefix[2].store(rng.gen_range(b'a'..=b'z'), Ordering::Relaxed);
        }
        trace!(
            "Switch prefix to '{}{}{}'",
            job.key_prefix[0].load(Ordering::Relaxed) as char,
            job.key_prefix[1].load(Ordering::Relaxed) as char,
            job.key_prefix[2].load(Ordering::Relaxed) as char
        );
        photonio::task::yield_now().await;
    }
}

async fn read_task(job: Arc<Job>, seed: u64) {
    let mut rng = SmallRng::seed_from_u64(seed);
    while !job.stop.load(Ordering::Relaxed) {
        let mut key = vec![0u8; job.args.key_size];
        assert!(key.len() > PREFIX_SIZE);
        key[0] = job.key_prefix[0].load(Ordering::Relaxed);
        key[1] = job.key_prefix[1].load(Ordering::Relaxed);
        key[2] = job.key_prefix[2].load(Ordering::Relaxed);
        fill_bytes(&mut rng, &mut key[3..]);
        let guard = job.table.pin();
        match guard.get(&key, u64::MAX).await {
            Ok(Some(_)) => {
                continue;
            }
            Ok(None) => {
                let mut value = vec![0u8; job.args.value_size];
                fill_bytes(&mut rng, value.as_mut_slice());
                if let Err(err) = job.table.put(&key, 0, &value).await {
                    error!("Write to DB: {err:?}");
                    std::process::abort();
                }
            }
            Err(err) => {
                error!("Read DB: {err:?}");
                std::process::abort();
            }
        }

        photonio::task::yield_now().await;
    }
}

fn fill_bytes(rng: &mut SmallRng, buf: &mut [u8]) {
    const BYTES: &[u8; 62] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    rng.fill(buf);
    buf.iter_mut().for_each(|v| *v = BYTES[(*v % 62) as usize]);
}
