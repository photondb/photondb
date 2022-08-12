use std::{collections::BTreeMap, time::Duration};

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use jemallocator::Jemalloc;
use rand::{
    distributions::{Distribution, Uniform},
    rngs::ThreadRng,
    thread_rng,
};

const NUM_KEYS: u64 = 10_000_000;

#[global_allocator]
static ALLOC: Jemalloc = Jemalloc;

type Map = BTreeMap<Vec<u8>, Vec<u8>>;

fn get(map: &mut Map, k: u64) {
    let key = k.to_be_bytes();
    map.get(key.as_slice()).unwrap();
}

fn put(map: &mut Map, k: u64) {
    let key = k.to_be_bytes();
    map.insert(key.to_vec(), key.to_vec());
}

struct Bench {
    map: Map,
    rng: ThreadRng,
    dist: Uniform<u64>,
}

impl Bench {
    fn new() -> Self {
        Self {
            map: Map::new(),
            rng: thread_rng(),
            dist: Uniform::from(0..NUM_KEYS),
        }
    }

    fn setup(&mut self) {
        for k in 0..NUM_KEYS {
            put(&mut self.map, k);
        }
    }

    fn bench<F>(&mut self, func: F)
    where
        F: Fn(&mut Map, u64),
    {
        let k = self.dist.sample(&mut self.rng);
        func(&mut self.map, k);
    }
}

fn bench(c: &mut Criterion) {
    let mut bench = Bench::new();
    bench.setup();
    c.bench_function("get", |b| b.iter(|| bench.bench(black_box(get))));
    c.bench_function("put", |b| b.iter(|| bench.bench(black_box(put))));
}

criterion_group!(
    name = benches;
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(3));
    targets = bench
);
criterion_main!(benches);
