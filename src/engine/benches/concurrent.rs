use std::time::{Duration, Instant};

use criterion::{criterion_group, criterion_main, Criterion};
use photondb_engine::tree::*;

const T: usize = 4;
const N: usize = 10_000_000;
const M: usize = 1000_000;
const STEP: usize = N / M;

fn get(map: &Map, i: usize) {
    let buf = i.to_be_bytes();
    let key = buf.as_slice();
    map.get(key, 0, |_| {}).unwrap();
}

fn put(map: &Map, i: usize) {
    let buf = i.to_be_bytes();
    let key = buf.as_slice();
    map.put(key, 0, key).unwrap();
}

fn concurrent_get(map: &Map) {
    let mut handles = Vec::new();
    for _ in 0..T {
        let map = map.clone();
        let handle = std::thread::spawn(move || {
            for i in (0..N).step_by(STEP) {
                get(&map, i);
            }
        });
        handles.push(handle);
    }
    for handle in handles {
        handle.join().unwrap();
    }
}

fn concurrent_put(map: &Map) {
    let mut handles = Vec::new();
    for _ in 0..T {
        let map = map.clone();
        let handle = std::thread::spawn(move || {
            for i in (0..N).step_by(STEP) {
                put(&map, i);
                get(&map, i);
            }
        });
        handles.push(handle);
    }
    for handle in handles {
        handle.join().unwrap();
    }
}

fn bench_get(map: &Map, iters: u64) -> Duration {
    let start = Instant::now();
    for _ in 0..iters {
        concurrent_get(map);
    }
    start.elapsed()
}

fn bench_put(map: &Map, iters: u64) -> Duration {
    let start = Instant::now();
    for _ in 0..iters {
        concurrent_put(map);
    }
    start.elapsed()
}

fn bench(c: &mut Criterion) {
    let opts = Options::default();
    let map = Map::open(opts).unwrap();
    for i in 0..N {
        put(&map, i);
    }

    c.bench_function("get", |b| b.iter_custom(|iters| bench_get(&map, iters)));
    c.bench_function("put", |b| b.iter_custom(|iters| bench_put(&map, iters)));
    println!("{:?}", map.stats());
}

criterion_group!(benches, bench);
criterion_main!(benches);
