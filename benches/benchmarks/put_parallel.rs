use lazy_static::lazy_static;
use crate::setup::PAIRS;
use std::thread;
use horreum::Horreum;
use criterion::{criterion_group, Criterion};

lazy_static! {
    static ref DB: Horreum = Horreum::new();
}

fn put_pairs() {
    let mut handles = Vec::new();
    for (key, value) in PAIRS.iter() {
        handles.push(thread::spawn(move || {
            DB.put(key.clone(), value.clone());
        }));
    }
    for handle in handles {
        handle.join().unwrap();
    }
}

pub fn bench_put(c: &mut Criterion) {
    let mut group = c.benchmark_group("Put key value pairs");
    group.bench_function("Put pairs", |b| {
        b.iter(|| put_pairs());
    });
    group.finish();
}

criterion_group!(benches, bench_put);
