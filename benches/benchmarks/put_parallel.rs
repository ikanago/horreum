use crate::setup::{COUNT, PAIRS};
use criterion::{criterion_group, BenchmarkId, Criterion};
use horreum::Horreum;
use lazy_static::lazy_static;

lazy_static! {
    static ref DB: Horreum = Horreum::new();
}

fn put_pairs(thread_num: usize) {
    crossbeam::scope(|s| {
        for pairs in PAIRS.chunks(COUNT / thread_num) {
            s.spawn(move |_| {
                for (key, value) in pairs {
                    DB.put(key.clone(), value.clone());
                }
            });
        }
    })
    .unwrap();
}

pub fn bench_put(c: &mut Criterion) {
    let mut group = c.benchmark_group("Put key value pairs in parallel");
    let n = 10;
    group.bench_with_input(BenchmarkId::new("put_pairs()", n), &n, |b, &n| {
        b.iter(|| put_pairs(n));
    });
    group.finish();
}

criterion_group!(benches, bench_put);
