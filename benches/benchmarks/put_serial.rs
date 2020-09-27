use crate::setup::PAIRS;
use criterion::{criterion_group, Criterion};
use horreum::Horreum;

fn put_pairs(db: &Horreum) {
    for (key, value) in PAIRS.iter() {
        db.put(key.clone(), value.clone());
    }
}

pub fn bench_put(c: &mut Criterion) {
    let mut group = c.benchmark_group("Put key value pairs in serial");
    group.bench_function("Put pairs", |b| {
        let db = Horreum::new();
        b.iter(|| put_pairs(&db));
    });
    group.finish();
}

criterion_group!(benches, bench_put);
