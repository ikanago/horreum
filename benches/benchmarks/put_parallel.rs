use crate::setup::{launch_db, COUNT, PAIRS};
use criterion::{criterion_group, BenchmarkId, Criterion};
use rayon::prelude::*;
use reqwest::Client;
use std::process::{Command, Stdio};

fn put_pairs(client: &Client, port: usize) {
    PAIRS.par_iter().for_each(|(key, value)| {
        let url = format!("http://localhost:{}?key={}&value={}", port, key, value);
        client.post(&url).send();
    });
}

pub fn bench_put(c: &mut Criterion) {
    let mut group = c.benchmark_group("Put key value pairs in parallel");
    let db_with_one_thread = launch_db(1, 8081);
    let db_with_more_threads = launch_db(8, 8082);
    let client = Client::new();

    group.bench_function("put_pairs_to_one_thread()", |b| {
        b.iter(|| put_pairs(&client, 8081));
    });
    group.bench_function("put_pairs_to_more_threads()", |b| {
        b.iter(|| put_pairs(&client, 8082));
    });
    group.finish();
}

criterion_group!(benches, bench_put);
