// use crate::setup::{COUNT, PAIRS, launch_db};
// use criterion::{criterion_group, BenchmarkId, Criterion};
// use rayon::prelude::*;
// use reqwest::Client;
// use std::process::{Command, Stdio};

// fn put_pairs(client: &Client) {
//     PAIRS.par_iter().for_each(|(key, value)| {
//         let url = format!("http://localhost:8080?key={}&value={}", key, value);
//         client.post(&url).send();
//     });
// }

// pub fn bench_put(c: &mut Criterion) {
//     let mut group = c.benchmark_group("Put key value pairs in serial");
//     let process = launch_db(n)
//     let client = Client::new();

//     group.bench_function("put_pairs()", |b| {
//         b.iter(|| put_pairs(&client));
//     });
//     group.finish();
// }

// criterion_group!(benches, bench_put);
