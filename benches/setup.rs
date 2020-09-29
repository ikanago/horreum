use lazy_static::lazy_static;
use rand::distributions::Alphanumeric;
use rand::Rng;
use std::process::{Child, Command, Stdio};

pub static COUNT: usize = 10;

lazy_static! {
    pub static ref PAIRS: Vec<(String, String)> = (0..COUNT)
        .map(|_| (random_string(10), random_string(10)))
        .collect();
}

pub fn launch_db(n: usize, port: usize) -> Child {
    let mut command = Command::new("./target/debug/main");
    command.arg(format!("-n {} -p {}", n, port));
    command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("Failed to launch database")
}

fn random_string(length: usize) -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(length)
        .collect()
}
