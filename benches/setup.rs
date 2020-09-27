use lazy_static::lazy_static;
use rand::distributions::Alphanumeric;
use rand::Rng;

static COUNT: usize = 1000;

lazy_static! {
    pub static ref PAIRS: Vec<(String, String)> = (0..COUNT)
        .map(|_| (random_string(), random_string()))
        .collect();
}

fn random_string() -> String {
    let length = 100;
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(length)
        .collect()
}
