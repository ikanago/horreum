use std::sync::{Arc, Mutex};
use std::collections::BTreeMap;

/// Database itselff holding index of data.
///
/// This struct is accessed and mutated its inner data from multiple threads.
/// Data inside of this database is not persisted to non-valatile memory now.
pub struct Horreum {
    index: Arc<Mutex<BTreeMap<Vec<u8>, Vec<u8>>>>,
}

impl Horreum {
    /// Constructs a new `Horreum`.
    pub fn new() -> Self {
        Self {
            index: Arc::new(Mutex::new(BTreeMap::new()))
        }
    }

    /// Acquire a lock for index and get value corresponding the key.
    pub fn get(&self, key: &Vec<u8>) -> Option<Vec<u8>> {
        let index = self.index.clone();
        let map = index.lock().unwrap();
        map.get(key).map(|value| value.clone())
    }

    /// Acquire a lock for index and insert a given key-value pair
    pub fn put(&self, key: Vec<u8>, value: Vec<u8>) {
        let index = self.index.clone();
        let mut map = index.lock().unwrap();
        map.insert(key, value);
    }
}
