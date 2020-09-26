use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

/// Database itselff holding index of data.
///
/// This struct is accessed and mutated its inner data from multiple threads.
/// Data inside of this database is not persisted to non-valatile memory now.
#[derive(Debug)]
pub struct Horreum {
    index: Arc<Mutex<BTreeMap<String, String>>>,
}

impl Horreum {
    /// Constructs a new `Horreum`.
    pub fn new() -> Self {
        Self {
            index: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    /// Acquire a lock for index and get value corresponding the key.
    pub fn get(&self, key: &str) -> Option<String> {
        let index = self.index.clone();
        let map = index.lock().unwrap();
        map.get(key).cloned()
    }

    /// Acquire a lock for index and insert a given key-value pair
    pub fn put(&self, key: String, value: String) {
        let index = self.index.clone();
        let mut map = index.lock().unwrap();
        map.insert(key, value);
    }

    /// Acquire a lock for index and insert a given key-value pair
    pub fn delete(&self, key: &str) -> Option<String> {
        let index = self.index.clone();
        let mut map = index.lock().unwrap();
        map.remove(key)
    }
}
