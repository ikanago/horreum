use std::collections::BTreeMap;

use bytes::Bytes;
use tokio::sync::RwLock;

/// MemTable is an in-memory key-value store.  
/// Imbound data is accumulated in `BTreeMap` this struct holds.
pub struct MemTable {
    // Because this struct is planned to use in asynchronous process,
    // a map of key and value is wrapped in `RwLock`.
    inner: RwLock<BTreeMap<Bytes, Bytes>>,
}

impl MemTable {
    /// Create a new instance.
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(BTreeMap::new()),
        }
    }

    /// Create a new key-value entry.
    pub async fn put(&mut self, key: Bytes, value: Bytes) {
        let mut map = self.inner.write().await;
        map.insert(key, value);
    }

    /// Get value corresponding to a given key.
    pub async fn get(&self, key: &Bytes) -> Option<Bytes> {
        let map = self.inner.read().await;
        map.get(key).cloned()
    }

    /// Delete value corresponding to a given key.
    pub async fn delete(&mut self, key: &Bytes) {
        let mut map = self.inner.write().await;
        map.remove(key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn put_and_get() {
        let mut table = MemTable::new();
        table.put(Bytes::from("abc"), Bytes::from("def")).await;
        table.put(Bytes::from("xyz"), Bytes::from("xxx")).await;
        assert_eq!(
            Some(Bytes::from("def")),
            table.get(&Bytes::from("abc")).await
        );
        assert_eq!(
            Some(Bytes::from("xxx")),
            table.get(&Bytes::from("xyz")).await
        );
    }

    #[tokio::test]
    async fn delete() {
        let mut table = MemTable::new();
        table.put(Bytes::from("abc"), Bytes::from("def")).await;
        table.put(Bytes::from("xyz"), Bytes::from("xxx")).await;
        table.delete(&Bytes::from("abc")).await;
        assert_eq!(None, table.get(&Bytes::from("abc")).await);
        assert_eq!(
            Some(Bytes::from("xxx")),
            table.get(&Bytes::from("xyz")).await
        );
    }
}
