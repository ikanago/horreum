use std::collections::BTreeMap;
use tokio::sync::RwLock;

/// MemTable is an in-memory key-value store.  
/// Imbound data is accumulated in `BTreeMap` this struct holds.
pub struct MemTable {
    // Because this struct is planned to use in asynchronous process,
    // a map of key and value is wrapped in `RwLock`.
    inner: RwLock<BTreeMap<Vec<u8>, Vec<u8>>>,
}

impl MemTable {
    /// Create a new instance.
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(BTreeMap::new()),
        }
    }

    /// Create a new key-value entry.
    pub async fn put(&mut self, key: Vec<u8>, value: Vec<u8>) {
        let mut map = self.inner.write().await;
        map.insert(key, value);
    }

    /// Get value corresponding to a given key.
    pub async fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let map = self.inner.read().await;
        map.get(key).cloned()
    }

    /// Delete value corresponding to a given key.
    pub async fn delete(&mut self, key: &[u8]) {
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
        table
            .put("abc".as_bytes().to_vec(), "def".as_bytes().to_vec())
            .await;
        table
            .put("xyz".as_bytes().to_vec(), "xxx".as_bytes().to_vec())
            .await;
        assert_eq!(
            Some("def".as_bytes().to_vec()),
            table.get("abc".as_bytes()).await
        );
        assert_eq!(
            Some("xxx".as_bytes().to_vec()),
            table.get("xyz".as_bytes()).await
        );
    }

    #[tokio::test]
    async fn delete() {
        let mut table = MemTable::new();
        table
            .put("abc".as_bytes().to_vec(), "def".as_bytes().to_vec())
            .await;
        table
            .put("xyz".as_bytes().to_vec(), "xxx".as_bytes().to_vec())
            .await;
        table.delete("abc".as_bytes()).await;
        assert_eq!(None, table.get("abc".as_bytes()).await);
        assert_eq!(
            Some("xxx".as_bytes().to_vec()),
            table.get("xyz".as_bytes()).await
        );
    }
}
