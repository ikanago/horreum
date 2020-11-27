use std::collections::BTreeMap;
use tokio::sync::RwLock;

/// `MemTable` is an in-memory key-value store.  
/// Imbound data is accumulated in `BTreeMap` this struct holds.
/// `MemTable` records deletion histories because `SSTable` needs them.
pub struct MemTable {
    // Because this struct is planned to use in asynchronous process,
    // a map of key and value is wrapped in `RwLock`.
    inner: RwLock<BTreeMap<Vec<u8>, Entry>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Entry {
    Value(Vec<u8>),
    Deleted,
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
        map.insert(key, Entry::Value(value));
    }

    /// Get value corresponding to a given key.
    /// If `MemTable` has a value for the key, return `Some(Value())`.
    /// If `MemTable` has an entry for the key but it has deleted, return `Some(Deleted)`.
    /// If `MemTable` has no entry for the key, return `None`.
    pub async fn get(&self, key: &[u8]) -> Option<Entry> {
        let map = self.inner.read().await;
        map.get(key).cloned()
    }

    /// Delete value corresponding to a given key.
    pub async fn delete(&mut self, key: &[u8]) {
        let mut map = self.inner.write().await;
        map.insert(key.to_vec(), Entry::Deleted);
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
            Some(Entry::Value("def".as_bytes().to_vec())),
            table.get("abc".as_bytes()).await
        );
        assert_eq!(
            Some(Entry::Value("xxx".as_bytes().to_vec())),
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
        assert_eq!(Some(Entry::Deleted), table.get("abc".as_bytes()).await);
        assert_eq!(None, table.get("111".as_bytes()).await);
        assert_eq!(
            Some(Entry::Value("xxx".as_bytes().to_vec())),
            table.get("xyz".as_bytes()).await
        );
    }
}
