use std::collections::BTreeMap;
use tokio::sync::RwLock;

use crate::command::Command;
use crate::format::InternalPair;

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

    pub async fn apply<'a>(&self, command: Command) -> Option<Entry> {
        match command {
            Command::Get { key } => self.get(&key).await,
            Command::Put { key, value } => self.put(key, value).await,
            Command::Delete { key } => self.delete(&key).await,
        }
    }

    /// Get value corresponding to a given key.
    /// If `MemTable` has a value for the key, return `Some(Value())`.
    /// If `MemTable` has an entry for the key but it has deleted, return `Some(Deleted)`.
    /// If `MemTable` has no entry for the key, return `None`.
    pub async fn get(&self, key: &[u8]) -> Option<Entry> {
        let map = self.inner.read().await;
        map.get(key).cloned()
    }

    /// Create a new key-value entry.
    pub async fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Option<Entry> {
        let mut map = self.inner.write().await;
        map.insert(key, Entry::Value(value))
    }

    /// Mark value corresponding to a key as deleted.
    /// Return `true` if there was an entry to delete.
    pub async fn delete(&self, key: &[u8]) -> Option<Entry> {
        let mut map = self.inner.write().await;
        // Check entry for the key to avoid mark a key which is not registered as `Deleted`.
        if map.get(key).is_some() {
            map.insert(key.to_vec(), Entry::Deleted)
        } else {
            None
        }
    }

    pub async fn flush(&self) -> Vec<InternalPair> {
        let map = self.inner.read().await;
        map.iter()
            .map(|(key, entry)| match entry {
                Entry::Value(value) => InternalPair::new(key, Some(value)),
                Entry::Deleted => InternalPair::new(key, None),
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn put_and_get() {
        let table = MemTable::new();
        assert_eq!(None, table.put(b"abc".to_vec(), b"def".to_vec()).await);
        assert_eq!(None, table.put(b"xyz".to_vec(), b"xxx".to_vec()).await);
        assert_eq!(
            Some(Entry::Value(b"xxx".to_vec())),
            table.put(b"xyz".to_vec(), b"qwerty".to_vec()).await
        );
        assert_eq!(Some(Entry::Value(b"def".to_vec())), table.get(b"abc").await);
        assert_eq!(
            Some(Entry::Value(b"qwerty".to_vec())),
            table.get(b"xyz").await
        );
    }

    #[tokio::test]
    async fn delete() {
        let table = MemTable::new();
        table.put(b"abc".to_vec(), b"def".to_vec()).await;
        table.put(b"xyz".to_vec(), b"xxx".to_vec()).await;
        assert_eq!(
            Some(Entry::Value(b"def".to_vec())),
            table.delete(b"abc").await
        );
        assert_eq!(None, table.delete(b"abcdef").await);
        assert_eq!(Some(Entry::Deleted), table.get(b"abc").await);
        assert_eq!(None, table.get(b"111").await);
        assert_eq!(Some(Entry::Value(b"xxx".to_vec())), table.get(b"xyz").await);
    }

    #[tokio::test]
    async fn delete_non_existing() {
        let table = MemTable::new();
        assert_eq!(None, table.delete(b"abc").await);
        assert_eq!(None, table.get(b"abc").await);
    }

    #[tokio::test]
    async fn flush() {
        let table = MemTable::new();
        table.put(b"abc".to_vec(), b"def".to_vec()).await;
        table.put(b"rust".to_vec(), b"nice".to_vec()).await;
        table.put(b"cat".to_vec(), b"hoge".to_vec()).await;
        table.put(b"xyz".to_vec(), b"xxx".to_vec()).await;
        table.delete(b"cat").await;
        assert_eq!(
            vec![
                InternalPair::new(b"abc", Some(b"def")),
                InternalPair::new(b"cat", None),
                InternalPair::new(b"rust", Some(b"nice")),
                InternalPair::new(b"xyz", Some(b"xxx")),
            ],
            table.flush().await
        );
    }
}
