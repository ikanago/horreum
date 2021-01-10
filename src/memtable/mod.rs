use crate::command::Command;
use crate::format::InternalPair;
use crate::Message;
use std::collections::BTreeMap;
use tokio::sync::{mpsc, RwLock};

/// `MemTable` is an in-memory key-value store.  
/// Imbound data is accumulated in `BTreeMap` this struct holds.
/// `MemTable` records deletion histories because `SSTable` needs them.
pub struct MemTable {
    // Because `MemTable` receives asynchronous request,
    // a map of key and value is wrapped in `RwLock`.
    inner: RwLock<BTreeMap<Vec<u8>, Option<Vec<u8>>>>,

    /// Receiver to receive command.
    command_rx: mpsc::Receiver<Message>,
}

impl MemTable {
    /// Create a new instance.
    pub fn new(command_rx: mpsc::Receiver<Message>) -> Self {
        Self {
            inner: RwLock::new(BTreeMap::new()),
            command_rx,
        }
    }

    /// Listen to requests and send back results.
    pub async fn listen(&mut self) {
        while let Some((command, tx)) = self.command_rx.recv().await {
            let entry = self.apply(command).await;
            if let Err(_) = tx.send(entry).await {
                dbg!("The receiver dropped");
            };
        }
    }

    /// Extract contents of `command` and apply them.
    pub async fn apply<'a>(&self, command: Command) -> Option<Vec<u8>> {
        match command {
            Command::Get { key } => self.get(&key).await,
            Command::Put { key, value } => self.put(key, value).await,
            Command::Delete { key } => self.delete(&key).await,
        }
    }

    /// Get value corresponding to a given key.
    pub async fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let map = self.inner.read().await;
        map.get(key).cloned().flatten()
    }

    /// Create a new key-value entry.
    pub async fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Option<Vec<u8>> {
        let mut map = self.inner.write().await;
        map.insert(key, Some(value)).flatten()
    }

    /// Mark value corresponding to a key as deleted.
    /// Return `true` if there was an entry to delete.
    pub async fn delete(&self, key: &[u8]) -> Option<Vec<u8>> {
        let mut map = self.inner.write().await;
        // Check entry for the key to avoid mark a key which is not registered as `Deleted`.
        map.insert(key.to_vec(), None).flatten()
    }

    pub async fn flush(&self) -> Vec<InternalPair> {
        let map = self.inner.read().await;
        map.iter()
            .map(|(key, entry)| match entry {
                Some(value) => InternalPair::new(key, Some(value)),
                None => InternalPair::new(key, None),
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn put_and_get() {
        let (_, rx) = mpsc::channel(1);
        let table = MemTable::new(rx);
        assert_eq!(None, table.put(b"abc".to_vec(), b"def".to_vec()).await);
        assert_eq!(None, table.put(b"xyz".to_vec(), b"xxx".to_vec()).await);
        assert_eq!(
            Some(b"xxx".to_vec()),
            table.put(b"xyz".to_vec(), b"qwerty".to_vec()).await
        );
        assert_eq!(Some(b"def".to_vec()), table.get(b"abc").await);
        assert_eq!(Some(b"qwerty".to_vec()), table.get(b"xyz").await);
    }

    #[tokio::test]
    async fn delete() {
        let (_, rx) = mpsc::channel(1);
        let table = MemTable::new(rx);
        table.put(b"abc".to_vec(), b"def".to_vec()).await;
        table.put(b"xyz".to_vec(), b"xxx".to_vec()).await;
        assert_eq!(Some(b"def".to_vec()), table.delete(b"abc").await);
        assert_eq!(None, table.delete(b"abcdef").await);
        assert_eq!(None, table.get(b"abc").await);
        assert_eq!(None, table.get(b"111").await);
        assert_eq!(Some(b"xxx".to_vec()), table.get(b"xyz").await);
    }

    #[tokio::test]
    async fn delete_non_existing() {
        let (_, rx) = mpsc::channel(1);
        let table = MemTable::new(rx);
        assert_eq!(None, table.delete(b"abc").await);
        assert_eq!(None, table.get(b"abc").await);
    }

    #[tokio::test]
    async fn flush() {
        let (_, rx) = mpsc::channel(1);
        let table = MemTable::new(rx);
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
