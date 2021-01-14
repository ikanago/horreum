use crate::command::Command;
use crate::format::InternalPair;
use crate::Message;
use log::{debug, warn};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::{mpsc, RwLock};

/// `MemTable` is an in-memory key-value store.
/// Imbound data is accumulated in `BTreeMap` this struct holds.
/// `MemTable` records deletion histories because `SSTable` needs them.
pub struct MemTable {
    // Because `MemTable` receives asynchronous request,
    // a map of key and value is wrapped in `RwLock`.
    inner: RwLock<BTreeMap<Vec<u8>, Option<Vec<u8>>>>,

    /// Limit of the contents size.
    /// If actual contents size exceeds this limit after write,
    /// Whole contents in a `MemTable` is flushed.
    size_limit: usize,

    /// Number of bytes `MemTable` currently stores.
    actual_size: AtomicUsize,

    /// Receiver to receive command.
    command_rx: mpsc::Receiver<Message>,

    /// Sender to send flushed data to `SSTableManager`.
    flushing_tx: crossbeam_channel::Sender<Vec<InternalPair>>,
}

impl MemTable {
    /// Create a new instance.
    pub fn new(
        size_limit: usize,
        command_rx: mpsc::Receiver<Message>,
        flushing_tx: crossbeam_channel::Sender<Vec<InternalPair>>,
    ) -> Self {
        Self {
            inner: RwLock::new(BTreeMap::new()),
            size_limit,
            actual_size: AtomicUsize::new(0),
            command_rx,
            flushing_tx,
        }
    }

    /// Listen to requests and send back results.
    pub async fn listen(&mut self) {
        while let Some((command, tx)) = self.command_rx.recv().await {
            let entry = self.apply(command).await;
            if let Err(_) = tx.send(entry) {
                warn!("The receiver already dropped");
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
        self.actual_size.fetch_add(key.len() + value.len(), Ordering::Release);
        let result = map.insert(key, Some(value)).flatten();
        // Drop lock here to acquire lock in `flush()` which may be called after.
        drop(map);

        debug!("{}", self.actual_size.load(Ordering::Acquire));
        if self.actual_size.load(Ordering::Acquire) > self.size_limit {
            self.flush().await;
            self.actual_size.store(0, Ordering::Release);
        }
        result
    }

    /// Mark value corresponding to a key as deleted.
    /// Return `true` if there was an entry to delete.
    pub async fn delete(&self, key: &[u8]) -> Option<Vec<u8>> {
        let mut map = self.inner.write().await;
        // Check entry for the key to avoid mark a key which is not registered as `Deleted`.
        let result = map.insert(key.to_vec(), None).flatten();
        self.actual_size.fetch_sub(1, Ordering::Acquire);
        result
    }

    /// Read whole data in `MemTable` and send to `SSTableManager`.
    async fn flush(&self) {
        // Acquire write lock to prevent other tasks update `MemTable` contents.
        // If the contents is updated while flushing, flushed data(passed to `SSTable`)
        // and desired one will be different.
        let map = self.inner.write().await;
        let pairs = map
            .iter()
            .map(|(key, entry)| match entry {
                Some(value) => InternalPair::new(key, Some(value)),
                None => InternalPair::new(key, None),
            })
            .collect();
        if let Err(_) = self.flushing_tx.send(pairs) {
            warn!("The receiver dropped");
        }

        let mut map = map;
        map.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crossbeam_channel::unbounded;

    const MEMTABLE_SIZE: usize = 128;

    #[tokio::test]
    async fn put_and_get() {
        let (_, rx) = mpsc::channel(1);
        let (tx, _) = unbounded();
        let table = MemTable::new(MEMTABLE_SIZE, rx, tx);
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
        let (tx, _) = unbounded();
        let table = MemTable::new(MEMTABLE_SIZE, rx, tx);
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
        let (tx, _) = unbounded();
        let table = MemTable::new(MEMTABLE_SIZE, rx, tx);
        assert_eq!(None, table.delete(b"abc").await);
        assert_eq!(None, table.get(b"abc").await);
    }
}
