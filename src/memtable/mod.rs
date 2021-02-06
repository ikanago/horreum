use crate::command::Command;
use crate::format::InternalPair;
use crate::Message;
use log::{debug, info, warn};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::{mpsc, oneshot, RwLock};

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
    flushing_tx: mpsc::Sender<Message>,
}

impl MemTable {
    /// Create a new instance.
    pub fn new(
        size_limit: usize,
        command_rx: mpsc::Receiver<Message>,
        flushing_tx: mpsc::Sender<Message>,
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
            Command::Flush { .. } => unreachable!("Flush command is not called in MemTable"),
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

        let new_key_len = key.len();
        let new_value_len = value.len();
        let prev_value = map.insert(key, Some(value));
        match prev_value.as_ref() {
            // There already exists key-value pair.
            // Add diff between new and old value length.
            Some(Some(value)) => self
                .actual_size
                .fetch_add(new_value_len - value.len(), Ordering::Release),
            // There exists deleted key-value pair.
            // Just add value length.
            Some(None) => self.actual_size.fetch_add(new_value_len, Ordering::Release),
            // New key-value pair.
            None => self
                .actual_size
                .fetch_add(new_key_len + new_value_len, Ordering::Release),
        };
        // Drop lock here to acquire lock in `flush()` which may be called after.
        drop(map);

        debug!("{}", self.actual_size.load(Ordering::Acquire));
        if self.actual_size.load(Ordering::Acquire) > self.size_limit {
            info!("MemTable data flushing has started");
            self.flush().await;
            self.actual_size.store(0, Ordering::Release);
        }
        prev_value.flatten()
    }

    /// Mark value corresponding to a key as deleted.
    /// Return `true` if there was an entry to delete.
    pub async fn delete(&self, key: &[u8]) -> Option<Vec<u8>> {
        let mut map = self.inner.write().await;

        // Check entry for the key to avoid mark a key which is not registered as `Deleted`.
        let prev_value = map.insert(key.to_vec(), None).flatten();
        if let Some(prev_value) = prev_value.as_ref() {
            self.actual_size
                .fetch_sub(prev_value.len(), Ordering::Acquire);
        }
        debug!("{}", self.actual_size.load(Ordering::Acquire));
        prev_value
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

        let (tx, rx) = oneshot::channel();
        if let Err(_) = self
            .flushing_tx
            .send((
                Command::Flush {
                    pairs,
                    size: self.actual_size.load(Ordering::SeqCst),
                },
                tx,
            ))
            .await
        {
            warn!("The receiver dropped");
        }
        // Wait for finishing flush
        if let Err(_) = rx.await {
            warn!("The sender dropped");
        }

        let mut map = map;
        map.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MEMTABLE_SIZE: usize = 128;

    #[tokio::test]
    async fn put_and_get() {
        let (_, rx) = mpsc::channel(1);
        let (tx, _) = mpsc::channel(1);
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
        let (tx, _) = mpsc::channel(1);
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
        let (tx, _) = mpsc::channel(1);
        let table = MemTable::new(MEMTABLE_SIZE, rx, tx);
        assert_eq!(None, table.delete(b"abc").await);
        assert_eq!(None, table.get(b"abc").await);
    }
}
