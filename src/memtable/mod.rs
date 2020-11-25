use std::collections::BTreeMap;

use bytes::Bytes;
use tokio::sync::RwLock;

pub struct MemTable {
    inner: RwLock<BTreeMap<Bytes, Bytes>>,
}

impl MemTable {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(BTreeMap::new()),
        }
    }

    pub async fn put(&mut self, key: Bytes, value: Bytes) {
        let mut map = self.inner.write().await;
        map.insert(key, value);
    }

    pub async fn get(&self, key: &Bytes) -> Option<Bytes> {
        let map = self.inner.read().await;
        map.get(key).cloned()
    }

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
        assert_eq!(
            None,
            table.get(&Bytes::from("abc")).await
        );
        assert_eq!(
            Some(Bytes::from("xxx")),
            table.get(&Bytes::from("xyz")).await
        );
    }
}
