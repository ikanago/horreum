use crate::{
    memtable::{Entry, MemTable},
    sstable::manager::SSTableManager,
};
use std::io;
use std::path::Path;
use std::{path::PathBuf, sync::Arc};

#[derive(Clone)]
pub struct Horreum {
    sstable_directory: PathBuf,
    memtable: Arc<MemTable>,
    sstable_manager: Arc<SSTableManager>,
}

impl Horreum {
    pub async fn new<P: AsRef<Path>>(
        sstable_directory: P,
        block_stride: usize,
    ) -> io::Result<Self> {
        let mut path_buf = PathBuf::new();
        path_buf.push(sstable_directory);
        let memtable = MemTable::new();
        let manager = SSTableManager::new(path_buf.as_path(), block_stride).await?;
        Ok(Self {
            sstable_directory: path_buf,
            memtable: Arc::new(memtable),
            sstable_manager: Arc::new(manager),
        })
    }

    pub async fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        match self.memtable.get(key).await {
            Some(entry) => match entry {
                Entry::Value(value) => Some(value),
                Entry::Deleted => Some(Vec::new()),
            },
            None => None,
        }
    }

    pub async fn put(&self, key: Vec<u8>, value: Vec<u8>) {
        self.memtable.put(key, value).await
    }

    pub async fn delete(&self, key: &[u8]) -> bool {
        self.memtable.delete(key).await
    }
}
