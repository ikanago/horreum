use crate::{memtable::MemTable, sstable::manager::SSTableManager};
use std::{sync::Arc, path::PathBuf};
use std::path::Path;

pub struct Horreum {
    sstable_directory: PathBuf,
    memtable: Arc<MemTable>,
    sstable: Arc<SSTableManager>,
}

impl Horreum {
    pub fn new<P: AsRef<Path>>(sstable_directory: P, block_stride: usize) -> Self {
        
    }
}