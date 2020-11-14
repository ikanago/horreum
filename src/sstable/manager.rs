use crate::sstable::format::InternalPair;
use crate::sstable::table::SSTable;
use std::io;
use std::path::{Path, PathBuf};

#[derive(Debug)]
pub struct SSTableManager {
    table_directory: PathBuf,
    block_stride: usize,
    tables: Vec<SSTable>,
}

impl SSTableManager {
    pub fn new<P: AsRef<Path>>(directory: P, block_stride: usize) -> io::Result<Self> {
        let mut table_directory = PathBuf::new();
        table_directory.push(directory);
        Ok(Self {
            table_directory,
            block_stride,
            tables: Vec::new(),
        })
    }

    pub fn create(&mut self, pairs: Vec<InternalPair>) -> io::Result<()> {
        let mut table_path = self.table_directory.clone();
        table_path.push(format!("table{}", self.tables.len()));
        let table = SSTable::new(table_path, pairs, self.block_stride)?;
        self.tables.push(table);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sstable::tests::*;

    #[test]
    fn create() {
        let name = "test_create";
        let _ = std::fs::create_dir(name);
        let mut manager = SSTableManager::new(name, 2).unwrap();
        let pairs = vec![
            InternalPair::new("abc00", Some("def")),
            InternalPair::new("abc01", Some("defg")),
            InternalPair::new("abc02", None),
        ];
        let expected: Vec<u8> = pairs.iter().flat_map(|pair| pair.serialize()).collect();
        manager.create(pairs).unwrap();
        assert_eq!(expected, read_file_to_buffer(manager.tables[0].path.as_path()));
        remove_sstable_directory(name);
    }
}
