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

    pub fn get(&mut self, key: &[u8]) -> io::Result<Option<InternalPair>> {
        for table in self.tables.iter_mut().rev() {
            let pair = table.get(key)?;
            if pair.is_some() {
                return Ok(pair);
            }
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sstable::tests::*;

    #[test]
    fn create() {
        let path = "test_create";
        let _ = std::fs::create_dir(path);
        let mut manager = SSTableManager::new(path, 2).unwrap();
        let pairs = vec![
            InternalPair::new("abc00", Some("def")),
            InternalPair::new("abc01", Some("defg")),
            InternalPair::new("abc02", None),
        ];
        let expected: Vec<u8> = pairs.iter().flat_map(|pair| pair.serialize()).collect();
        manager.create(pairs).unwrap();
        assert_eq!(
            expected,
            read_file_to_buffer(manager.tables[0].path.as_path())
        );
        remove_sstable_directory(path);
    }

    #[test]
    fn get_pairs() {
        let path = "get_create";
        let _ = std::fs::create_dir(path);
        let mut manager = SSTableManager::new(path, 2).unwrap();
        let pairs1 = vec![
            InternalPair::new("abc00", Some("def")),
            InternalPair::new("abc01", Some("defg")),
        ];
        let pairs2 = vec![
            InternalPair::new("abc00", Some("xyz")),
            InternalPair::new("abc01", None),
        ];
        let pairs3 = vec![InternalPair::new("abc02", Some("def"))];
        manager.create(pairs1).unwrap();
        manager.create(pairs2).unwrap();
        manager.create(pairs3).unwrap();
        assert_eq!(
            InternalPair::new("abc00", Some("xyz")),
            manager.get("abc00".as_bytes()).unwrap().unwrap()
        );
        assert_eq!(
            InternalPair::new("abc01", None),
            manager.get("abc01".as_bytes()).unwrap().unwrap()
        );
        assert_eq!(
            InternalPair::new("abc02", Some("def")),
            manager.get("abc02".as_bytes()).unwrap().unwrap()
        );
        remove_sstable_directory(path);
    }
}
