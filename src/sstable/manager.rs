use crate::sstable::format::InternalPair;
use crate::sstable::table::{SSTable, SSTableIterator};
use std::io;
use std::collections::VecDeque;
use std::mem;
use std::path::{Path, PathBuf};

#[derive(Debug)]
pub struct SSTableManager {
    /// Directory to store SSTable's files.
    table_directory: PathBuf,
    block_stride: usize,
    /// Array of SSTables this struct manages.
    /// Front element is the newer.
    tables: VecDeque<SSTable>,
}

impl SSTableManager {
    pub fn new<P: AsRef<Path>>(directory: P, block_stride: usize) -> io::Result<Self> {
        let mut table_directory = PathBuf::new();
        table_directory.push(directory);
        Ok(Self {
            table_directory,
            block_stride,
            tables: VecDeque::new(),
        })
    }

    /// Create a new SSTable with given pairs.
    pub fn create(&mut self, pairs: Vec<InternalPair>) -> io::Result<()> {
        let table_path = self.new_table_path();
        let table = SSTable::new(table_path, pairs, self.block_stride)?;
        self.tables.push_front(table);
        Ok(())
    }

    fn new_table_path(&self) -> PathBuf {
        let mut table_path = self.table_directory.clone();
        table_path.push(format!("table{}", self.tables.len()));
        table_path
    }

    /// Get a pair by given key among SSTables.
    pub fn get(&mut self, key: &[u8]) -> io::Result<Option<InternalPair>> {
        for table in self.tables.iter_mut().rev() {
            let pair = table.get(key)?;
            if pair.is_some() {
                return Ok(pair);
            }
        }
        Ok(None)
    }

    pub fn compact(&mut self) -> io::Result<()> {
        let n_tables = self.tables.len();
        let tables = mem::replace(&mut self.tables, VecDeque::new());
        let mut table_iters = tables
            .into_iter()
            .map(|table| table.into_iter())
            .collect::<Vec<SSTableIterator>>();
        let mut merge_candidates = (0..n_tables)
            .map(|i| table_iters[i].next())
            .collect::<Vec<Option<InternalPair>>>();
        let mut pairs = Vec::new();

        loop {
            let min_pair = merge_candidates
                .iter()
                .filter(|pair| pair.is_some())
                .min_by_key(|pair| &pair.as_ref().unwrap().key)
                .unwrap()
                .as_ref()
                .unwrap()
                .clone();
            let min_key = min_pair.key.clone();
            pairs.push(min_pair);
            merge_candidates
                .iter_mut()
                .enumerate()
                .for_each(|(i, pair_opt)| {
                    if let Some(pair) = pair_opt {
                        if pair.key == min_key {
                            *pair_opt = table_iters[i].next();
                        }
                    }
                });
            if merge_candidates.iter().all(|x| x.is_none()) {
                break;
            }
        }
        let table_path = self.new_table_path();
        let merged_table = SSTable::new("hoge", pairs, self.block_stride)?;
        self.tables.push_front(merged_table);
        Ok(())
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

    #[test]
    fn compaction() {
        let path = "compaction";
        let _ = std::fs::create_dir(path);
        let mut manager = SSTableManager::new(path, 2).unwrap();
        let pairs1 = vec![
            InternalPair::new("abc00", Some("def")),
            InternalPair::new("abc01", Some("defg")),
            InternalPair::new("abc02", Some("xyz")),
            InternalPair::new("abc03", Some("defg")),
        ];
        let pairs2 = vec![
            InternalPair::new("abc00", Some("xyz")),
            InternalPair::new("abc01", None),
        ];
        let pairs3 = vec![
            InternalPair::new("abc02", Some("def")),
            InternalPair::new("abc04", Some("hoge")),
            InternalPair::new("abc05", None),
        ];
        manager.create(pairs1).unwrap();
        manager.create(pairs2).unwrap();
        manager.create(pairs3).unwrap();
        manager.compact().unwrap();
        let expected: Vec<u8> = vec![
            InternalPair::new("abc00", Some("xyz")),
            InternalPair::new("abc01", None),
            InternalPair::new("abc02", Some("def")),
            InternalPair::new("abc03", Some("defg")),
            InternalPair::new("abc04", Some("hoge")),
            InternalPair::new("abc05", None),
        ]
        .iter()
        .flat_map(|pair| pair.serialize())
        .collect();
        assert_eq!(
            expected,
            read_file_to_buffer(manager.tables[0].path.as_path())
        );
        remove_sstable_directory(path);
    }
}
