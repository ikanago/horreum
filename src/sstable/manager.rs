use crate::sstable::format::InternalPair;
use crate::sstable::storage::PersistedFile;
use crate::sstable::table::{SSTable, SSTableIterator};
use std::cmp::Reverse;
use std::collections::VecDeque;
use std::fs;
use std::io;
use std::mem;
use std::path::{Path, PathBuf};

/// Manage multiple SSTable instances.
/// All operation to an SSTalbe is taken via this struct.
#[derive(Debug)]
pub struct SSTableManager {
    /// Directory to store SSTable's files.
    /// Files in the directory is sorted by thier name(like table_0, table_1, table_2...).
    /// File with bigger number at the end of the file name is newer one.
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

        let mut paths: Vec<_> = fs::read_dir(&table_directory)?
            .into_iter()
            .filter_map(|path| path.ok())
            .collect();
        paths.sort_by_key(|path| Reverse(path.path()));
        let tables = paths
            .iter()
            .filter_map(|path| SSTable::open(path.path(), block_stride).ok())
            .collect();
        Ok(Self {
            table_directory,
            block_stride,
            tables,
        })
    }

    /// Create a new SSTable with given pairs.
    pub fn create(&mut self, pairs: Vec<InternalPair>) -> io::Result<()> {
        let table_path = self.new_table_path();
        let bytes: Vec<u8> = InternalPair::serialize_flatten(&pairs);
        let file = PersistedFile::new(table_path, &bytes).unwrap();
        let table = SSTable::new(file, pairs, 3).unwrap();
        self.tables.push_front(table);
        Ok(())
    }

    fn new_table_path(&self) -> PathBuf {
        let mut table_path = self.table_directory.clone();
        table_path.push(format!("table_{}", self.tables.len()));
        table_path
    }

    /// Get a pair by given key among SSTables.
    pub fn get(&mut self, key: &[u8]) -> io::Result<Option<InternalPair>> {
        for table in self.tables.iter_mut() {
            let pair = table.get(key)?;
            if pair.is_some() {
                return Ok(pair);
            }
        }
        Ok(None)
    }

    /// Compact current all SSTables into a new one.
    pub fn compact(&mut self) -> io::Result<()> {
        let num_tables = self.tables.len();
        let tables = mem::replace(&mut self.tables, VecDeque::new());
        let table_iterators = tables
            .into_iter()
            .map(|table| table.into_iter())
            .collect::<Vec<SSTableIterator>>();
        let pairs = Self::compact_inner(num_tables, table_iterators);

        let table_path = self.new_table_path();
        let bytes: Vec<u8> = pairs.iter().flat_map(|pair| pair.serialize()).collect();
        let file = PersistedFile::new(table_path, &bytes).unwrap();
        let merged_table = SSTable::new(file, pairs, self.block_stride)?;
        self.tables.push_front(merged_table);
        Ok(())
    }

    /// Read SSTable elements one by one for each SSTable and hold them as `merge_candidate`.
    /// Select a minimum key of them to keep sorted order.
    /// If there are multiple key of the same order, the newer one is selected.
    fn compact_inner(
        // Because `self.tables` is replaced with new `VecDeque` in `compact()`,
        // `num_tables` is given explicitly.
        num_tables: usize,
        mut table_iterators: Vec<impl Iterator<Item = InternalPair>>,
    ) -> Vec<InternalPair> {
        // Array of current first elements for each SSTable.
        let mut merge_candidates = (0..num_tables)
            .map(|i| table_iterators[i].next())
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
                            *pair_opt = table_iterators[i].next();
                        }
                    }
                });
            if merge_candidates.iter().all(|x| x.is_none()) {
                break;
            }
        }
        pairs
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sstable::tests::*;

    #[test]
    fn open_existing_files() -> io::Result<()> {
        let path = "test_open_existing_files";
        let _ = std::fs::create_dir(path);
        let pairs1 = vec![
            InternalPair::new("abc00", Some("def")),
            InternalPair::new("abc01", Some("defg")),
        ];
        let pairs2 = vec![
            InternalPair::new("abc00", Some("xyz")),
            InternalPair::new("abc01", None),
        ];
        let pairs3 = vec![InternalPair::new("abc02", Some("def"))];
        let data1 = InternalPair::serialize_flatten(&pairs1);
        let data2 = InternalPair::serialize_flatten(&pairs2);
        let data3 = InternalPair::serialize_flatten(&pairs3);
        prepare_sstable_file("test_open_existing_files/table_0", &data1)?;
        prepare_sstable_file("test_open_existing_files/table_1", &data2)?;
        prepare_sstable_file("test_open_existing_files/table_2", &data3)?;

        let mut manager = SSTableManager::new(path, 3)?;
        assert_eq!(
            InternalPair::new("abc00", Some("xyz")),
            manager.get("abc00".as_bytes())?.unwrap()
        );
        assert_eq!(
            InternalPair::new("abc01", None),
            manager.get("abc01".as_bytes())?.unwrap()
        );
        assert_eq!(
            InternalPair::new("abc02", Some("def")),
            manager.get("abc02".as_bytes())?.unwrap()
        );
        Ok(())
    }

    #[test]
    fn get_pairs() -> io::Result<()> {
        let path = "test_get_create";
        let _ = std::fs::create_dir(path);
        let mut manager = SSTableManager::new(path, 2)?;
        let pairs1 = vec![
            InternalPair::new("abc00", Some("def")),
            InternalPair::new("abc01", Some("defg")),
        ];
        let pairs2 = vec![
            InternalPair::new("abc00", Some("xyz")),
            InternalPair::new("abc01", None),
        ];
        let pairs3 = vec![InternalPair::new("abc02", Some("def"))];
        manager.create(pairs1)?;
        manager.create(pairs2)?;
        manager.create(pairs3)?;
        assert_eq!(
            InternalPair::new("abc00", Some("xyz")),
            manager.get("abc00".as_bytes())?.unwrap()
        );
        assert_eq!(
            InternalPair::new("abc01", None),
            manager.get("abc01".as_bytes())?.unwrap()
        );
        assert_eq!(
            InternalPair::new("abc02", Some("def")),
            manager.get("abc02".as_bytes())?.unwrap()
        );
        Ok(())
    }

    #[test]
    fn compaction() {
        let table1 = vec![
            InternalPair::new("abc00", Some("def")),
            InternalPair::new("abc01", Some("defg")),
            InternalPair::new("abc02", Some("xyz")),
            InternalPair::new("abc03", Some("defg")),
        ];
        let table2 = vec![
            InternalPair::new("abc00", Some("xyz")),
            InternalPair::new("abc01", None),
        ];
        let table3 = vec![
            InternalPair::new("abc02", Some("def")),
            InternalPair::new("abc04", Some("hoge")),
            InternalPair::new("abc05", None),
        ];
        let expected = vec![
            InternalPair::new("abc00", Some("xyz")),
            InternalPair::new("abc01", None),
            InternalPair::new("abc02", Some("def")),
            InternalPair::new("abc03", Some("defg")),
            InternalPair::new("abc04", Some("hoge")),
            InternalPair::new("abc05", None),
        ];
        let tables = vec![table3, table2, table1];
        let num_table = tables.len();
        let table_iterators = tables.into_iter().map(|table| table.into_iter()).collect();
        assert_eq!(
            expected,
            SSTableManager::compact_inner(num_table, table_iterators)
        );
    }
}
