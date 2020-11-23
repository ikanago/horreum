use super::format::InternalPair;
use super::index::Index;
use super::storage::PersistedFile;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::Path;

/// Represents a SSTable.
#[derive(Debug)]
pub struct SSTable {
    /// API to access an SSTable file.
    pub(crate) file: PersistedFile,
    /// Stores pairs of key and position to start read the key from the file.
    pub(crate) index: Index,
}

impl SSTable {
    /// Create a new instance of `Table`.
    pub fn new(
        file: PersistedFile,
        pairs: Vec<InternalPair>,
        block_stride: usize,
    ) -> io::Result<Self> {
        let index = Index::new(pairs, block_stride);
        Ok(Self { file, index })
    }

    pub fn open<P: AsRef<Path>>(path: P, block_stride: usize) -> io::Result<Self> {
        let mut file = PersistedFile::open(path)?;
        let mut data = Vec::new();
        file.read_to_end(&mut data)?;
        // Handle this Result
        let pairs = InternalPair::deserialize_from_bytes(&mut data).unwrap();
        let index = Index::new(pairs, block_stride);

        Ok(Self { file, index })
    }

    /// Get key-value pair from SSTable file.
    /// First, find block which stores the target pair.
    /// Then search the block from the front.
    pub fn get(&mut self, key: &[u8]) -> io::Result<Option<InternalPair>> {
        let (search_origin, length) = match self.index.get(key) {
            Some(pos) => pos,
            None => return Ok(None),
        };
        let mut block_bytes = self.file.read_at(search_origin, length)?;

        // Handle this Result
        let pairs = InternalPair::deserialize_from_bytes(&mut block_bytes).unwrap();
        let pair = pairs.into_iter().find(|pair| pair.key == key);
        Ok(pair)
    }
}

impl IntoIterator for SSTable {
    type Item = InternalPair;
    type IntoIter = SSTableIterator;

    fn into_iter(self) -> Self::IntoIter {
        SSTableIterator::new(self.file)
    }
}

#[derive(Debug)]
pub struct SSTableIterator {
    file: PersistedFile,
}

impl SSTableIterator {
    pub fn new(file: PersistedFile) -> Self {
        let mut file = file;
        file.buffer.seek(SeekFrom::Start(0)).unwrap();
        Self { file }
    }
}

impl Iterator for SSTableIterator {
    type Item = InternalPair;

    fn next(&mut self) -> Option<Self::Item> {
        match InternalPair::deserialize(&mut self.file) {
            Ok(pair) => {
                if !pair.key.is_empty() {
                    Some(pair)
                } else {
                    None
                }
            }
            Err(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sstable::tests::*;

    #[test]
    fn create_table() -> io::Result<()> {
        let path = "test_create_table";
        let pairs = vec![
            InternalPair::new("abc", Some("defg")),
            InternalPair::new("abc", None),
            InternalPair::new("日本語💖", Some("ржавчина")),
        ];
        let file = PersistedFile::new(path, &pairs)?;
        let _table = SSTable::new(file, pairs.clone(), 1)?;
        assert_eq!(
            InternalPair::serialize_flatten(&pairs),
            read_file_to_buffer(path)
        );
        Ok(())
    }

    #[test]
    fn search_table() -> io::Result<()> {
        let path = "test_search_table";
        let pairs = vec![
            InternalPair::new("abc00", Some("def")),
            InternalPair::new("abc01", Some("defg")),
            InternalPair::new("abc02", Some("de")),
            InternalPair::new("abc03", Some("defgh")),
            InternalPair::new("abc04", Some("defg")),
            InternalPair::new("abc05", Some("defghij")),
            InternalPair::new("abc06", Some("def")),
            InternalPair::new("abc07", Some("defgh")),
            InternalPair::new("abc08", None),
            InternalPair::new("abc09", None),
            InternalPair::new("abc10", None),
            InternalPair::new("abc11", None),
            InternalPair::new("abc12", None),
            InternalPair::new("abc13", None),
            InternalPair::new("abc14", None),
            InternalPair::new("abc15", None),
        ];
        let file = PersistedFile::new(path, &pairs)?;
        let mut table = SSTable::new(file, pairs, 3)?;
        assert_eq!(
            InternalPair::new("abc04", Some("defg")),
            table.get("abc04".as_bytes()).unwrap().unwrap()
        );
        assert_eq!(
            InternalPair::new("abc15", None),
            table.get("abc15".as_bytes()).unwrap().unwrap()
        );
        assert_eq!(None, table.get("abc011".as_bytes()).unwrap());
        assert_eq!(None, table.get("abc16".as_bytes()).unwrap());
        Ok(())
    }

    #[test]
    fn iterate_table() -> io::Result<()> {
        let path = "test_iterate_table";
        let pairs = vec![
            InternalPair::new("abc00", Some("def")),
            InternalPair::new("abc01", Some("defg")),
            InternalPair::new("abc02", None),
        ];
        let file = PersistedFile::new(path, &pairs)?;
        let table = SSTable::new(file, pairs, 3)?;
        let mut table_iter = table.into_iter();
        assert_eq!(
            Some(InternalPair::new("abc00", Some("def"))),
            table_iter.next()
        );
        assert_eq!(
            Some(InternalPair::new("abc01", Some("defg"))),
            table_iter.next()
        );
        assert_eq!(Some(InternalPair::new("abc02", None)), table_iter.next());
        assert_eq!(None, table_iter.next());
        Ok(())
    }

    #[test]
    fn open_existing_file() -> io::Result<()> {
        let path = "test_open_existing_file";
        let pairs = vec![
            InternalPair::new("abc00", Some("def")),
            InternalPair::new("abc01", Some("defg")),
            InternalPair::new("abc02", None),
        ];
        let data = InternalPair::serialize_flatten(&pairs);
        prepare_sstable_file(path, &data)?;

        let table = SSTable::open(path, 3)?;
        let opened_pairs: Vec<InternalPair> = table.into_iter().collect();
        assert_eq!(pairs, opened_pairs);
        Ok(())
    }
}
