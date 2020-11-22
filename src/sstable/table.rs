use crate::sstable::format::InternalPair;
use crate::sstable::index::{Block, Index};
use crate::sstable::storage::PersistedFile;
use std::io::{self, Seek, SeekFrom};

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
    /// Assume `pairs` is sorted.  
    /// Insert into an index every `block_stride` pair.
    pub fn new(
        file: PersistedFile,
        pairs: Vec<InternalPair>,
        block_stride: usize,
    ) -> io::Result<Self> {
        let mut index = Index::new();
        let mut read_data = Vec::new();

        for pair_chunk in pairs.chunks(block_stride) {
            let mut block = Block::new(&pair_chunk[0].key, read_data.len(), 0);
            let mut block_data: Vec<u8> = InternalPair::serialize_flatten(pair_chunk);
            block.set_length(block_data.len());
            index.push(block);
            read_data.append(&mut block_data);
        }

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
            Err(e) => {
                dbg!(e);
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sstable::tests::*;

    #[test]
    fn create_table() {
        let path = "test_create_table";
        let pairs = vec![
            InternalPair::new("abc", Some("defg")),
            InternalPair::new("abc", None),
            InternalPair::new("日本語💖", Some("ржавчина")),
        ];
        let expected: Vec<u8> = InternalPair::serialize_flatten(&pairs);
        let file = PersistedFile::new(path, &expected.clone()).unwrap();
        let _table = SSTable::new(file, pairs, 1).unwrap();
        assert_eq!(expected, read_file_to_buffer(path));
    }

    #[test]
    fn search_table() {
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
        let bytes: Vec<u8> = InternalPair::serialize_flatten(&pairs);
        let file = PersistedFile::new(path, &bytes).unwrap();
        let mut table = SSTable::new(file, pairs, 3).unwrap();
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
    }

    #[test]
    fn iterate_table() {
        let path = "test_iterate_table";
        let pairs = vec![
            InternalPair::new("abc00", Some("def")),
            InternalPair::new("abc01", Some("defg")),
            InternalPair::new("abc02", None),
        ];
        let bytes: Vec<u8> = InternalPair::serialize_flatten(&pairs);
        let file = PersistedFile::new(path, &bytes).unwrap();
        let table = SSTable::new(file, pairs, 3).unwrap();
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
    }
}
