use super::index::Index;
use super::storage::PersistedFile;
use crate::format::InternalPair;
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
            InternalPair::new(b"abc", Some(b"defg")),
            InternalPair::new(b"abc", None),
            InternalPair::new("日本語💖".as_bytes(), Some("ржавчина".as_bytes())),
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
            InternalPair::new(b"abc00", Some(b"def")),
            InternalPair::new(b"abc01", Some(b"defg")),
            InternalPair::new(b"abc02", Some(b"de")),
            InternalPair::new(b"abc03", Some(b"defgh")),
            InternalPair::new(b"abc04", Some(b"defg")),
            InternalPair::new(b"abc05", Some(b"defghij")),
            InternalPair::new(b"abc06", Some(b"def")),
            InternalPair::new(b"abc07", Some(b"defgh")),
            InternalPair::new(b"abc08", None),
            InternalPair::new(b"abc09", None),
            InternalPair::new(b"abc10", None),
            InternalPair::new(b"abc11", None),
            InternalPair::new(b"abc12", None),
            InternalPair::new(b"abc13", None),
            InternalPair::new(b"abc14", None),
            InternalPair::new(b"abc15", None),
        ];
        let file = PersistedFile::new(path, &pairs)?;
        let mut table = SSTable::new(file, pairs, 3)?;
        assert_eq!(
            InternalPair::new(b"abc04", Some(b"defg")),
            table.get(b"abc04").unwrap().unwrap()
        );
        assert_eq!(
            InternalPair::new(b"abc15", None),
            table.get(b"abc15").unwrap().unwrap()
        );
        assert_eq!(None, table.get(b"abc011").unwrap());
        assert_eq!(None, table.get(b"abc16").unwrap());
        Ok(())
    }

    #[test]
    fn iterate_table() -> io::Result<()> {
        let path = "test_iterate_table";
        let pairs = vec![
            InternalPair::new(b"abc00", Some(b"def")),
            InternalPair::new(b"abc01", Some(b"defg")),
            InternalPair::new(b"abc02", None),
        ];
        let file = PersistedFile::new(path, &pairs)?;
        let table = SSTable::new(file, pairs, 3)?;
        let mut table_iter = table.into_iter();
        assert_eq!(
            Some(InternalPair::new(b"abc00", Some(b"def"))),
            table_iter.next()
        );
        assert_eq!(
            Some(InternalPair::new(b"abc01", Some(b"defg"))),
            table_iter.next()
        );
        assert_eq!(Some(InternalPair::new(b"abc02", None)), table_iter.next());
        assert_eq!(None, table_iter.next());
        Ok(())
    }

    #[test]
    fn open_existing_file() -> io::Result<()> {
        let path = "test_open_existing_file";
        let pairs = vec![
            InternalPair::new(b"abc00", Some(b"def")),
            InternalPair::new(b"abc01", Some(b"defg")),
            InternalPair::new(b"abc02", None),
        ];
        let data = InternalPair::serialize_flatten(&pairs);
        prepare_sstable_file(path, &data)?;

        let table = SSTable::open(path, 3)?;
        let opened_pairs: Vec<InternalPair> = table.into_iter().collect();
        assert_eq!(pairs, opened_pairs);
        Ok(())
    }
}
