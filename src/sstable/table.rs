use super::index::Index;
use super::storage::PersistedFile;
use crate::format::InternalPair;
use std::io;
use std::path::Path;

/// Represents an SSTable.
#[derive(Debug)]
pub struct SSTable {
    /// API to access an SSTable file.
    pub(crate) file: PersistedFile,

    /// SSTable contents size in bytes
    size: usize,

    /// Stores pairs of key and position to start read the key from the file.
    pub(crate) index: Index,
}

impl SSTable {
    /// Create a new instance of `Table`.
    pub fn new(
        file: PersistedFile,
        pairs: Vec<InternalPair>,
        size: usize,
        block_stride: usize,
    ) -> io::Result<Self> {
        let index = Index::new(pairs, block_stride);
        Ok(Self { file, size, index })
    }

    /// Open existing file and load key-value pairs in it.
    pub async fn open<P: AsRef<Path>>(path: P, block_stride: usize) -> io::Result<Self> {
        let mut file = PersistedFile::open(path).await?;
        let pairs = file.read_all().await?;
        let size = pairs
            .iter()
            .map(|pair| {
                pair.key.len()
                    + match pair.value.as_ref() {
                        Some(value) => value.len(),
                        None => 0,
                    }
            })
            .sum();
        let index = Index::new(pairs, block_stride);

        Ok(Self { file, size, index })
    }

    /// Get key-value pair from SSTable file.
    /// First, find block which stores the target pair.
    /// Then search the block from the front.
    pub async fn get(&mut self, key: &[u8]) -> io::Result<Option<InternalPair>> {
        let (search_origin, length) = match self.index.get(key) {
            Some(pos) => pos,
            None => return Ok(None),
        };
        let mut block_bytes = self.file.read_at(search_origin, length).await?;

        // Handle this Result
        let pairs = InternalPair::deserialize_from_bytes(&mut block_bytes)
            .await
            .unwrap();
        let pair = match pairs.binary_search_by_key(&key, |entry| &entry.key) {
            Ok(pos) => Some(pairs[pos].clone()),
            Err(_) => None,
        };
        Ok(pair)
    }

    /// Get all key-value pairs in the file.
    pub async fn get_all(&mut self) -> io::Result<Vec<InternalPair>> {
        self.file.read_all().await
    }

    /// Get the size of data in this SSTable.
    pub(crate) fn get_size(&self) -> usize {
        self.size
    }

    pub fn into_file(self) -> PersistedFile {
        self.file
    }

    /// Delete the SSTable file.
    pub async fn delete(&mut self) -> io::Result<()> {
        self.file.delete().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sstable::tests::*;

    #[tokio::test]
    async fn create_table() -> io::Result<()> {
        let path = "test_create_table";
        let pairs = vec![
            InternalPair::new(b"abc", Some(b"defg")),
            InternalPair::new(b"abc", None),
            InternalPair::new("日本語💖".as_bytes(), Some("ржавчина".as_bytes())),
        ];
        let file = PersistedFile::new(path, &pairs).await?;
        let _table = SSTable::new(file, pairs.clone(), 39, 1)?;
        assert_eq!(
            InternalPair::serialize_flatten(&pairs),
            read_file_to_buffer(path)
        );
        Ok(())
    }

    #[tokio::test]
    async fn search_table() -> io::Result<()> {
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
        let file = PersistedFile::new(path, &pairs).await?;
        let mut table = SSTable::new(file, pairs, 113, 3)?;
        assert_eq!(
            Some(InternalPair::new(b"abc04", Some(b"defg"))),
            table.get(b"abc04").await?
        );
        assert_eq!(
            Some(InternalPair::new(b"abc15", None)),
            table.get(b"abc15").await?
        );
        assert_eq!(None, table.get(b"abc011").await?);
        assert_eq!(None, table.get(b"abc16").await?);
        Ok(())
    }

    #[tokio::test]
    async fn iterate_table() -> io::Result<()> {
        let path = "test_iterate_table";
        let pairs = vec![
            InternalPair::new(b"abc00", Some(b"def")),
            InternalPair::new(b"abc01", Some(b"defg")),
            InternalPair::new(b"abc02", None),
        ];
        let file = PersistedFile::new(path, &pairs).await?;
        let mut table = SSTable::new(file, pairs, 22, 3)?;
        let mut pairs = table.get_all().await?.into_iter();
        assert_eq!(
            Some(InternalPair::new(b"abc00", Some(b"def"))),
            pairs.next()
        );
        assert_eq!(
            Some(InternalPair::new(b"abc01", Some(b"defg"))),
            pairs.next()
        );
        assert_eq!(Some(InternalPair::new(b"abc02", None)), pairs.next());
        assert_eq!(None, pairs.next());
        Ok(())
    }

    #[tokio::test]
    async fn open_existing_file() -> io::Result<()> {
        let path = "test_open_existing_file";
        let pairs = vec![
            InternalPair::new(b"abc00", Some(b"def")),
            InternalPair::new(b"abc01", Some(b"defg")),
            InternalPair::new(b"abc02", None),
        ];
        let data = InternalPair::serialize_flatten(&pairs);
        prepare_sstable_file(path, &data)?;

        let mut table = SSTable::open(path, 3).await?;
        let opened_pairs = table.get_all().await?;
        assert_eq!(pairs, opened_pairs);
        Ok(())
    }
}
