use crate::format::InternalPair;
use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use tokio::fs::{File, OpenOptions};
use tokio::io::{self, AsyncReadExt, AsyncWriteExt};

/// Represents manipulating an SSTable file.
/// Contents of the file will never be modified.
#[derive(Debug)]
pub struct PersistedFile {
    /// SSTable file.
    file: File,
}

impl PersistedFile {
    /// Serialize and write array of `InternalePair` and return a new `PersistedFile` instance.
    pub async fn new<P: AsRef<Path>>(path: P, pairs: &[InternalPair]) -> io::Result<Self> {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(path)
            .await?;

        let data = InternalPair::serialize_flatten(&pairs);
        file.write_all(&data).await?;
        file.seek(SeekFrom::Start(0)).await?;
        Ok(Self { file })
    }

    /// Create an instance based on an existing file.
    pub async fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let mut path_buf = PathBuf::new();
        path_buf.push(path);
        let file = File::open(&path_buf.as_path()).await?;
        Ok(Self { file })
    }

    /// Read file contents at `position` by `length`.
    pub async fn read_at(&mut self, position: usize, length: usize) -> io::Result<Vec<u8>> {
        self.file.seek(SeekFrom::Start(position as u64)).await?;
        let mut bytes = vec![0; length];
        self.file.read_exact(&mut bytes).await?;
        Ok(bytes)
    }

    /// Read all file contents.
    pub async fn read_all(&mut self) -> io::Result<Vec<InternalPair>> {
        self.file.seek(SeekFrom::Start(0)).await?;
        let mut buffer = Vec::new();
        self.file.read_to_end(&mut buffer).await?;
        Ok(InternalPair::deserialize_from_bytes(&mut buffer)
            .await
            .unwrap())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn read() -> io::Result<()> {
        let pairs = vec![
            InternalPair::new(b"abc00", Some(b"def")),
            InternalPair::new(b"abc01", None),
        ];
        let mut file = PersistedFile::new("test_read", &pairs).await?;
        let mut buffer = Vec::new();
        file.file.read_to_end(&mut buffer).await?;
        assert_eq!(
            vec![
                5, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 97, 98, 99, 48, 48, 100, 101, 102,
                5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 97, 98, 99, 48, 49,
            ],
            buffer
        );
        Ok(())
    }

    #[tokio::test]
    async fn read_all() -> io::Result<()> {
        let pairs = vec![
            InternalPair::new(b"abc00", Some(b"def")),
            InternalPair::new(b"abc01", Some(b"xxx")),
            InternalPair::new(b"abc02", None),
        ];
        let mut file = PersistedFile::new("test_read_all", &pairs).await?;
        assert_eq!(pairs, file.read_all().await?);
        Ok(())
    }
}
