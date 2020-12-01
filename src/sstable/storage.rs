use crate::format::InternalPair;
use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::fs::{File, OpenOptions};
use tokio::io::{
    self, AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt, AsyncWrite, AsyncWriteExt, BufReader,
};

/// Represents manipulating an SSTable file.
#[derive(Debug)]
pub struct PersistedFile {
    pub(crate) path: PathBuf,
    pub(crate) file: File,
}

impl PersistedFile {
    /// Serialize and write array of `InternalePair` and return a new `PersistedFile` instance.
    pub async fn new<P: AsRef<Path>>(path: P, pairs: &[InternalPair]) -> io::Result<Self> {
        let mut path_buf = PathBuf::new();
        path_buf.push(path);
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(path_buf.as_path())
            .await?;

        let data = InternalPair::serialize_flatten(&pairs);
        file.write_all(&data).await?;
        file.seek(SeekFrom::Start(0)).await?;
        Ok(Self {
            path: path_buf,
            file,
        })
    }

    /// Create an instance based on an existing file.
    pub async fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let mut path_buf = PathBuf::new();
        path_buf.push(path);
        let file = File::open(&path_buf.as_path()).await?;
        Ok(Self {
            path: path_buf,
            file,
        })
    }

    pub async fn read_at(&mut self, positioin: usize, length: usize) -> io::Result<Vec<u8>> {
        self.file.seek(SeekFrom::Start(positioin as u64)).await?;
        let mut bytes = vec![0; length];
        self.file.read_exact(&mut bytes).await?;
        Ok(bytes)
    }
}

//impl AsyncRead for PersistedFile {
//    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<io::Result<usize>> {
//        self.buffer
//    }
//}

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
}
