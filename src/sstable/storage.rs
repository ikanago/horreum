use crate::format::InternalPair;
use std::path::{Path, PathBuf};
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, Read, Write,SeekFrom, Seek};

/// Represents manipulating an SSTable file.
#[derive(Debug)]
pub struct PersistedFile {
    pub(crate) path: PathBuf,
    pub(crate) buffer: BufReader<File>,
}

impl PersistedFile {
    /// Serialize and write array of `InternalePair` and return a new `PersistedFile` instance.
    pub fn new<P: AsRef<Path>>(path: P, pairs: &[InternalPair]) -> io::Result<Self> {
        let mut path_buf = PathBuf::new();
        path_buf.push(path);
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(path_buf.as_path())?;

        let data = InternalPair::serialize_flatten(&pairs);
        file.write_all(&data)?;
        file.seek(SeekFrom::Start(0))?;
        Ok(Self {
            path: path_buf,
            buffer: BufReader::new(file),
        })
    }

    /// Create an instance based on an existing file.
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let mut path_buf = PathBuf::new();
        path_buf.push(path);
        let file = File::open(&path_buf.as_path())?;
        Ok(Self {
            path: path_buf,
            buffer: BufReader::new(file),
        })
    }

    pub fn read_at(&mut self, positioin: usize, length: usize) -> io::Result<Vec<u8>> {
        self.buffer.seek(SeekFrom::Start(positioin as u64))?;
        let mut bytes = vec![0; length];
        self.buffer.read_exact(&mut bytes)?;
        Ok(bytes)
    }
}

impl Read for PersistedFile {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.buffer.read(buf)
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
        let mut file = PersistedFile::new("test_read", &pairs)?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;
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
