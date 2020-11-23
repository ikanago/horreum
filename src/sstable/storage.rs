use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

/// Represents manipulating an SSTable file.
#[derive(Debug)]
pub struct PersistedFile {
    pub(crate) path: PathBuf,
    pub(crate) buffer: BufReader<File>,
}

impl PersistedFile {
    pub fn new<P: AsRef<Path>>(path: P, data: &[u8]) -> io::Result<Self> {
        let mut path_buf = PathBuf::new();
        path_buf.push(path);
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(path_buf.as_path())?;
        file.write_all(data)?;
        let mut buffer = BufReader::new(file);
        buffer.seek(SeekFrom::Start(0))?;
        Ok(Self {
            path: path_buf,
            buffer,
        })
    }

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

    #[test]
    fn read() {
        let data = vec![1, 2, 3, 4, 5, 6, 7, 8];
        let mut file = PersistedFile::new("test_read", &data).unwrap();
        let mut buffer = vec![0; 4];
        file.read_exact(&mut buffer).unwrap();
        assert_eq!(vec![1, 2, 3, 4], buffer);
        file.read_exact(&mut buffer).unwrap();
        assert_eq!(vec![5, 6, 7, 8], buffer);
    }
}
