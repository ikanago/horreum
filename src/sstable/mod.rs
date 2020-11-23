pub mod format;
mod index;
pub mod manager;
mod storage;
mod table;

#[cfg(test)]
pub(crate) mod tests {
    use std::fs::{File, OpenOptions};
    use std::io::{self, Read, Write};
    use std::path::Path;

    pub(crate) fn read_file_to_buffer<P: AsRef<Path>>(path: P) -> Vec<u8> {
        let mut file = File::open(path).unwrap();
        let mut buffer: Vec<u8> = vec![];
        file.read_to_end(&mut buffer).unwrap();
        buffer
    }

    pub(crate) fn prepare_sstable_file<P: AsRef<Path>>(path: P, data: &[u8]) -> io::Result<()> {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(path)?;
        file.write_all(data)?;
        Ok(())
    }
}
