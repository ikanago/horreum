pub mod format;
mod index;
pub mod manager;
mod storage;
pub mod table;

#[cfg(test)]
pub(crate) mod tests {
    use std::fs::File;
    use std::io::Read;
    use std::path::Path;

    pub(crate) fn read_file_to_buffer<P: AsRef<Path>>(path: P) -> Vec<u8> {
        let mut file = File::open(path).unwrap();
        let mut buffer: Vec<u8> = vec![];
        file.read_to_end(&mut buffer).unwrap();
        buffer
    }

    pub(crate) fn remove_sstable_file<P: AsRef<Path>>(path: P) {
        // Because this function is only called from test, it can panic
        // if error occurs.
        std::fs::remove_file(path).unwrap()
    }

    pub(crate) fn remove_sstable_directory<P: AsRef<Path>>(path: P) {
        std::fs::remove_dir_all(path).unwrap()
    }
}
