pub mod format;
mod index;
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

    pub(crate) fn cleanup_file<P: AsRef<Path>>(path: P) {
        std::fs::remove_file(path).unwrap()
    }
}
