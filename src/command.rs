pub enum Command<'a> {
    Get { key: &'a [u8] },
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: &'a [u8] },
}
