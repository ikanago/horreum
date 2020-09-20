use std::sync::{Arc, Mutex};
use std::collections::BTreeMap;

pub struct Horreum<'a> {
    index: Arc<Mutex<BTreeMap<&'a [u8], &'a [u8]>>>
}

impl<'a> Horreum<'a> {
    pub fn new() -> Self {
        Self {
            index: Arc::new(Mutex::new(BTreeMap::new()))
        }
    }
}
