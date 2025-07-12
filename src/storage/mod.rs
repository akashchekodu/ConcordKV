use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
pub mod wal;
pub use wal::WriteAheadLog;

#[derive(Clone, Default)]
pub struct MemTable {
    inner: Arc<Mutex<BTreeMap<Vec<u8>, Vec<u8>>>>,
}

impl MemTable {
    pub fn put(&self, key: Vec<u8>, value: Vec<u8>) {
        let mut map = self.inner.lock().unwrap();
        map.insert(key, value);
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let map = self.inner.lock().unwrap();
        map.get(key).cloned()
    }
}
