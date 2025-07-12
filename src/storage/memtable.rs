use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use super::sstable::{flush_to_sstable, load_sstable};
use super::compactor::Compactor;
pub const TOMBSTONE: &[u8] = b"__tombstone__";


#[derive(Clone)]
pub struct MemTable {
    inner: Arc<Mutex<BTreeMap<Vec<u8>, Vec<u8>>>>,
    sst_dir: PathBuf,
    flush_threshold: usize,
    current_size: Arc<Mutex<usize>>,
    sst_counter: Arc<Mutex<u64>>,
    compactor: Compactor,
    compaction_trigger_threshold: usize,
}

impl MemTable {
    pub fn new(sst_dir: PathBuf, flush_threshold: usize) -> Self {
        fs::create_dir_all(&sst_dir).unwrap();

        let compactor = Compactor::new(sst_dir.clone());
        let compactor_clone = compactor.clone();

        // 🧵 Background compaction thread
        thread::spawn(move || loop {
            thread::sleep(Duration::from_secs(10));
            if let Err(e) = compactor_clone.compact() {
                eprintln!("[Compactor Thread] Compaction failed: {}", e);
            } else {
                println!("[Compactor Thread] Compaction completed.");
            }
        });

        Self {
            inner: Arc::new(Mutex::new(BTreeMap::new())),
            sst_dir: sst_dir.clone(),
            flush_threshold,
            current_size: Arc::new(Mutex::new(0)),
            sst_counter: Arc::new(Mutex::new(0)),
            compactor,
            compaction_trigger_threshold: 4,
        }
    }

    pub fn put(&self, key: Vec<u8>, value: Vec<u8>) {
        {
            let mut map = self.inner.lock().unwrap();
            let mut size = self.current_size.lock().unwrap();
            *size += key.len() + value.len();
            map.insert(key, value);
        }

        let size = *self.current_size.lock().unwrap();
        if size > self.flush_threshold {
            self.flush().unwrap();
        }
    }

    fn flush(&self) -> std::io::Result<()> {
        let map = self.inner.lock().unwrap();
        let mut sst_id = self.sst_counter.lock().unwrap();
        flush_to_sstable(&map, &self.sst_dir, *sst_id)?;
        *sst_id += 1;
        drop(map);

        let mut map = self.inner.lock().unwrap();
        map.clear();

        let mut size = self.current_size.lock().unwrap();
        *size = 0;

        // 🧹 Trigger compaction if SST files exceed threshold
        let sst_count = fs::read_dir(&self.sst_dir)?
            .filter(|f| {
                f.as_ref()
                    .ok()
                    .and_then(|e| e.path().extension().map(|ext| ext == "sst"))
                    .unwrap_or(false)
            })
            .count();

        if sst_count >= self.compaction_trigger_threshold {
            println!("[MemTable] Compaction triggered ({} SSTs)", sst_count);
            self.compactor.compact()?; // Sync call
            let mut counter = self.sst_counter.lock().unwrap();
            *counter = 1; // Reset ID
        }

        Ok(())
    }

    /// Deletes a key using a tombstone marker
    pub fn delete(&self, key: Vec<u8>) {
        {
            let mut map = self.inner.lock().unwrap();
            let mut size = self.current_size.lock().unwrap();
            *size += key.len() + TOMBSTONE.len();
            map.insert(key, TOMBSTONE.to_vec());
        }

        let size = *self.current_size.lock().unwrap();
        if size > self.flush_threshold {
            self.flush().unwrap();
        }
    }
    pub fn scan(&self, prefix: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
        let map = self.inner.lock().unwrap();

        map.range(prefix.to_vec()..)  // All keys >= prefix
            .take_while(|(k, _)| k.starts_with(prefix))
            .filter(|(_, v)| v.as_slice() != TOMBSTONE)
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }


    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let map = self.inner.lock().unwrap();
        if let Some(val) = map.get(key) {
            if val == TOMBSTONE {
                return None;
            }
            return Some(val.clone());
        }
        drop(map); // unlock before disk IO

        let mut files = fs::read_dir(&self.sst_dir)
            .ok()?
            .filter_map(Result::ok)
            .collect::<Vec<_>>();

        files.sort_by_key(|f| std::cmp::Reverse(f.file_name())); // newest first

        for entry in files {
            let path = entry.path();
            if let Ok(map) = load_sstable(&path) {
                if let Some(val) = map.get(key) {
                    if val == TOMBSTONE {
                        return None;
                    }
                    return Some(val.clone());
                }
            }
        }

        None
    }

}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_put_and_get_in_memory() {
        let dir = PathBuf::from("test_memtable_dir");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        let memtable = MemTable::new(dir.clone(), 1024 * 1024); // 1MB threshold

        memtable.put(b"key1".to_vec(), b"value1".to_vec());
        memtable.put(b"key2".to_vec(), b"value2".to_vec());

        assert_eq!(memtable.get(b"key1"), Some(b"value1".to_vec()));
        assert_eq!(memtable.get(b"key2"), Some(b"value2".to_vec()));
        assert_eq!(memtable.get(b"key3"), None);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_flush_to_sstable_and_read_back() {
        let dir = PathBuf::from("test_memtable_flush");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        let memtable = MemTable::new(dir.clone(), 1); // Flush immediately

        memtable.put(b"apple".to_vec(), b"red".to_vec());
        memtable.put(b"banana".to_vec(), b"yellow".to_vec());

        assert_eq!(memtable.get(b"apple"), Some(b"red".to_vec()));
        assert_eq!(memtable.get(b"banana"), Some(b"yellow".to_vec()));
        assert_eq!(memtable.get(b"grape"), None);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_delete_removes_key() {
        let dir = PathBuf::from("test_memtable_delete");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        let memtable = MemTable::new(dir.clone(), 1024 * 1024); // 1MB flush threshold

        memtable.put(b"apple".to_vec(), b"red".to_vec());
        assert_eq!(memtable.get(b"apple"), Some(b"red".to_vec()));

        memtable.delete(b"apple".to_vec());
        assert_eq!(memtable.get(b"apple"), None); // Should be gone

        let _ = fs::remove_dir_all(&dir);
    }
    #[test]
fn test_delete_survives_flush() {
    let dir = PathBuf::from("test_delete_flush_dir");
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();

    // force flush on each write
    let memtable = MemTable::new(dir.clone(), 1);

    memtable.put(b"apple".to_vec(), b"red".to_vec());
    assert_eq!(memtable.get(b"apple"), Some(b"red".to_vec()));

    memtable.delete(b"apple".to_vec());
    assert_eq!(memtable.get(b"apple"), None);

    // Force a manual flush
    memtable.flush().unwrap();

    // Load a fresh memtable to simulate restart
    let memtable2 = MemTable::new(dir.clone(), 1024); 
    assert_eq!(memtable2.get(b"apple"), None);

    let _ = fs::remove_dir_all(&dir);
}

}
