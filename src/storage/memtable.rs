use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use super::sstable::{flush_to_sstable, load_sstable};
use super::compactor::Compactor;

/// Special marker used to indicate a tombstone (logical delete).
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
            sst_dir,
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

    /// Deletes a key using a tombstone marker.
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

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let map = self.inner.lock().unwrap();
        match map.get(key) {
            Some(val) if val != TOMBSTONE => Some(val.clone()),
            _ => None,
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

    /// Flush in-memory data to SSTable and reset memory.
    pub fn flush(&self) -> std::io::Result<()> {
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
}
