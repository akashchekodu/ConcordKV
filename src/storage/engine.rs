use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use super::{MemTable, WriteAheadLog, Compactor, TOMBSTONE};
use super::sstable::load_sstable;

pub struct KVEngine {
    memtable: MemTable,
    wal: WriteAheadLog,
    sst_dir: PathBuf,
    _compactor_handle: Option<std::thread::JoinHandle<()>>,
}

impl KVEngine {
    pub fn new(wal_path: impl AsRef<std::path::Path>, sst_dir: impl Into<PathBuf>) -> Self {
        let wal_path = wal_path.as_ref().to_path_buf();
        let sst_dir = sst_dir.into();

        let wal = WriteAheadLog::new(&wal_path).expect("failed to open WAL");
        let memtable = MemTable::new(sst_dir.clone(), 2 * 1024 * 1024);

        // Replay WAL
        if let Ok(entries) = WriteAheadLog::replay(&wal_path) {
            for (k, v) in entries {
                if v == TOMBSTONE {
                    memtable.delete(k);
                } else {
                    memtable.put(k, v);
                }
            }
        }

        // Start compactor thread
        let compactor = Compactor::new(sst_dir.clone());
        let handle = thread::spawn(move || loop {
            thread::sleep(Duration::from_secs(30));
            println!("[Compactor] Running...");
            if let Err(e) = compactor.compact() {
                eprintln!("[Compactor] Error: {}", e);
            } else {
                println!("[Compactor] Compaction successful");
            }
        });

        Self {
            memtable,
            wal,
            sst_dir,
            _compactor_handle: Some(handle),
        }
    }

    pub fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), std::io::Error> {
        self.wal.append(&key, &value)?;
        self.memtable.put(key, value);
        Ok(())
    }

    pub fn delete(&self, key: Vec<u8>) -> Result<(), std::io::Error> {
        self.wal.append_delete(&key)?;
        self.memtable.delete(key);
        Ok(())
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        if let Some(value) = self.memtable.get(key) {
            return Some(value);
        }

        // Fallback to SSTables
        let mut sst_files = std::fs::read_dir(&self.sst_dir)
            .ok()?
            .filter_map(Result::ok)
            .map(|f| f.path())
            .filter(|f| f.extension().map_or(false, |ext| ext == "sst"))
            .collect::<Vec<_>>();

        sst_files.sort_by(|a, b| b.cmp(a)); // Newest first

        for file in sst_files {
            if let Ok(map) = load_sstable(&file) {
                if let Some(value) = map.get(key) {
                    if value == TOMBSTONE {
                        return None;
                    }
                    return Some(value.clone());
                }
            }
        }

        None
    }

    pub fn scan(&self, prefix: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
        use std::collections::BTreeMap;

        // Start with MemTable results
        let mut result_map: BTreeMap<Vec<u8>, Vec<u8>> = self.memtable
            .scan(prefix)
            .into_iter()
            .collect();

        // Safely list SST files
        let mut sst_files = match std::fs::read_dir(&self.sst_dir) {
            Ok(files) => files
                .filter_map(Result::ok)
                .map(|f| f.path())
                .filter(|f| f.extension().map_or(false, |ext| ext == "sst"))
                .collect::<Vec<_>>(),
            Err(_) => vec![],
        };

        sst_files.sort_by(|a, b| b.cmp(a)); // Newest first

        for file in sst_files {
            if let Ok(map) = super::sstable::load_sstable(&file) {
                for (k, v) in map {
                    if !k.starts_with(prefix) {
                        continue;
                    }
                    if result_map.contains_key(&k) {
                        continue; // MemTable takes priority
                    }
                    if v == TOMBSTONE {
                        continue; // Deleted
                    }
                    result_map.insert(k, v);
                }
            }
        }

        result_map.into_iter().collect()
    }


}


#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_put_get_delete() {
        let dir = tempdir().unwrap();
        let engine = KVEngine::new(dir.path().join("wal.log"), dir.path());

        engine.put(b"apple".to_vec(), b"red".to_vec()).unwrap();
        assert_eq!(engine.get(b"apple"), Some(b"red".to_vec()));

        engine.delete(b"apple".to_vec()).unwrap();
        assert_eq!(engine.get(b"apple"), None);
    }

    #[test]
    fn test_flush_and_fallback_sstable() {
        let dir = tempdir().unwrap();
        let engine = KVEngine::new(dir.path().join("wal.log"), dir.path());

        for i in 0..1000 {
            engine.put(format!("key{i}").into_bytes(), format!("val{i}").into_bytes()).unwrap();
        }

        // Force flush to SSTable
        engine.memtable.flush().unwrap();

        // Verify fallback read from SSTable
        assert_eq!(engine.get(b"key500"), Some(b"val500".to_vec()));
    }

    #[test]
    fn test_wal_recovery() {
        let dir = tempdir().unwrap();
        {
            let engine = KVEngine::new(dir.path().join("wal.log"), dir.path());
            engine.put(b"k1".to_vec(), b"v1".to_vec()).unwrap();
        }

        // Reconstruct engine to simulate restart
        let recovered = KVEngine::new(dir.path().join("wal.log"), dir.path());
        assert_eq!(recovered.get(b"k1"), Some(b"v1".to_vec()));
    }

    #[test]
    fn test_scan() {
        let dir = tempdir().unwrap();
        let engine = KVEngine::new(dir.path().join("wal.log"), dir.path());

        engine.put(b"cat:1".to_vec(), b"black".to_vec()).unwrap();
        engine.put(b"cat:2".to_vec(), b"white".to_vec()).unwrap();
        engine.put(b"dog:1".to_vec(), b"brown".to_vec()).unwrap();

        let result = engine.scan(b"cat:");
        assert_eq!(result.len(), 2);

        let keys: Vec<_> = result.into_iter().map(|(k, _)| k).collect();
        assert!(keys.contains(&b"cat:1".to_vec()));
        assert!(keys.contains(&b"cat:2".to_vec()));
    }
}
