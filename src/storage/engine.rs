use std::sync::Arc;
use std::thread;
use std::path::PathBuf;


use super::{MemTable, WriteAheadLog, Compactor, TOMBSTONE};

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
            std::thread::sleep(std::time::Duration::from_secs(30));
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
        self.memtable.get(key)
    }

    pub fn scan(&self, prefix: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.memtable.scan(prefix)
    }
}
