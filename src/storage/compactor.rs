use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use super::sstable::{flush_to_sstable, load_sstable};

#[derive(Clone)]
pub struct Compactor {
    sst_dir: PathBuf,
}

impl Compactor {
    pub fn new(sst_dir: PathBuf) -> Self {
        Self { sst_dir }
    }

    pub fn compact(&self) -> std::io::Result<()> {
        let mut merged = BTreeMap::new();

        // Read all SSTables (oldest first)
        let mut files = fs::read_dir(&self.sst_dir)?
            .filter_map(Result::ok)
            .map(|f| f.path())
            .filter(|f| f.extension().map_or(false, |ext| ext == "sst"))
            .collect::<Vec<_>>();

        files.sort(); // oldest first

        for file in &files {
            if let Ok(map) = load_sstable(file) {
                for (k, v) in map {
                    merged.insert(k, v); // newer values overwrite
                }
            }
        }

        // Delete old SSTables
        for file in &files {
            let _ = fs::remove_file(file);
        }

        // Write single compacted SSTable
        flush_to_sstable(&merged, &self.sst_dir, 0)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_compaction_merges_sstables() {
        let dir = PathBuf::from("test_compact_dir");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        // SSTable 0
        let mut sst1 = BTreeMap::new();
        sst1.insert(b"apple".to_vec(), b"red".to_vec());
        sst1.insert(b"banana".to_vec(), b"yellow".to_vec());
        flush_to_sstable(&sst1, &dir, 0).unwrap();

        // SSTable 1 (overwrites "banana")
        let mut sst2 = BTreeMap::new();
        sst2.insert(b"banana".to_vec(), b"green".to_vec());
        sst2.insert(b"grape".to_vec(), b"purple".to_vec());
        flush_to_sstable(&sst2, &dir, 1).unwrap();

        let compactor = Compactor::new(dir.clone());
        compactor.compact().unwrap();

        let merged = load_sstable(&dir.join("sstable_0.sst")).unwrap();
        assert_eq!(merged.get(&b"apple"[..]), Some(&b"red".to_vec()));
        assert_eq!(merged.get(&b"banana"[..]), Some(&b"green".to_vec()));
        assert_eq!(merged.get(&b"grape"[..]), Some(&b"purple".to_vec()));

        let files = fs::read_dir(&dir).unwrap().collect::<Vec<_>>();
        assert_eq!(files.len(), 1); // only 1 SSTable should remain

        let _ = fs::remove_dir_all(&dir);
    }
}
