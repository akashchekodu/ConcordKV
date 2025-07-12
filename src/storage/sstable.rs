use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

/// Write a BTreeMap to disk as a sorted SSTable
pub fn flush_to_sstable(
    data: &BTreeMap<Vec<u8>, Vec<u8>>,
    sstable_dir: &Path,
    sstable_id: u64,
) -> std::io::Result<PathBuf> {
    let file_path = sstable_dir.join(format!("sstable_{}.sst", sstable_id));
    let file = File::create(&file_path)?;
    let mut writer = BufWriter::new(file);

    for (key, value) in data.iter() {
        writer.write_all(&(key.len() as u32).to_le_bytes())?;
        writer.write_all(key)?;
        writer.write_all(&(value.len() as u32).to_le_bytes())?;
        writer.write_all(value)?;
    }

    writer.flush()?;
    Ok(file_path)
}

/// Load key-value pairs from an SSTable file
pub fn load_sstable(path: &Path) -> std::io::Result<BTreeMap<Vec<u8>, Vec<u8>>> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    let mut map = BTreeMap::new();

    loop {
        let mut len_buf = [0u8; 4];
        if reader.read_exact(&mut len_buf).is_err() {
            break;
        }
        let key_len = u32::from_le_bytes(len_buf) as usize;
        let mut key = vec![0u8; key_len];
        reader.read_exact(&mut key)?;

        reader.read_exact(&mut len_buf)?;
        let val_len = u32::from_le_bytes(len_buf) as usize;
        let mut val = vec![0u8; val_len];
        reader.read_exact(&mut val)?;

        map.insert(key, val);
    }

    Ok(map)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::fs;
    use std::path::PathBuf;

    #[test]
    fn test_flush_and_load_sstable() {
        // Setup temp dir
        let test_dir = PathBuf::from("test_sst");
        let _ = fs::remove_dir_all(&test_dir); // cleanup if exists
        fs::create_dir_all(&test_dir).unwrap();

        // Original map to flush
        let mut original = BTreeMap::new();
        original.insert(b"apple".to_vec(), b"red".to_vec());
        original.insert(b"banana".to_vec(), b"yellow".to_vec());
        original.insert(b"grape".to_vec(), b"purple".to_vec());

        // Flush to SSTable
        flush_to_sstable(&original, &test_dir, 1).unwrap();

        // Read it back from "sstable_1.sst"
        let loaded = load_sstable(&test_dir.join("sstable_1.sst")).unwrap();

        // Assert equality
        assert_eq!(original.len(), loaded.len());
        for (k, v) in original {
            assert_eq!(loaded.get(&k), Some(&v));
        }

        // Clean up
        let _ = fs::remove_dir_all(&test_dir);
    }
}
