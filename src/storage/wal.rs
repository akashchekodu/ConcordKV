use std::fs::{OpenOptions, File};
use std::io::{BufWriter, Write, BufReader, BufRead};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::io::Read;
use crate::storage::memtable::TOMBSTONE;


#[derive(Clone)]
pub struct WriteAheadLog {
    file: Arc<Mutex<BufWriter<File>>>,
}

impl WriteAheadLog {
    pub fn new<P: AsRef<Path>>(path: P) -> std::io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    pub fn append(&self, key: &[u8], value: &[u8]) -> std::io::Result<()> {
        let mut writer = self.file.lock().unwrap();
        // Write length-prefixed key and value...
        writer.write_all(&(key.len() as u32).to_le_bytes())?;
        writer.write_all(key)?;
        writer.write_all(&(value.len() as u32).to_le_bytes())?;
        writer.write_all(value)?;
        // Flush user-space buffer
        writer.flush()?;

        // Then sync through the OS to disk
        writer
            .get_ref()         // Get the inner File
            .sync_all()?;      // Force OS to write all buffered data

        Ok(())
    }
    pub fn append_delete(&self, key: &[u8]) -> std::io::Result<()> {
        let mut writer = self.file.lock().unwrap();

        writer.write_all(&(key.len() as u32).to_le_bytes())?;
        writer.write_all(key)?;
        writer.write_all(&(TOMBSTONE.len() as u32).to_le_bytes())?;
        writer.write_all(TOMBSTONE)?;
        writer.flush()?;
        writer.get_ref().sync_all()?; // ensure flushed to disk

        Ok(())
    }



    pub fn replay<P: AsRef<Path>>(path: P) -> std::io::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let file = match File::open(path.as_ref()) {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(vec![]),
            Err(e) => return Err(e),
            Ok(f) => f,
        };
        let mut reader = BufReader::new(file);
        let mut result = Vec::new();

        loop {
            let mut len_buf = [0u8; 4];
            // If we can't read 4 bytes for key length, break — likely clean EOF
            if let Err(e) = reader.read_exact(&mut len_buf) {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    break;
                } else {
                    return Err(e);
                }
            }

            let key_len = u32::from_le_bytes(len_buf) as usize;
            let mut key = vec![0u8; key_len];
            reader.read_exact(&mut key)?;

            reader.read_exact(&mut len_buf)?;
            let val_len = u32::from_le_bytes(len_buf) as usize;
            let mut val = vec![0u8; val_len];
            reader.read_exact(&mut val)?;

            result.push((key, val));
        }

        Ok(result)
    }

}
