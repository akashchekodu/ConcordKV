pub mod wal;
pub mod sstable;
pub mod memtable;
pub mod compactor; // ✅ Declare the compactor module
pub mod engine;


// Re-exports
pub use wal::WriteAheadLog;
pub use memtable::MemTable;
pub use sstable::{flush_to_sstable, load_sstable};
pub use compactor::Compactor; // ✅ Optional: re-export if needed in lib.rs
pub use self::memtable::TOMBSTONE; // ✅ Add this line
