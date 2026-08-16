use super::constants;
use crate::disk::decode;
use crate::memtable::{inner::MemtableNode, record::Record};
use crate::util;
use memmap2::{Mmap, MmapOptions};
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

/*
* WAL LOG FORMAT
* Data insert record
*   [0_u8][log size varint][key length varint][key][data length varint][data]
*
* Tombstone insert record
*   [1_u8][log size varint][key length varint][key]
* */

fn unix_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
}

#[derive(Debug)]
pub struct Wal {
    fd: File,
    buffer: Vec<u8>,
    last_flush: u128,
    pub filename: PathBuf,
}

impl Wal {
    #[cfg(test)]
    pub fn from_fd_and_path(fd: File, filename: PathBuf) -> Wal {
        let buffer = Vec::with_capacity(constants::DEFAULT_WAL_BUFFER_CAPACITY);
        let last_flush = unix_ms();
        Wal {
            fd,
            last_flush,
            buffer,
            filename,
        }
    }

    pub(crate) fn new() -> std::io::Result<Wal> {
        let wal_file_name = util::generate_wal_file_name();
        let fd = File::create_new(&wal_file_name)?;
        let buffer = Vec::with_capacity(constants::DEFAULT_WAL_BUFFER_CAPACITY);
        let last_flush = unix_ms();
        Ok(Wal {
            fd,
            last_flush,
            buffer,
            filename: wal_file_name,
        })
    }

    fn needs_flush(&self) -> bool {
        let now: u128 = unix_ms();
        self.buffer.len() >= constants::DEFAULT_WAL_BUFFER_SIZE
            || now > (self.last_flush + constants::DEFAULT_WAL_BUFFER_FLUSH_TIME)
    }

    pub(crate) fn write_log(&mut self, node: &MemtableNode) {
        // Flush before buffering if needed.
        if self.needs_flush() {
            self.flush()
        }

        self.buffer.extend(node.copy_record());
    }

    #[cfg(test)]
    pub fn flush_for_test(&mut self) {
        self.flush()
    }

    fn flush(&mut self) {
        let _ = self.fd.write(&self.buffer);
        let _ = self.fd.flush();
        self.buffer.clear();
        self.last_flush = unix_ms();
    }
}

pub struct WalIterator {
    _fd: File,
    buffer: Mmap,
    offset: usize,
}

impl WalIterator {
    pub(crate) fn new(path: &Path) -> std::io::Result<WalIterator> {
        let fd = File::open(path)?;
        let mmap = unsafe { MmapOptions::new().map(&fd)? };

        Ok(WalIterator {
            _fd: fd,
            buffer: mmap,
            offset: 0_usize,
        })
    }
}

impl Iterator for WalIterator {
    type Item = Record;
    fn next(&mut self) -> Option<Self::Item> {
        if self.offset >= self.buffer.len() {
            return None;
        }

        decode::decode_disk_record(&self.buffer, &mut self.offset)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memtable::inner::MemtableNode;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    static WAL_COUNTER: AtomicU64 = AtomicU64::new(0);

    struct WalGuard(PathBuf);
    impl Drop for WalGuard {
        fn drop(&mut self) {
            let _ = fs::remove_file(&self.0);
        }
    }

    fn make_wal() -> (Wal, WalGuard) {
        let id = WAL_COUNTER.fetch_add(1, Ordering::Relaxed);
        let thread_id = std::thread::current().id();
        let path: PathBuf = format!("test_wal_mod_{thread_id:?}_{id}.log").into();
        let fd = fs::File::create(&path).unwrap();
        (Wal::from_fd_and_path(fd, path.clone()), WalGuard(path))
    }

    fn data_node(key: &[u8], value: &[u8]) -> MemtableNode {
        MemtableNode::new_for_test(key.to_vec(), value.to_vec())
    }

    fn tombstone_node(key: &[u8]) -> MemtableNode {
        MemtableNode::new_tombstone_for_test(key.to_vec())
    }

    #[test]
    fn test_data_record_roundtrip() {
        let (mut wal, guard) = make_wal();
        wal.write_log(&data_node(b"hello", b"world"));
        wal.flush_for_test();

        let records: Vec<_> = WalIterator::new(&guard.0).unwrap().collect();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].key(), b"hello".as_slice());
        assert_eq!(records[0].value(), Some(b"world".as_slice()));
    }

    #[test]
    fn test_tombstone_roundtrip() {
        let (mut wal, guard) = make_wal();
        wal.write_log(&tombstone_node(b"gone"));
        wal.flush_for_test();

        let records: Vec<_> = WalIterator::new(&guard.0).unwrap().collect();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].key(), b"gone".as_slice());
        assert!(records[0].value().is_none());
    }

    #[test]
    fn test_multiple_records_in_order() {
        let (mut wal, guard) = make_wal();
        for i in 0..5u8 {
            wal.write_log(&data_node(format!("k{i:?}").as_bytes(), &[i]));
        }
        wal.flush_for_test();

        let records: Vec<_> = WalIterator::new(&guard.0).unwrap().collect();
        assert_eq!(records.len(), 5);
        for i in 0..5u8 {
            assert_eq!(records[i as usize].key(), format!("k{i:?}").as_bytes());
            assert_eq!(records[i as usize].value(), Some([i].as_slice()));
        }
    }

    #[test]
    fn test_mixed_data_and_tombstones() {
        let (mut wal, guard) = make_wal();
        wal.write_log(&data_node(b"a", b"v1"));
        wal.write_log(&tombstone_node(b"b"));
        wal.write_log(&data_node(b"c", b"v3"));
        wal.flush_for_test();

        let records: Vec<_> = WalIterator::new(&guard.0).unwrap().collect();
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].value(), Some(b"v1".as_slice()));
        assert!(records[1].value().is_none());
        assert_eq!(records[2].value(), Some(b"v3".as_slice()));
    }

    #[test]
    fn test_buffer_not_written_before_flush() {
        let (mut wal, guard) = make_wal();
        wal.write_log(&data_node(b"k", b"v"));

        // Nothing flushed yet — file should be empty.
        let file_len = fs::metadata(&guard.0).unwrap().len();
        assert_eq!(file_len, 0);

        wal.flush_for_test();
        let file_len = fs::metadata(&guard.0).unwrap().len();
        assert!(file_len > 0);
    }

    #[test]
    fn test_size_based_flush_trigger() {
        let (mut wal, guard) = make_wal();
        // First write fills buffer to ~= DEFAULT_WAL_BUFFER_SIZE.
        let big_value = vec![0u8; constants::DEFAULT_WAL_BUFFER_SIZE];
        wal.write_log(&data_node(b"k1", &big_value));
        // Second write: needs_flush() fires, flushes the first record to disk.
        wal.write_log(&data_node(b"k2", b"v"));

        let file_len = fs::metadata(&guard.0).unwrap().len();
        assert!(file_len > 0);
    }
}
