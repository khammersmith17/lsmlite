pub mod bloom_filter;
pub mod compaction_writer;
pub mod encode;
pub mod footer;
pub mod iterator;
use crate::constants;
use crate::disk;
use crate::error::SSTableError;
use crate::memtable::inner::Blob;
use crate::restart::SSTableArtifact;
use crate::ssindex::SstIndex;
use crate::util;
use std::collections::VecDeque;
use std::fs::File;
use std::io::Read;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
#[allow(unused)]
pub(crate) mod level_cache;

#[derive(Clone, Debug)]
pub struct SSTableCache {
    cache: Arc<RwLock<VecDeque<Arc<Mutex<SSTable>>>>>,
    pub compaction_rate: usize,
}

impl SSTableCache {
    pub fn new(compaction_rate: usize) -> SSTableCache {
        let cache = Arc::new(RwLock::new(VecDeque::new()));
        SSTableCache {
            cache,
            compaction_rate,
        }
    }

    /// Read in SSTables from disk on restart. Tables are sorted from newest to oldest by `[crate::restart]` module.
    /// They are then read in order and deserialized.
    pub fn from_restart(disk_tables: Vec<SSTableArtifact>, compaction_rate: usize) -> SSTableCache {
        let mut inner_cache: VecDeque<Arc<Mutex<SSTable>>> =
            VecDeque::with_capacity(disk_tables.len());

        for table_meta in disk_tables.into_iter() {
            let SSTableArtifact { filename, .. } = table_meta;
            let read_table: SSTable =
                SSTable::from_path(filename).expect("Unable to read SSTable on restart");
            let wrapped_table: Arc<Mutex<SSTable>> = Arc::new(Mutex::new(read_table));
            inner_cache.push_back(wrapped_table);
        }

        let cache = Arc::new(RwLock::new(inner_cache));

        SSTableCache {
            cache,
            compaction_rate,
        }
    }

    pub async fn search(&self, key: &[u8]) -> Result<Blob, SSTableError> {
        let handle = self.cache.read().await;
        let num_tables = handle.len();

        for i in 0..num_tables {
            let search_result = {
                let table_handle = handle[i].lock().await;
                table_handle.search(key).await
            };

            if search_result.is_ok() {
                return search_result;
            }

            match search_result {
                Err(SSTableError::Tombstone) => return Err(SSTableError::DiskRecordNotFound),
                _ => continue,
            }
        }
        Err(SSTableError::DiskRecordNotFound)
    }

    pub async fn push(&self, table: SSTable) -> usize {
        let wrapped_table = Arc::new(Mutex::new(table));
        let mut handle = self.cache.write().await;
        handle.push_front(wrapped_table);
        handle.len()
    }

    pub async fn replace_with_compact_table(&self, new_table: SSTable) {
        let mut cache_handle = self.cache.write().await;
        cache_handle.clear();
        cache_handle.push_front(Arc::new(Mutex::new(new_table)));
    }

    pub async fn clone_tables(&self) -> Vec<Arc<Mutex<SSTable>>> {
        let handle = self.cache.read().await;
        handle.iter().cloned().collect()
    }
}

fn validate_buffer_and_get_version(fd: &mut File) -> Result<u16, SSTableError> {
    let mut header_buffer = vec![0_u8; 7];
    fd.read_exact(&mut header_buffer)?;
    if header_buffer != constants::LSMLITE_SSTABLE_HEADER {
        return Err(SSTableError::InvalidSSTableFile);
    }

    let mut version_buffer = vec![0_u8; 2];
    fd.read_exact(&mut version_buffer)?;
    let version_arr = util::get_be_array2(version_buffer);
    Ok(u16::from_be_bytes(version_arr))
}

#[derive(Debug)]
pub struct SSTable {
    pub index: SstIndex,
    pub fd: Arc<Mutex<File>>,
    // version for when SSTable file format changes.
    // Not used for now.
    #[allow(unused)]
    version: u16,
    pub(crate) size: u64,
}

impl SSTable {
    pub fn from_path(file_name: PathBuf) -> Result<SSTable, SSTableError> {
        let fd = File::open(file_name)?;
        Self::from_fd(fd)
    }

    pub fn from_fd(mut fd: File) -> Result<SSTable, SSTableError> {
        let version = validate_buffer_and_get_version(&mut fd)?;
        let index = SstIndex::from_disk_sstable(&mut fd)?;
        let size = fd.metadata()?.len();
        let fd = Arc::new(Mutex::new(fd));
        Ok(SSTable {
            index,
            fd,
            version,
            size,
        })
    }

    pub fn min_key(&self) -> &[u8] {
        self.index.min_key()
    }

    pub fn max_key(&self) -> &[u8] {
        self.index.max_key()
    }

    pub(crate) async fn search(&self, key: &[u8]) -> Result<Blob, SSTableError> {
        let Some(offset) = self.index.get_offset_for_key(key) else {
            return Err(SSTableError::DiskRecordNotFound);
        };

        let mut fd = self.fd.lock().await;
        disk::read_data_block(&mut fd, offset, key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memtable::inner::MemtableInner;
    use crate::sstable::encode::write_sstable;
    use std::fs;

    fn make_memtable_with_records(records: &[(&[u8], &[u8])]) -> (MemtableInner, PathBuf) {
        let (mut table, wal_path) = MemtableInner::new_for_test();
        for (key, data) in records {
            table
                .insert_data_record(key.to_vec(), data.to_vec())
                .unwrap();
        }
        (table, wal_path)
    }

    #[tokio::test]
    async fn roundtrip_search() {
        let records: Vec<(&'static [u8], &'static [u8])> = vec![
            (b"apple", b"fruit"),
            (b"banana", b"yellow"),
            (b"cherry", b"red"),
            (b"date", b"sweet"),
            (b"elderberry", b"dark"),
        ];

        let sstable_path: PathBuf =
            format!("test_roundtrip_{:?}.sstable", std::thread::current().id()).into();

        let (table, wal_path) = make_memtable_with_records(&records);
        {
            let mut fd = fs::File::create(&sstable_path).unwrap();
            write_sstable(&table, &mut fd).unwrap();
        }

        let _ = fs::remove_file(&wal_path);

        let sstable = SSTable::from_path(sstable_path.clone()).unwrap();

        for (key, expected_data) in &records {
            let result = sstable.search(key).await.unwrap();
            assert_eq!(result, *expected_data, "data mismatch for key {:?}", key);
        }

        assert!(matches!(
            sstable.search(b"notakey").await,
            Err(SSTableError::DiskRecordNotFound)
        ));

        let _ = fs::remove_file(&sstable_path);
    }
}
