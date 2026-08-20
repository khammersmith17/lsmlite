use super::SSTable;
use crate::error::SSTableError;
use crate::memtable::inner::Blob;
use std::cmp::Ordering;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::sync::RwLock;

pub struct LevelZero {
    // Never resize
    level: Arc<RwLock<Vec<SSTable>>>,
    capacity: usize,
}

impl LevelZero {
    pub(crate) async fn get(&self, key: &[u8]) -> Result<Blob, SSTableError> {
        let level = self.level.read().await;
        for sst in &*level {
            if let Ok(value) = sst.search(key).await {
                return Ok(value);
            }
        }
        Err(SSTableError::DiskRecordNotFound)
    }
}

/*
* Consider:
* should the level table be a VecDeque?
* When a new SSTable comes down from being compacted above, it likely will merge with some tables
* in the middle. Though the vec will be ~small, the entries will be expensive to copy and move
* over, since they store both the trie and the bloom filter.
*
* */

fn candidate_table_search_fn<'a>(key: &'a [u8]) -> Box<dyn Fn(&SSTable) -> Ordering + 'a> {
    Box::new(|table: &SSTable| {
        let cmp_min = table.min_key().cmp(key);
        let cmp_max = table.max_key().cmp(key);

        match (cmp_min, cmp_max) {
            (Ordering::Greater, Ordering::Less) => Ordering::Equal,
            (Ordering::Less, _) => Ordering::Less,
            _ => Ordering::Greater,
        }
    })
}

fn binary_search_for_candidate_sstable(key: &[u8], level: &[SSTable]) -> Option<usize> {
    let Ok(c) = level.binary_search_by(candidate_table_search_fn(key)) else {
        return None;
    };
    Some(c)
}

struct CacheLevel {
    table: Arc<RwLock<Vec<SSTable>>>,
    // What level in the SSTableCache this is.
    level: u8,
    // stores the min key compacted to perform round robin SSTable eviction at this level.
    compaction_key: Blob,
    max_size: u64,
    current_size: u64,
}

impl CacheLevel {
    fn needs_compaction(&self) -> bool {
        self.current_size >= self.max_size
    }

    pub(crate) async fn get(&self, key: &[u8]) -> Result<Blob, SSTableError> {
        let handle = self.table.read().await;
        let Some(candidate_table) = binary_search_for_candidate_sstable(key, &*handle) else {
            return Err(SSTableError::DiskRecordNotFound);
        };
        handle[candidate_table].search(key).await
    }
}

pub(super) struct SSTableLevelCache {
    zero: LevelZero,
    tables: Arc<RwLock<Vec<CacheLevel>>>,
}

impl SSTableLevelCache {
    pub(crate) async fn level_needs_compaction(&self, level: usize) -> bool {
        let handle = self.tables.read().await;
        if level >= handle.len() {
            return false;
        }
        handle[level].needs_compaction()
    }

    pub(crate) async fn get(&self, key: &[u8]) -> Result<Blob, SSTableError> {
        // Check level 0 first
        if let Ok(value) = self.zero.get(key).await {
            return Ok(value);
        }

        let rest = self.tables.read().await;

        for level in &*rest {
            if let Ok(value) = level.get(key).await {
                return Ok(value);
            }
        }

        Err(SSTableError::DiskRecordNotFound)
    }
}
