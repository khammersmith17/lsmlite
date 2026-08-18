use super::SSTable;
use crate::memtable::inner::Blob;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct LevelZero {
    // Never resize
    level: Arc<RwLock<VecDeque<SSTable>>>,
}

/*
* Consider:
* should the level table be a VecDeque?
* When a new SSTable comes down from being compacted above, it likely will merge with some tables
* in the middle. Though the vec will be ~small, the entries will be expensive to copy and move
* over, since they store both the trie and the bloom filter.
*
* */

struct Level {
    table: Arc<RwLock<Vec<SSTable>>>,
}

pub(super) struct LevelCache {
    tables: RwLock<Vec<SSTable>>,
    // what level in the SSTableCache this is.
    // This informs the heuristic for if a key is in the SSTable file.
    level: u8,
}
