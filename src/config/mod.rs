use crate::constants;
use serde::Deserialize;
use std::collections::HashMap;

#[derive(Deserialize)]
pub struct NldbConfig {
    pub max_memtable_nodes: u64,
    pub max_memtable_size: u64,
    pub compaction_rate: u64,
    pub cache_size: u64,
    pub cache_shards: u64,
    pub compaction_queue_size: u64,
    pub memtable_flush_queue_size: u64,
}

impl Default for NldbConfig {
    fn default() -> NldbConfig {
        NldbConfig {
            max_memtable_nodes: constants::DEFAULT_MAX_MEMTABLE_NODES,
            max_memtable_size: constants::DEFAULT_MAX_MEMTABLE_SIZE,
            compaction_rate: constants::DEFAULT_COMPACTION_RATE,
            cache_size: constants::DEFAULT_CACHE_SIZE,
            cache_shards: constants::get_default_cache_shards(),
            compaction_queue_size: constants::DEFAULT_COMPACTION_QUEUE_SIZE,
            memtable_flush_queue_size: constants::DEFAULT_MEMTABLE_FLUSH_QUEUE,
        }
    }
}

impl From<HashMap<String, u64>> for NldbConfig {
    fn from(parsed: HashMap<String, u64>) -> Self {
        let mut max_memtable_nodes: Option<u64> = None;
        let mut max_memtable_size: Option<u64> = None;
        let mut compaction_rate: Option<u64> = None;
        let mut cache_size: Option<u64> = None;
        let mut cache_shards: Option<u64> = None;
        let mut compaction_queue_size: Option<u64> = None;
        let mut memtable_flush_queue_size: Option<u64> = None;

        for (param, val) in parsed.into_iter() {
            let striped = param
                .strip_prefix("--")
                .expect("cli param not prefixed with --");

            match striped {
                "max_memtable_nodes" => {
                    max_memtable_nodes = Some(val);
                }
                "max_memtable_size" => {
                    max_memtable_size = Some(val);
                }
                "compaction_rate" => {
                    compaction_rate = Some(val);
                }
                "cache_size" => {
                    cache_size = Some(val);
                }
                "compaction_queue_size" => {
                    compaction_queue_size = Some(val);
                }
                "memtable_flush_queue_size" => {
                    memtable_flush_queue_size = Some(val);
                }
                "cache_shards" => cache_shards = Some(val),
                _ => unreachable!(),
            }
        }
        NldbConfig {
            max_memtable_nodes: max_memtable_nodes.unwrap_or(constants::DEFAULT_MAX_MEMTABLE_NODES),
            max_memtable_size: max_memtable_size.unwrap_or(constants::DEFAULT_MAX_MEMTABLE_SIZE),
            compaction_rate: compaction_rate.unwrap_or(constants::DEFAULT_COMPACTION_RATE),
            cache_size: cache_size.unwrap_or(constants::DEFAULT_CACHE_SIZE),
            cache_shards: cache_shards.unwrap_or(constants::get_default_cache_shards()),
            compaction_queue_size: compaction_queue_size
                .unwrap_or(constants::DEFAULT_COMPACTION_QUEUE_SIZE),
            memtable_flush_queue_size: memtable_flush_queue_size
                .unwrap_or(constants::DEFAULT_MEMTABLE_FLUSH_QUEUE),
        }
    }
}
