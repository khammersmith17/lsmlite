// SSTABLE HEADER
pub const NLDB_SSTABLE_HEADER: [u8; 4] = [78, 76, 68, 66];
pub const V0_HEADER: u16 = 0_u16;
pub const HEADER_SIZE: u64 = 6_u64;

// DISK RECORD HEADERS
pub const INSERT_LOG_HEADER: u8 = 0_u8;
pub const TOMBSTONE_LOG_HEADER: u8 = 1_u8;

// DEFAULT SSTABLE CONFIGS
pub const DISK_BLOCK_SIZE: u64 = 4_000_u64; // 4 KB
pub const FOOTER_SIZE: u64 = 24_u64;
pub const DEFAULT_COMPACTION_RATE: u64 = 10_u64;
pub const DEFAULT_COMPACTION_QUEUE_SIZE: u64 = 100_u64;

// DEFAULT WAL CONFIGS
pub const DEFAULT_WAL_BUFFER_SIZE: usize = 64_000_usize; // 64 KB
pub const DEFAULT_WAL_BUFFER_CAPACITY: usize = 100_000_usize; // 100 KB
pub const DEFAULT_WAL_BUFFER_FLUSH_TIME: u128 = 400_u128; // 400 ms

// DEFAULT MEMTABLE CONFIGS
pub const DEFAULT_MAX_MEMTABLE_NODES: u64 = 4096_u64; // 4k
pub const DEFAULT_MAX_MEMTABLE_SIZE: u64 = 4_000_000_u64; // 4 MB
pub const DEFAULT_MEMTABLE_FLUSH_QUEUE: u64 = 100_u64;

// CONFIG FILE PATH
pub const NLDB_CONFIG_FILE_PATH: &str = "nldb_conf.yaml";

// TCP ADDRESS
pub const TCP_ADDRESS: &str = "127.0.0.1:7211";

// DATA CONSTRAINTS
pub const MAX_MESSAGE_SIZE: usize = (2 << 19) * 10;
pub const MAX_KEY_SIZE: usize = 1024; // 1 KB

// DEFAULT CACHE CONFIGS
use num_cpus;
use std::sync::OnceLock;

pub const DEFAULT_CACHE_SIZE: u64 = 2048_u64; // Large depends on object size.
static DEFAULT_NUM_CACHE_SHARDS: OnceLock<u64> = OnceLock::new();

pub fn get_default_cache_shards() -> u64 {
    *DEFAULT_NUM_CACHE_SHARDS.get_or_init(|| num_cpus::get() as u64)
}

// 4KB buffer size for initial read when seeking directly to offset of a record.
pub const RECORD_READ_BUFFER_SIZE: u64 = 4096;

// ERROR CODES
pub mod error_codes {
    pub const INTERNAL_ERROR: u8 = 1_u8;
    pub const INVALID_QUERY: u8 = 2_u8;
    pub const KEY_NOT_FOUND: u8 = 3_u8;
    pub const KEY_SIZE_CONSTRAINT: u8 = 4_u8;
    pub const RECORD_SIZE_CONSTRAINT: u8 = 5_u8;
}

pub const TCP_SERVER_SUCCESS: u8 = 1_u8;
pub const TCP_SERVER_ERROR: u8 = 0_u8;
