use crate::constants;
use crate::memtable::inner::Blob;
use crate::sstable::{bloom_filter::BloomFilter, footer::SSTableFooter};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
pub(crate) mod trie;
use trie::Trie;

#[derive(Debug)]
pub struct SstIndex {
    ends: (Blob, Blob),
    trie: Trie,
    bloom_filter: BloomFilter,
    pub(crate) data_block_end: u64,
}

impl SstIndex {
    /// Read the SSTable Index from disk, given a file descriptor.
    pub(crate) fn from_disk_sstable(fd: &mut File) -> std::io::Result<SstIndex> {
        let footer_offset = fd.metadata()?.len() - constants::FOOTER_SIZE;
        let footer = SSTableFooter::from_disk_sstable(fd, footer_offset)?;
        let mut index_table_buffer =
            vec![0_u8; (footer.bloom_filter_start - footer.index_block_start) as usize];
        fd.seek(SeekFrom::Start(footer.index_block_start))?;
        fd.read_exact(&mut index_table_buffer)?;

        let trie = Trie::deserialize_from_disk(&index_table_buffer);
        let bloom_filter_len = footer_offset - footer.bloom_filter_start;
        let mut bloom_filter_buffer = vec![0_u8; bloom_filter_len as usize];
        fd.read_exact(&mut bloom_filter_buffer)?;

        let bloom_filter = BloomFilter::from_bytes(&bloom_filter_buffer);
        let ends = (trie.min_key(), trie.max_key());

        Ok(SstIndex {
            ends,
            trie,
            bloom_filter,
            data_block_end: footer.index_block_start,
        })
    }

    pub(crate) fn min_key(&self) -> &[u8] {
        &self.ends.0
    }

    pub(crate) fn max_key(&self) -> &[u8] {
        &self.ends.1
    }

    /// Get offset to the start of a log for a particular key.
    /// Returns None early bloom filter check.
    ///
    /// If bloom filter check determines key existance, go down to the trie.
    pub(crate) fn get_offset_for_key(&self, key: &[u8]) -> Option<u64> {
        if !self.bloom_filter.contains(key) {
            return None;
        }

        self.trie.get(key)
    }
}
