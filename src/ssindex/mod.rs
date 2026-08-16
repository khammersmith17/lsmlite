use crate::constants;
use crate::memtable::inner::Blob;
use crate::sstable::{bloom_filter::BloomFilter, footer::SSTableFooter};
use crate::util;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};

// Leaving here to fill out the impl later.
#[allow(unused)]
mod trie;

pub enum IndexResult {
    Range(u64, u64),
    Exact(u64),
    None,
}

#[derive(Debug)]
pub struct SstIndex {
    keys: Vec<Blob>,
    offsets: Vec<u64>,
    bloom_filter: BloomFilter,
    pub data_block_end: u64,
}

/// Read the index table in from disk.
/// The layout for each index entry is:
///     [key length (varint)][variable length key][offset (8 Big Endian u64)]
fn decode_index(buffer: &[u8], index_len: usize) -> (Vec<Blob>, Vec<u64>) {
    let mut keys: Vec<Blob> = Vec::with_capacity(index_len);
    let mut offsets: Vec<u64> = Vec::with_capacity(index_len);

    let mut offset = 0_usize;

    for _ in 0..index_len {
        let (key_len, bytes_walked) = util::decode_varint(buffer, offset);
        offset += bytes_walked;
        let key_buffer = buffer[offset..offset + key_len as usize].to_vec();
        keys.push(key_buffer);

        offset += key_len as usize;

        let offset_array = util::get_be_array8(buffer, offset);
        offsets.push(u64::from_be_bytes(offset_array));
        offset += 8_usize;
    }
    (keys, offsets)
}

impl SstIndex {
    /// Read the SSTable Index from disk, given a file descriptor.
    pub fn from_disk_sstable(fd: &mut File) -> std::io::Result<SstIndex> {
        let footer_offset = fd.metadata()?.len() - constants::FOOTER_SIZE;
        let footer = SSTableFooter::from_disk_sstable(fd, footer_offset)?;
        let mut index_table_buffer =
            vec![0_u8; (footer.bloom_filter_start - footer.index_block_start) as usize];
        fd.seek(SeekFrom::Start(footer.index_block_start))?;
        fd.read_exact(&mut index_table_buffer)?;

        let (keys, offsets) = decode_index(&index_table_buffer, footer.index_block_len as usize);

        let bloom_filter_len = footer_offset - footer.bloom_filter_start;
        let mut bloom_filter_buffer = vec![0_u8; bloom_filter_len as usize];
        fd.read_exact(&mut bloom_filter_buffer)?;

        let bloom_filter = BloomFilter::from_bytes(&bloom_filter_buffer);

        Ok(SstIndex {
            keys,
            offsets,
            bloom_filter,
            data_block_end: footer.index_block_start,
        })
    }

    /// Returns the start of the index range a key falls into, if the key is in the SSTable file on
    /// disk, otherwise None is returned.
    pub fn range_search_start(&self, key: &[u8]) -> IndexResult {
        if !self.bloom_filter.contains(key) {
            return IndexResult::None;
        }

        let range_start_idx = self.search_key(key);

        if self.keys[range_start_idx].eq(key) {
            return IndexResult::Exact(self.offsets[range_start_idx]);
        }

        if range_start_idx == self.offsets.len() - 1 {
            IndexResult::Range(self.offsets[range_start_idx], self.data_block_end)
        } else {
            IndexResult::Range(
                self.offsets[range_start_idx],
                self.offsets[range_start_idx + 1],
            )
        }
    }

    fn search_key(&self, key: &[u8]) -> usize {
        self.keys
            .partition_point(|edge| key >= edge.as_slice())
            .min(self.keys.len())
            .saturating_sub(1_usize)
    }
}
