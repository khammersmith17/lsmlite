use crate::util;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};

pub struct SSTableFooter {
    pub index_block_start: u64,
    pub index_block_len: u64,
    pub bloom_filter_start: u64,
}

impl SSTableFooter {
    pub fn from_disk_sstable(fd: &mut File, footer_offset: u64) -> std::io::Result<SSTableFooter> {
        fd.seek(SeekFrom::Start(footer_offset))?;
        let mut footer_buffer = vec![0_u8; 3 * 8];
        fd.read_exact(&mut footer_buffer)?;
        let index_block_start_array = util::get_be_array8(&footer_buffer, 0);
        let index_block_start = u64::from_be_bytes(index_block_start_array);

        let index_block_len_array = util::get_be_array8(&footer_buffer, 8);
        let index_block_len = u64::from_be_bytes(index_block_len_array);

        let bloom_filter_start_array = util::get_be_array8(&footer_buffer, 16);
        let bloom_filter_start = u64::from_be_bytes(bloom_filter_start_array);

        Ok(SSTableFooter {
            index_block_start,
            index_block_len,
            bloom_filter_start,
        })
    }
}
