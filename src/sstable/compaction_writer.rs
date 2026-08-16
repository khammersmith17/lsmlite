use crate::constants;
use crate::disk::{DiskRecord, encode};
use crate::memtable::inner::Blob;
use crate::sstable::{
    bloom_filter::BloomFilter,
    encode::{encode_footer, encode_index_block},
};
use crate::util;
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;

fn key_from_record_buffer(buffer: &[u8]) -> Blob {
    // Get log size.
    let (_, bytes_walked) = util::decode_varint(buffer, 1_usize);
    // Walk past log size varint.
    let offset = bytes_walked + 1;

    // Get key len.
    let (key_len, bytes_walked) = util::decode_varint(buffer, offset);
    // Get key start.
    let key_start = offset + bytes_walked;
    let key_end = key_start + key_len as usize;

    // Parse string.
    buffer[key_start..key_end].to_vec()
}

pub struct CompactionWriter {
    fd: File,
    filename: PathBuf,
    index: Vec<(Blob, u64)>,
    disk_offset: u64,
    write_buffer: Vec<u8>,
    bloom_filter: BloomFilter,
}

impl CompactionWriter {
    pub fn new() -> std::io::Result<CompactionWriter> {
        let filename = util::generate_sstable_file_name();
        let mut fd = File::create(&filename)?;

        let _ = fd.write(&constants::NLDB_SSTABLE_HEADER)?;
        let _ = fd.write(&constants::V0_HEADER.to_be_bytes())?;

        let index = Vec::with_capacity(1024);
        let write_buffer = Vec::with_capacity(4096);

        let bloom_filter = BloomFilter::new(1024);
        Ok(CompactionWriter {
            fd,
            filename,
            index,
            disk_offset: constants::HEADER_SIZE,
            write_buffer,
            bloom_filter,
        })
    }

    /// Push a data record into the SSTable file.
    /// Expects a data record, not a tombstone record.
    pub fn push(&mut self, record: DiskRecord) -> std::io::Result<()> {
        self.bloom_filter.insert(&record.key);
        let encoded_record = encode::merge_encode_record(record);
        let record_size = encoded_record.len();

        let before = self.disk_offset % constants::DISK_BLOCK_SIZE;
        let after = (self.disk_offset + record_size as u64) % constants::DISK_BLOCK_SIZE;

        if before > after || self.disk_offset == constants::HEADER_SIZE {
            let key = key_from_record_buffer(&encoded_record);
            self.index.push((key, self.disk_offset));
        }

        if self.write_buffer.len() + record_size >= self.write_buffer.capacity() {
            let _ = self.fd.write(&self.write_buffer)?;
            self.write_buffer.clear();
        }

        self.write_buffer.extend(encoded_record);
        self.disk_offset += record_size as u64;

        Ok(())
    }

    /// Flush buffer, then write index block, and bloom filter.
    pub fn finish(mut self) -> std::io::Result<PathBuf> {
        let _ = self.fd.write(&self.write_buffer)?;

        let index_block_start = self.disk_offset;
        let index_block_len = self.index.len();
        let index_block_buffer = encode_index_block(self.index);
        let bloom_filter_start = index_block_start + index_block_buffer.len() as u64;
        let _ = self.fd.write(&index_block_buffer)?;

        let bloom_filter_buffer = self.bloom_filter.serialize();
        let _ = self.fd.write(&bloom_filter_buffer)?;

        let footer = encode_footer(
            index_block_start,
            index_block_len as u64,
            bloom_filter_start,
        );
        let _ = self.fd.write(&footer)?;
        self.fd.flush()?;
        Ok(self.filename)
    }
}
