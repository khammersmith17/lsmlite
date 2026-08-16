pub mod decode;
pub mod encode;
use crate::constants;
use crate::error::SSTableError;
use crate::memtable::inner::{Blob, NodeData};
use crate::util;
use std::cmp::Ordering;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};

/*
* Disk LOG FORMAT
* Data insert record
*   [0_u8][log size varint][key length varint][key][data length varint][data]
*
* Tombstone insert record
*   [1_u8][log size varint][key length varint][key]
* */

fn is_tombstone(header: u8) -> bool {
    header == constants::TOMBSTONE_LOG_HEADER
}

pub struct DiskRecord {
    pub key: Blob,
    pub data: NodeData,
}

impl PartialEq for DiskRecord {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl Eq for DiskRecord {}

// Sort based on Keys.
impl PartialOrd for DiskRecord {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.key.cmp(&other.key))
    }
}

impl Ord for DiskRecord {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

pub fn search_data_block(
    fd: &mut File,
    block_start: u64,
    block_end: u64,
    search_key: &[u8],
) -> Result<Blob, SSTableError> {
    let block_size = (block_end - block_start) as usize;
    fd.seek(SeekFrom::Start(block_start))?;
    let mut block_buffer = vec![0_u8; block_size];
    fd.read_exact(&mut block_buffer)?;

    let mut offset = 0_usize;

    while offset < block_size {
        let header = block_buffer[offset];
        offset += 1;

        let (log_size, bytes_walked) = util::decode_varint(&block_buffer, offset);
        offset += bytes_walked;
        let log_end = offset + log_size as usize;

        let (key_size, bytes_walked) = util::decode_varint(&block_buffer, offset);
        offset += bytes_walked;

        let key = &block_buffer[offset..offset + key_size as usize];
        if !key.eq(search_key) {
            offset = log_end;
            continue;
        }

        if is_tombstone(header) {
            return Err(SSTableError::Tombstone);
        }

        offset += key_size as usize;
        let (data_size, bytes_walked) = util::decode_varint(&block_buffer, offset);
        offset += bytes_walked;

        return Ok(block_buffer[offset..offset + data_size as usize].to_vec());
    }
    Err(SSTableError::DiskRecordNotFound)
}

pub fn read_data_block(fd: &mut File, offset: u64, key: &[u8]) -> Result<Blob, SSTableError> {
    /*
     * Seek to exact offset on disk.
     * Read in up to 4 KB into memory.
     * parse record
     * read in data blob
     * */
    let _ = fd.seek(SeekFrom::Start(offset))?;

    let mut buffer = vec![0_u8; constants::RECORD_READ_BUFFER_SIZE as usize];
    let _ = fd.read(&mut buffer)?;

    let mut buffer_offset = 0_usize;

    let header = buffer[buffer_offset];
    buffer_offset += 1;

    if is_tombstone(header) {
        return Err(SSTableError::Tombstone);
    }

    let (log_size, log_bytes_walked) = util::decode_varint(&buffer, buffer_offset);
    buffer_offset += log_bytes_walked;

    let key_varint_len = util::varint_size_from_len(key.len());
    #[cfg(debug_assertions)]
    {
        let key_start = buffer_offset + key_varint_len;
        debug_assert_eq!(&buffer[key_start..key_start + key.len()], key);
    }
    let total_key_bytes = key_varint_len + key.len();
    buffer_offset += total_key_bytes;

    if buffer_offset as u64 + (log_size - total_key_bytes as u64)
        > constants::RECORD_READ_BUFFER_SIZE
    {
        return read_oversized_record_from_disk(fd, buffer_offset, buffer);
    }

    let (blob_size, bytes_walked) = util::decode_varint(&buffer, buffer_offset);
    buffer_offset += bytes_walked;

    // TODO: try to determine a zero copy approach.
    Ok(buffer[buffer_offset..buffer_offset + blob_size as usize].to_vec())
}

/// Read the overflow when a log size exceeds 4 KB. Maintains 4KB read buffer and only requiring an
/// additional IO syscall when the log size exceeds the default read buffer size.
/// Assumes that the key and value size has already been read.
/// Determine required overflow size, and read in to a tightly packed overflow buffer.
/// Read passed the blob size, and extend with overflow buffer.
fn read_oversized_record_from_disk(
    fd: &mut File,
    mut offset: usize,
    buffer: Vec<u8>,
) -> Result<Blob, SSTableError> {
    let (blob_size, bytes_walked) = util::decode_varint(&buffer, offset);
    offset += bytes_walked;

    let first_part = &buffer[offset..];
    debug_assert!(first_part.len() < blob_size as usize);

    // Allocate a new vec, copy the current read in blob in.
    let mut result = Vec::with_capacity(blob_size as usize);
    result.extend_from_slice(first_part);

    // Fill the rest with emtpy data.
    result.resize(blob_size as usize, 0_u8);

    // Read the rest of the blob directly into the result buffer.
    let _ = fd.read_exact(&mut result[first_part.len()..])?;
    Ok(result)
}

#[cfg(test)]
mod tests {
    use crate::disk::decode::decode_disk_record;
    use crate::disk::encode::{encode_insert_record, encode_tombstone_record};
    use crate::memtable::inner::NodeData;
    use crate::util;

    fn make_insert_record(key: &[u8], data: &[u8]) -> Vec<u8> {
        let (key_varint, varint_len) = util::encode_varint(key.len());
        encode_insert_record(key, &key_varint[..varint_len], data)
    }

    fn make_tombstone_record(key: &[u8]) -> Vec<u8> {
        let (key_varint, varint_len) = util::encode_varint(key.len());
        encode_tombstone_record(key, &key_varint[..varint_len])
    }

    #[test]
    fn insert_record_roundtrip() {
        let key = b"hello";
        let data = b"world";
        let buf = make_insert_record(key, data);

        let mut offset = 0;
        let record = decode_disk_record(&buf, &mut offset).unwrap();

        assert_eq!(record.key, key);
        assert!(matches!(record.data, NodeData::Data(ref d) if d == data));
        assert_eq!(offset, buf.len());
    }

    #[test]
    fn tombstone_record_roundtrip() {
        let key = b"deleted_key";
        let buf = make_tombstone_record(key);

        let mut offset = 0;
        let record = decode_disk_record(&buf, &mut offset).unwrap();

        assert_eq!(record.key, key);
        assert!(matches!(record.data, NodeData::Tombstone));
        assert_eq!(offset, buf.len());
    }

    #[test]
    fn insert_record_empty_data() {
        let key = b"emptyval";
        let buf = make_insert_record(key, b"");

        let mut offset = 0;
        let record = decode_disk_record(&buf, &mut offset).unwrap();

        assert_eq!(record.key, key);
        assert!(matches!(record.data, NodeData::Data(ref d) if d.is_empty()));
    }

    #[test]
    fn insert_record_large_key_and_data() {
        let key = b"k".repeat(200);
        let data = vec![0xAB_u8; 500];
        let buf = make_insert_record(&key, &data);

        let mut offset = 0;
        let record = decode_disk_record(&buf, &mut offset).unwrap();

        assert_eq!(record.key, key);
        assert!(matches!(record.data, NodeData::Data(ref d) if *d == data));
        assert_eq!(offset, buf.len());
    }

    #[test]
    fn multiple_records_sequential_decode() {
        let buf1 = make_insert_record(b"alpha", b"1");
        let buf2 = make_tombstone_record(b"beta");
        let buf3 = make_insert_record(b"gamma", b"3");

        let mut combined = Vec::new();
        combined.extend_from_slice(&buf1);
        combined.extend_from_slice(&buf2);
        combined.extend_from_slice(&buf3);

        let mut offset = 0;
        let r1 = decode_disk_record(&combined, &mut offset).unwrap();
        let r2 = decode_disk_record(&combined, &mut offset).unwrap();
        let r3 = decode_disk_record(&combined, &mut offset).unwrap();

        assert_eq!(r1.key, b"alpha");
        assert!(matches!(r1.data, NodeData::Data(ref d) if d == b"1"));
        assert_eq!(r2.key, b"beta");
        assert!(matches!(r2.data, NodeData::Tombstone));
        assert_eq!(r3.key, b"gamma");
        assert!(matches!(r3.data, NodeData::Data(ref d) if d == b"3"));
        assert_eq!(offset, combined.len());
    }

    #[test]
    fn invalid_header_returns_none() {
        let buf = [0xFF_u8, 0x00];
        let mut offset = 0;
        let result = decode_disk_record(&buf, &mut offset);
        assert!(result.is_none());
    }
}
