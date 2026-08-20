use crate::constants;
use crate::memtable::{inner::Blob, record::Record};
use crate::util;

pub(crate) fn encode_data_record(key: Blob, value: Blob) -> Record {
    let (key_varint, key_varint_len) = util::encode_varint(key.len());
    let (value_varint, value_varint_len) = util::encode_varint(value.len());
    let log_size = key_varint_len + value_varint_len + key.len() + value.len();
    let (log_size_varint, log_size_varint_len) = util::encode_varint(log_size);
    let buffer_size = log_size_varint_len + log_size + 1_usize; // +1 for header
    let mut record_buffer = vec![0_u8; buffer_size];

    record_buffer[0] = constants::DATA_LOG_HEADER;
    let mut offset = 1_usize;
    record_buffer[offset..offset + log_size_varint_len]
        .copy_from_slice(&log_size_varint[..log_size_varint_len]);
    offset += log_size_varint_len;

    record_buffer[offset..offset + key_varint_len].copy_from_slice(&key_varint[..key_varint_len]);
    offset += key_varint_len;

    let key_offset = (offset, key.len());

    record_buffer[offset..offset + key.len()].copy_from_slice(&key);
    offset += key.len();

    record_buffer[offset..offset + value_varint_len]
        .copy_from_slice(&value_varint[..value_varint_len]);
    offset += value_varint_len;

    let value_offset = Some((offset, value.len()));

    record_buffer[offset..offset + value.len()].copy_from_slice(&value);
    offset += value.len();

    debug_assert_eq!(offset, record_buffer.len());

    Record {
        blob: record_buffer,
        key: key_offset,
        value: value_offset,
    }
}

pub(crate) fn encode_tombstone_record(key: Blob) -> Record {
    let (key_varint, key_varint_len) = util::encode_varint(key.len());

    let log_size = key_varint_len + key.len();
    let (log_size_varint, log_size_varint_len) = util::encode_varint(log_size);
    let buffer_size = log_size_varint_len + log_size + 1;

    let mut record_buffer = vec![0_u8; buffer_size];
    record_buffer[0] = constants::TOMBSTONE_LOG_HEADER;

    let mut offset = 1_usize;
    record_buffer[offset..offset + log_size_varint_len]
        .copy_from_slice(&log_size_varint[..log_size_varint_len]);
    offset += log_size_varint_len;

    record_buffer[offset..offset + key_varint_len].copy_from_slice(&key_varint[..key_varint_len]);
    offset += key_varint_len;

    let key_offset = (offset, key.len());

    record_buffer[offset..offset + key.len()].copy_from_slice(&key);

    Record {
        blob: record_buffer,
        key: key_offset,
        value: None,
    }
}
