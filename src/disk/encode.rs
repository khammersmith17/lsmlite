use crate::constants;
use crate::memtable::inner::{MemtableNode, NodeData};
use crate::util;

pub fn encode_memtable_node(node: &MemtableNode) -> Vec<u8> {
    let node_key = &node.key;
    let (key_varint, varint_len) = util::encode_varint(node_key.len());
    match node.data {
        NodeData::Data(ref data) => encode_insert_record(node_key, &key_varint[..varint_len], data),
        NodeData::Tombstone => encode_tombstone_record(node_key, &key_varint[..varint_len]),
    }
}

pub fn merge_encode_record(record: crate::disk::DiskRecord) -> Vec<u8> {
    let key = &record.key;
    let (key_varint, varint_len) = util::encode_varint(key.len());

    match record.data {
        NodeData::Data(ref data) => encode_insert_record(key, &key_varint[..varint_len], data),
        NodeData::Tombstone => unreachable!("Expected data record, got a tombstone record"),
    }
}

pub fn encode_tombstone_record(key: &[u8], key_varint: &[u8]) -> Vec<u8> {
    // Define log buffer size.
    let log_size = key.len() + key_varint.len();
    let (log_len_varint, log_varint_len) = util::encode_varint(log_size);
    let buffer_size = log_varint_len + log_size + 1;

    let mut buffer = vec![0_u8; buffer_size];
    // Write header + log length varint.
    buffer[0] = constants::TOMBSTONE_LOG_HEADER;
    buffer[1..1 + log_varint_len].copy_from_slice(&log_len_varint[..log_varint_len]);

    // Write key length varint.
    let key_varint_start = 1 + log_varint_len;
    buffer[key_varint_start..key_varint_start + key_varint.len()].copy_from_slice(key_varint);

    // Write key.
    let key_start = key_varint_start + key_varint.len();
    buffer[key_start..].copy_from_slice(key);
    buffer
}

pub fn encode_insert_record(key: &[u8], key_varint: &[u8], data: &[u8]) -> Vec<u8> {
    // Define log buffer size.
    let (data_len_varint, data_varint_len) = util::encode_varint(data.len());
    let log_size = key.len() + key_varint.len() + data_varint_len + data.len();
    let (log_len_varint, log_varint_len) = util::encode_varint(log_size);
    let buffer_size = log_varint_len + log_size + 1;

    let mut buffer = vec![0_u8; buffer_size];
    // Write header + log length varint.
    buffer[0] = constants::INSERT_LOG_HEADER;
    buffer[1..1 + log_varint_len].copy_from_slice(&log_len_varint[..log_varint_len]);

    // Write key varint.
    let key_varint_start = 1 + log_varint_len;
    buffer[key_varint_start..key_varint_start + key_varint.len()].copy_from_slice(key_varint);

    // Write key.
    let key_start = key_varint_start + key_varint.len();
    buffer[key_start..key_start + key.len()].copy_from_slice(key);

    // Write data varint.
    let data_varint_start = key_start + key.len();
    buffer[data_varint_start..data_varint_start + data_varint_len]
        .copy_from_slice(&data_len_varint[..data_varint_len]);

    // Write data.
    let data_start = data_varint_start + data_varint_len;
    buffer[data_start..].copy_from_slice(data);
    buffer
}
