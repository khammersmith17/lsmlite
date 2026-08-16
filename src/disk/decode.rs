use super::DiskRecord;
use crate::constants;
use crate::memtable::inner::NodeData;
use crate::util;

pub fn decode_disk_record(buffer: &[u8], offset: &mut usize) -> Option<DiskRecord> {
    let header = buffer[*offset];
    *offset += 1;

    match header {
        constants::INSERT_LOG_HEADER => Some(decode_insert_record(buffer, offset)),
        constants::TOMBSTONE_LOG_HEADER => Some(decode_tombstone_record(buffer, offset)),
        _ => None,
    }
}

fn decode_insert_record(buffer: &[u8], offset: &mut usize) -> DiskRecord {
    let (log_size, bytes_walked) = util::decode_varint(buffer, *offset);
    *offset += bytes_walked;

    let start = *offset;

    let (key_length, bytes_walked) = util::decode_varint(buffer, *offset);
    *offset += bytes_walked;

    let key = buffer[*offset..*offset + key_length as usize].to_vec();
    *offset += key_length as usize;

    let (data_len, bytes_walked) = util::decode_varint(buffer, *offset);
    *offset += bytes_walked;

    let data = buffer[*offset..*offset + data_len as usize].to_vec();
    *offset += data_len as usize;

    debug_assert_eq!(log_size as usize, *offset - start);

    DiskRecord {
        key,
        data: NodeData::Data(data),
    }
}

fn decode_tombstone_record(buffer: &[u8], offset: &mut usize) -> DiskRecord {
    let (log_size, bytes_walked) = util::decode_varint(buffer, *offset);
    *offset += bytes_walked;

    let start = *offset;

    let (key_length, bytes_walked) = util::decode_varint(buffer, *offset);
    *offset += bytes_walked;

    let key = buffer[*offset..*offset + key_length as usize].to_vec();
    *offset += key_length as usize;

    debug_assert_eq!(log_size as usize, *offset - start);

    DiskRecord {
        key,
        data: NodeData::Tombstone,
    }
}
