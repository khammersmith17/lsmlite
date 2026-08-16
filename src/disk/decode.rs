use crate::constants;
use crate::memtable::record::Record;
use crate::util;

pub fn decode_disk_record(buffer: &[u8], offset: &mut usize) -> Option<Record> {
    let header = buffer[*offset];
    *offset += 1;

    match header {
        constants::DATA_LOG_HEADER => Some(decode_insert_record(buffer, offset)),
        constants::TOMBSTONE_LOG_HEADER => Some(decode_tombstone_record(buffer, offset)),
        _ => None,
    }
}

fn decode_insert_record(buffer: &[u8], offset: &mut usize) -> Record {
    // Back one to account for the header that has already been parsed.
    let start_offset = *offset - 1;
    let (log_size, bytes_walked) = util::decode_varint(buffer, *offset);
    *offset += bytes_walked;

    let start = *offset;

    let (key_length, bytes_walked) = util::decode_varint(buffer, *offset);
    *offset += bytes_walked;

    let key = ((*offset - start_offset) as usize, key_length as usize);
    *offset += key_length as usize;

    let (data_len, bytes_walked) = util::decode_varint(buffer, *offset);
    *offset += bytes_walked;

    let value = Some(((*offset - start_offset) as usize, data_len as usize));
    *offset += data_len as usize;

    debug_assert_eq!(log_size as usize, *offset - start);
    // Step back
    let blob = buffer[start_offset..*offset].to_vec();

    Record { blob, key, value }
}

fn decode_tombstone_record(buffer: &[u8], offset: &mut usize) -> Record {
    // Back one to account for the header that has already been parsed.
    let start_offset = *offset - 1;
    let (log_size, bytes_walked) = util::decode_varint(buffer, *offset);
    *offset += bytes_walked;

    let start = *offset;

    let (key_length, bytes_walked) = util::decode_varint(buffer, *offset);
    *offset += bytes_walked;

    let key = ((*offset - start_offset) as usize, key_length as usize);
    *offset += key_length as usize;

    debug_assert_eq!(log_size as usize, *offset - start);

    let blob = buffer[start_offset..*offset].to_vec();

    Record {
        blob,
        key,
        value: None,
    }
}
