use super::inner::Blob;
use crate::constants;
use crate::disk::encode;
use crate::util;
use std::cmp::Ordering;

pub(crate) enum RecordType {
    Data,
    Tombstone,
}

#[derive(PartialEq, Eq, Debug)]
pub struct Record {
    pub(crate) blob: Blob,
    pub(crate) key: (usize, usize),
    pub(crate) value: Option<(usize, usize)>,
}

impl From<Record> for Blob {
    fn from(record: Record) -> Blob {
        let Record { blob, .. } = record;
        blob
    }
}

impl Record {
    pub(crate) fn new_data_record(key: Blob, value: Blob) -> Record {
        encode::encode_data_record(key, value)
    }

    pub(crate) fn new_tombstone_record(key: Blob) -> Record {
        encode::encode_tombstone_record(key)
    }

    pub(crate) fn cmp_key(&self, other: &Record) -> Ordering {
        self.key().cmp(other.key())
    }

    pub(crate) fn record_type(&self) -> RecordType {
        match self.value {
            Some(_) => RecordType::Data,
            None => RecordType::Tombstone,
        }
    }

    pub(crate) fn key(&self) -> &[u8] {
        let (offset, len) = self.key;
        &self.blob[offset..offset + len]
    }

    pub(crate) fn value(&self) -> Option<&[u8]> {
        match self.value {
            Some((offset, len)) => Some(&self.blob[offset..offset + len]),
            None => None,
        }
    }

    pub(crate) fn take_blob(self) -> Blob {
        let Self { blob, .. } = self;
        blob
    }

    pub(crate) fn copy_value(&self) -> Option<Blob> {
        match self.value {
            Some((offset, len)) => Some(self.blob[offset..offset + len].to_vec()),
            None => None,
        }
    }

    pub(crate) fn copy_key(&self) -> Blob {
        let (offset, len) = self.key;
        self.blob[offset..offset + len].to_vec()
    }

    pub(crate) fn copy_record(&self) -> Blob {
        self.blob.clone()
    }

    pub(crate) fn size(&self) -> usize {
        self.blob.len()
    }

    pub(crate) fn update_from_record(&mut self, record: &Record) {
        /*
         * 3 conditions need updating
         * data -> data
         * tombstone -> data
         * data -> tombstone
         * tombstone -> tombstone is a no-op
         *
         * */
        match record.record_type() {
            RecordType::Data => {
                self.overwrite_data(record.value().unwrap());
            }
            RecordType::Tombstone => self.update_data_to_tombstone(),
        }
    }

    fn overwrite_data(&mut self, new_value: &[u8]) {
        /*
         * 1. Figure new capacity requirement.
         * 2. resize blob, resize is no-op when no additional space is needed.
         * 3. copy all data in.
         *   - if the log_size varint length changes, key needs to be momentarily copied.
         *
         * */

        let key_len = self.key.1;
        let key_varint_len = util::varint_size_from_len(key_len);

        let (value_varint, value_varint_len) = util::encode_varint(new_value.len());
        let log_size = value_varint_len + key_varint_len + key_len + new_value.len();
        let (log_size_varint, log_size_varint_len) = util::encode_varint(log_size);

        let buffer_size = log_size_varint_len + log_size + 1_usize;

        // No-ops when additional capacity is not needed.
        self.blob.resize(buffer_size, 0_u8);
        // key start - key varint len gets to the end of the log size varint.
        // minus 1 accounts for header.
        let current_log_size_varint_len = self.key.0 - key_varint_len - 1;
        let value_varint_offset = if current_log_size_varint_len != log_size_varint_len {
            // copy key to temp buffer.
            // overwrite the new log size varint.
            // overwrite the new key.
            // update the key offsets.

            // Temp buffer to store the key.
            let temp_key_buffer = self.blob[self.key.0..self.key.0 + self.key.1].to_vec();
            let (key_varint, _) = util::encode_varint(key_len);

            let mut offset = 1_usize;
            self.blob[offset..offset + log_size_varint_len]
                .copy_from_slice(&log_size_varint[..log_size_varint_len]);
            offset += log_size_varint_len;

            self.blob[offset..offset + key_varint_len]
                .copy_from_slice(&key_varint[..key_varint_len]);
            offset += key_varint_len;

            self.blob[offset..offset + key_len].copy_from_slice(&temp_key_buffer);

            self.key = (offset, key_len);
            offset + key_len
        } else {
            self.blob[1_usize..log_size_varint_len + 1]
                .copy_from_slice(&log_size_varint[..log_size_varint_len]);
            self.key.0 + key_len
        };
        self.blob[value_varint_offset..value_varint_offset + value_varint_len]
            .copy_from_slice(&value_varint[..value_varint_len]);

        let value_offset = value_varint_offset + value_varint_len;

        self.blob[value_offset..value_offset + new_value.len()].copy_from_slice(new_value);

        // Set the header to be a data log.
        self.blob[0] = constants::DATA_LOG_HEADER;
    }

    fn update_data_to_tombstone(&mut self) {
        /*
         * The current record buffer will be large enough to fit in a buffer. However, the log size
         * in the buffer header will change. Given this is cheap, it is just recomputed here, and
         * the buffer if overwritten with the new state.
         * */
        let key = self.copy_key();
        let (key_varint, key_varint_len) = util::encode_varint(key.len());
        let log_size = key_varint_len + key.len();

        let (log_size_varint, log_size_varint_len) = util::encode_varint(log_size);

        let total_size = log_size + log_size_varint_len + 1;
        self.blob.resize(total_size, 0_u8);

        self.blob[0] = constants::TOMBSTONE_LOG_HEADER;
        let mut offset = 1_usize;
        self.blob[offset..offset + log_size_varint_len]
            .copy_from_slice(&log_size_varint[..log_size_varint_len]);
        offset += log_size_varint_len;

        self.blob[offset..offset + key_varint_len].copy_from_slice(&key_varint[..key_varint_len]);
        offset += key_varint_len;

        self.key = (offset, key.len());

        self.blob[offset..offset + key.len()].copy_from_slice(&key);
        self.value = None;
    }
}

