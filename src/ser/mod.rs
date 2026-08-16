use crate::constants::{self, error_codes};
use crate::error::NldbError;
use crate::memtable::inner::Blob;
use crate::util;
use bytes::{BufMut, BytesMut};
/*
* 1 byte success flag
* 0x01 - success
* 0x00 - not success
*
* ERROR CODES:
*  0x01 - Internal Error.
*  0x02 - Invalid Query.
*  0x03 - Key not found.
*  0x04 - Key size constraint.
*  0x05 - Record size constriant.
*
* On all paths:
*   A successful operation starts with a 0x01 byte.
*   An unsuccessful operation starts with a 0x00 byte. Then an error code, as listed above.
*
* Per operation success path:
* GET
*   When key is found:
*       [0x01][blob len varint][blob]
*   When key is not found:
*       [0x00][0x01]
*
* INSERT
*   [0x01]
*
* DELETE
*   [0x01]
*
* */

/// On delete, construct the error message compliant to the above protocol.
pub fn construct_error_message(e: NldbError, buffer: &mut BytesMut) {
    buffer.clear();
    buffer.put_u8(constants::TCP_SERVER_ERROR);
    let error_code: u8 = match e {
        NldbError::InvalidQuery => error_codes::INVALID_QUERY,
        NldbError::RecordSizeConstraint => error_codes::RECORD_SIZE_CONSTRAINT,
        NldbError::KeySizeConstraint => error_codes::KEY_SIZE_CONSTRAINT,
        _ => error_codes::INTERNAL_ERROR,
    };
    buffer.put_u8(error_code);
}

pub fn insert_response(buffer: &mut BytesMut) {
    buffer.clear();
    buffer.put_slice(&[constants::TCP_SERVER_SUCCESS]);
}

pub fn delete_response(buffer: &mut BytesMut) {
    buffer.clear();
    buffer.put_slice(&[constants::TCP_SERVER_SUCCESS]);
}

pub fn serialize_get_response(blob_opt: Option<Blob>, buffer: &mut BytesMut) {
    buffer.clear();
    if let Some(blob) = blob_opt {
        let (varint, size) = util::encode_varint(blob.len());
        buffer.put_u8(constants::TCP_SERVER_SUCCESS);
        buffer.put_slice(&varint[..size]);
        buffer.put_slice(&blob);
    } else {
        buffer.put_slice(&[constants::TCP_SERVER_ERROR, error_codes::KEY_NOT_FOUND]);
    }
}
