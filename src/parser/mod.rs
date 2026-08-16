use crate::constants;
use crate::error::NldbError;
use crate::memtable::inner::Blob;
use crate::util;

/*
* All incoming statements will have a 4 byte Big Endian encoded size.
* Statements are intentionally simple.
*
* 4 byte length header does not include the size. The parser here expects the 4 byte header not to
*   be present. The 4 byte header should not be included in the buffer when reading from the TCP
*   Stream.
*
* [4 bytes]GET <key len varint><key>
* [4 bytes]INSERT <key len varint><key><value len varint><value>
* [4 bytes]DELETE <key len varint><key>
* TODO: AUTH <username len varint><username><password len varint><password>
* */

// Normalize all statements to all upper case.
fn normalize_statement(typeh: &mut [u8]) {
    for b in typeh.iter_mut() {
        if *b >= 97_u8 && *b <= 122 {
            *b = (*b - 97_u8) + 65_u8;
        }
    }
}

fn parse_to_space(buffer: &[u8], offset: &mut usize) {
    let buffer_size = buffer.len();
    while *offset < buffer_size && buffer[*offset] != b' ' {
        *offset += 1;
    }
}

fn parse_key(buffer: &[u8], offset: usize) -> Result<(Blob, usize), NldbError> {
    let (key_len, bytes_walked) = util::decode_untrusted_varint(buffer, offset)?;
    let key_start = offset + bytes_walked;
    let key_end = key_start + key_len as usize;

    // Keys should not be 0 len.
    if key_len == 0 {
        return Err(NldbError::InvalidQuery);
    }

    if key_end > buffer.len() {
        return Err(NldbError::InvalidQuery);
    }

    let key = buffer[key_start..key_end].to_vec();
    Ok((key, key_end))
}

fn validate_data(key: &[u8], value_opt: Option<&[u8]>) -> Result<(), NldbError> {
    if key.len() > constants::MAX_KEY_SIZE {
        return Err(NldbError::KeySizeConstraint);
    }

    let Some(value) = value_opt else {
        return Ok(());
    };

    if key.len() + value.len() > constants::MAX_MESSAGE_SIZE {
        return Err(NldbError::RecordSizeConstraint);
    }

    Ok(())
}

enum RequestType {
    Get,
    Delete,
    Insert,
}

impl RequestType {
    fn parse(typeh: &mut [u8]) -> Result<RequestType, NldbError> {
        normalize_statement(typeh);
        match &*typeh {
            b"GET" => Ok(RequestType::Get),
            b"INSERT" => Ok(RequestType::Insert),
            b"DELETE" => Ok(RequestType::Delete),
            _ => Err(NldbError::InvalidQuery),
        }
    }
}

pub enum NldbRequest {
    Get { key: Blob },
    Delete { key: Blob },
    Insert { key: Blob, value: Blob },
}

impl NldbRequest {
    pub fn parse(mut buffer: Vec<u8>) -> Result<NldbRequest, NldbError> {
        let buffer_size = buffer.len();

        let mut offset = 0_usize;
        parse_to_space(&buffer, &mut offset);

        if offset >= buffer_size {
            return Err(NldbError::InvalidQuery);
        }

        let statement_type = RequestType::parse(&mut buffer[..offset])?;
        offset += 1;

        match statement_type {
            RequestType::Get => Self::parse_get(buffer, offset),
            RequestType::Insert => Self::parse_insert(buffer, offset),
            RequestType::Delete => Self::parse_delete(buffer, offset),
        }
    }

    fn parse_get(buffer: Vec<u8>, offset: usize) -> Result<NldbRequest, NldbError> {
        let (key, _) = parse_key(&buffer, offset)?;
        validate_data(&key, None)?;
        Ok(NldbRequest::Get { key })
    }

    fn parse_insert(buffer: Vec<u8>, offset: usize) -> Result<NldbRequest, NldbError> {
        let (key, offset) = parse_key(&buffer, offset)?;

        let (value_len, bytes_walked) = util::decode_untrusted_varint(&buffer, offset)?;
        let value_start = offset + bytes_walked;

        let value_end = value_start + value_len as usize;
        if value_end > buffer.len() {
            return Err(NldbError::InvalidQuery);
        }
        let value = buffer[value_start..value_end].to_vec();

        validate_data(&key, Some(&value))?;

        Ok(NldbRequest::Insert { key, value })
    }

    fn parse_delete(buffer: Vec<u8>, offset: usize) -> Result<NldbRequest, NldbError> {
        let (key, _) = parse_key(&buffer, offset)?;
        validate_data(&key, None)?;
        Ok(NldbRequest::Delete { key })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(s: &[u8]) -> Result<NldbRequest, NldbError> {
        NldbRequest::parse(s.to_vec())
    }

    fn encode_varint(mut value: usize) -> Vec<u8> {
        let mut out = Vec::new();
        loop {
            let mut byte = (value & 0x7F) as u8;
            value >>= 7;
            if value != 0 {
                byte |= 0x80;
            }
            out.push(byte);
            if value == 0 {
                break;
            }
        }
        out
    }

    fn make_get(key: &[u8]) -> Vec<u8> {
        let mut buf = b"GET ".to_vec();
        buf.extend(encode_varint(key.len()));
        buf.extend_from_slice(key);
        buf
    }

    fn make_delete(key: &[u8]) -> Vec<u8> {
        let mut buf = b"DELETE ".to_vec();
        buf.extend(encode_varint(key.len()));
        buf.extend_from_slice(key);
        buf
    }

    fn make_insert(key: &[u8], value: &[u8]) -> Vec<u8> {
        let mut buf = b"INSERT ".to_vec();
        buf.extend(encode_varint(key.len()));
        buf.extend_from_slice(key);
        buf.extend(encode_varint(value.len()));
        buf.extend_from_slice(value);
        buf
    }

    // --- GET ---

    #[test]
    fn test_get_simple() {
        let req = parse(&make_get(b"foo")).unwrap();
        let NldbRequest::Get { key } = req else {
            panic!("wrong variant")
        };
        assert_eq!(key, b"foo");
    }

    #[test]
    fn test_get_case_insensitive_command() {
        let mut buf = b"get ".to_vec();
        buf.extend(encode_varint(3));
        buf.extend_from_slice(b"foo");
        let req = parse(&buf).unwrap();
        assert!(matches!(req, NldbRequest::Get { .. }));
    }

    #[test]
    fn test_get_key_with_spaces() {
        let req = parse(&make_get(b"hello world")).unwrap();
        let NldbRequest::Get { key } = req else {
            panic!()
        };
        assert_eq!(key, b"hello world");
    }

    #[test]
    fn test_get_multibyte_key_varint() {
        let key = vec![b'k'; 200];
        let req = parse(&make_get(&key)).unwrap();
        let NldbRequest::Get { key: parsed_key } = req else {
            panic!()
        };
        assert_eq!(parsed_key.len(), 200);
    }

    // --- DELETE ---

    #[test]
    fn test_delete_simple() {
        let req = parse(&make_delete(b"bar")).unwrap();
        let NldbRequest::Delete { key } = req else {
            panic!()
        };
        assert_eq!(key, b"bar");
    }

    // --- INSERT ---

    #[test]
    fn test_insert_simple() {
        let req = parse(&make_insert(b"mykey", b"myvalue")).unwrap();
        let NldbRequest::Insert { key, value } = req else {
            panic!()
        };
        assert_eq!(key, b"mykey");
        assert_eq!(value, b"myvalue");
    }

    #[test]
    fn test_insert_value_with_spaces() {
        let req = parse(&make_insert(b"k", b"hello world")).unwrap();
        let NldbRequest::Insert { value, .. } = req else {
            panic!()
        };
        assert_eq!(value, b"hello world");
    }

    #[test]
    fn test_insert_binary_value() {
        let value = vec![0u8, 1, 2, 255, 128];
        let req = parse(&make_insert(b"k", &value)).unwrap();
        let NldbRequest::Insert { value: parsed, .. } = req else {
            panic!()
        };
        assert_eq!(parsed, value);
    }

    // --- Error cases ---

    #[test]
    fn test_unknown_command_errors() {
        assert!(parse(b"FOOBAR \x03foo").is_err());
    }

    #[test]
    fn test_empty_payload_errors() {
        assert!(parse(b"").is_err());
    }

    #[test]
    fn test_command_only_no_space_errors() {
        assert!(parse(b"GET").is_err());
    }

    #[test]
    fn test_empty_key_errors() {
        // varint 0 → empty key
        let buf = b"GET \x00".to_vec();
        assert!(parse(&buf).is_err());
    }

    #[test]
    fn test_get_key_len_exceeds_buffer_errors() {
        let mut buf = b"GET ".to_vec();
        buf.extend(encode_varint(100));
        buf.extend_from_slice(b"short");
        assert!(parse(&buf).is_err());
    }

    #[test]
    fn test_insert_value_len_exceeds_buffer_errors() {
        let mut buf = b"INSERT ".to_vec();
        buf.extend(encode_varint(3));
        buf.extend_from_slice(b"key");
        buf.extend(encode_varint(100));
        buf.extend_from_slice(b"tiny");
        assert!(parse(&buf).is_err());
    }
}

#[cfg(test)]
mod varint_tests {
    use crate::error::NldbError;
    use crate::util::decode_untrusted_varint;

    #[test]
    fn test_single_byte() {
        let (val, walked) = decode_untrusted_varint(&[0x01], 0).unwrap();
        assert_eq!(val, 1);
        assert_eq!(walked, 1);
    }

    #[test]
    fn test_zero() {
        let (val, walked) = decode_untrusted_varint(&[0x00], 0).unwrap();
        assert_eq!(val, 0);
        assert_eq!(walked, 1);
    }

    #[test]
    fn test_two_byte() {
        let (val, walked) = decode_untrusted_varint(&[0x80, 0x01], 0).unwrap();
        assert_eq!(val, 128);
        assert_eq!(walked, 2);
    }

    #[test]
    fn test_at_offset() {
        let buf = [0x00, 0x05];
        let (val, walked) = decode_untrusted_varint(&buf, 1).unwrap();
        assert_eq!(val, 5);
        assert_eq!(walked, 1);
    }

    #[test]
    fn test_max_single_byte() {
        let (val, walked) = decode_untrusted_varint(&[0x7F], 0).unwrap();
        assert_eq!(val, 127);
        assert_eq!(walked, 1);
    }

    #[test]
    fn test_empty_buffer_errors() {
        assert!(matches!(
            decode_untrusted_varint(&[], 0),
            Err(NldbError::InvalidQuery)
        ));
    }

    #[test]
    fn test_offset_at_end_errors() {
        assert!(matches!(
            decode_untrusted_varint(&[0x01], 1),
            Err(NldbError::InvalidQuery)
        ));
    }

    #[test]
    fn test_all_continuation_bytes_errors() {
        // 10 bytes all with continuation bit set — should error before shift overflow
        let buf = vec![0xFF; 10];
        assert!(matches!(
            decode_untrusted_varint(&buf, 0),
            Err(NldbError::InvalidQuery)
        ));
    }
}
