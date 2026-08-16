use crate::memtable::inner::{Blob, NodeData};
use std::string::FromUtf8Error;
#[derive(Debug)]
pub enum MemtableError {
    TableFull(Blob, NodeData),
}

#[derive(Debug)]
pub enum SSTableError {
    DiskRecordNotFound,
    IOError(std::io::Error),
    Tombstone,
    InvalidSSTableFile,
}

impl From<std::io::Error> for SSTableError {
    fn from(err: std::io::Error) -> SSTableError {
        SSTableError::IOError(err)
    }
}

#[derive(Debug)]
pub enum NldbError {
    InvalidQuery,
    InvalidParameter,
    InvalidConfigFile(yaml_serde::Error),
    IOError(std::io::Error),
    KeySizeConstraint,
    RecordSizeConstraint,
}

impl From<FromUtf8Error> for NldbError {
    fn from(_err: FromUtf8Error) -> NldbError {
        NldbError::InvalidQuery
    }
}

impl From<std::num::ParseIntError> for NldbError {
    fn from(_err: std::num::ParseIntError) -> NldbError {
        NldbError::InvalidParameter
    }
}

impl From<std::io::Error> for NldbError {
    fn from(err: std::io::Error) -> NldbError {
        NldbError::IOError(err)
    }
}

impl From<yaml_serde::Error> for NldbError {
    fn from(err: yaml_serde::Error) -> NldbError {
        NldbError::InvalidConfigFile(err)
    }
}
