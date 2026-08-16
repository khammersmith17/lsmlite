use crate::memtable::inner::Blob;
use std::string::FromUtf8Error;
#[derive(Debug)]
pub enum MemtableError {
    TableFull(Blob, Option<Blob>),
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
pub enum LsmliteError {
    InvalidQuery,
    InvalidParameter,
    InvalidConfigFile(yaml_serde::Error),
    IOError(std::io::Error),
    KeySizeConstraint,
    RecordSizeConstraint,
}

impl From<FromUtf8Error> for LsmliteError {
    fn from(_err: FromUtf8Error) -> LsmliteError {
        LsmliteError::InvalidQuery
    }
}

impl From<std::num::ParseIntError> for LsmliteError {
    fn from(_err: std::num::ParseIntError) -> LsmliteError {
        LsmliteError::InvalidParameter
    }
}

impl From<std::io::Error> for LsmliteError {
    fn from(err: std::io::Error) -> LsmliteError {
        LsmliteError::IOError(err)
    }
}

impl From<yaml_serde::Error> for LsmliteError {
    fn from(err: yaml_serde::Error) -> LsmliteError {
        LsmliteError::InvalidConfigFile(err)
    }
}
