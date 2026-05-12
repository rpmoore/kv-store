extern crate core;

use std::error;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::fs::File;
use std::io::Read;

pub mod auth;
pub mod healthcheck;
pub mod crc64hasher;

pub mod storage {
    tonic::include_proto!("storage"); // The string specified here must match the proto package name
}

pub mod admin {
    tonic::include_proto!("admin");
}

pub type BoxError = Box<dyn Error + Send + Sync + 'static>;

pub fn read_file_bytes(path: &str) -> std::io::Result<Vec<u8>> {
    let mut file = File::open(path)?;
    let mut buffer = vec![];
    file.read_to_end(&mut buffer)?;
    Ok(buffer)
}

pub trait KVStore {

    // Figure out how to report errors back for all of these methods
    // Also need to figure out what methods we should add to this trait to support
    // the most common key value operations
    fn get(&self, key: impl Into<String>) -> Result<Vec<u8>, BoxError>;
    fn insert(&mut self, key: impl Into<String>, value: &[u8]) -> Result<(), BoxError>;

    fn remove(&mut self, key: impl Into<String>) -> Result<Vec<u8>, BoxError>;
}