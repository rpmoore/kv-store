use std::fs::File;
use std::io::Read;

pub mod auth;
pub mod healthcheck;

pub mod storage {
    tonic::include_proto!("storage"); // The string specified here must match the proto package name
}

pub mod admin {
    tonic::include_proto!("admin");
}

pub fn read_file_bytes(path: &str) -> std::io::Result<Vec<u8>> {
    let mut file = File::open(path)?;
    let mut buffer = vec![];
    file.read_to_end(&mut buffer)?;
    Ok(buffer)
}
