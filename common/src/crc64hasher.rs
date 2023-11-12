use std::fmt::{Debug, Formatter};
use std::hash::Hasher;
use crc64fast::Digest;

#[derive(Clone)]
pub struct Crc64Hasher(Digest);

impl Debug for Crc64Hasher {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("crc64 hasher")
    }
}

impl Crc64Hasher {
    pub fn new() -> Crc64Hasher {
        Crc64Hasher(Digest::new())
    }
}

impl Hasher for Crc64Hasher {
    fn finish(&self) -> u64 {
        self.0.sum64()
    }

    fn write(&mut self, bytes: &[u8]) {
       self.0.write(bytes)
    }
}