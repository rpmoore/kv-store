use std::sync::Arc;
use common::BoxError;

pub fn add(left: u64, right: u64) -> u64 {
    left + right
}

pub struct WiscKeStore {
    keys : std::collections::HashMap<String, Vec<u8>>,
}

impl common::KVStore for WiscKeStore {
    fn get(&self, key: impl Into<String>) -> Result<Vec<u8>, BoxError> {
        // Implementation goes here
        todo!()
    }

    fn insert(&mut self, key: impl Into<String>, value: &[u8]) -> Result<(), BoxError> {
        // Implementation goes here
            todo!()
    }

    fn remove(&mut self, key: impl Into<String>) -> Result<Vec<u8>, BoxError> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}
