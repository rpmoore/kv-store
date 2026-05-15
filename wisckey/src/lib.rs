use std::fs::File;
use std::io::Write;
use std::sync::{Arc, LockResult, RwLock};
use common::BoxError;

pub fn add(left: u64, right: u64) -> u64 {
    left + right
}


#[derive(Debug, Clone)]
pub struct WiscKeStore {
    l0_rw_lock : Arc<RwLock<skiplist::SkipMap<String, Vec<u8>>>>,
    wal : Arc<File>
}

impl common::kv::KVStore for WiscKeStore {
    fn get(&self, key: impl Into<String>) -> Result<Vec<u8>, BoxError> {
        // Implementation goes here
        todo!()
    }

    fn insert(&mut self, key: impl Into<String>, value: &[u8]) -> Result<(), BoxError> {
        // Implementation goes here

        let insert_result = match       self.l0_rw_lock.write() {
            Ok(mut l0_list) => {
                if let _ = l0_list.insert(key.into(), value.to_vec() {
                    self.wal.write(value)

                }



            }
            Err(err) => {
                return Err(Box::new(err));
            }
        };

        if let Err(err) = insert_result {
            return Err(Box::new(err));
        }

        Ok(())
    }

    fn remove(&mut self, key: impl Into<String>) -> Result<Vec<u8>, BoxError> {
        todo!()
    }

    fn list() {

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
