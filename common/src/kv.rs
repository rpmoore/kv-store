use crate::BoxError;

pub trait KVStore {

    // Figure out how to report errors back for all of these methods
    // Also need to figure out what methods we should add to this trait to support
    // the most common key value operations
    fn get(&self, key: impl Into<String>) -> Result<Vec<u8>, BoxError>;
    fn insert(&mut self, key: impl Into<String>, value: &[u8]) -> Result<(), BoxError>;

    fn remove(&mut self, key: impl Into<String>) -> Result<Vec<u8>, BoxError>;

    // this should probably return a cursor of some kind, and should provide some limited querying capabilities
    fn list();
}