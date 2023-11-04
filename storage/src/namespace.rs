use rocksdb::{BoundColumnFamily, DB, DEFAULT_COLUMN_FAMILY_NAME, Error, ErrorKind, Options, WriteBatch};
use std::sync::Arc;
use std::path::Path;
use serde::{Serialize, Deserialize};
use tracing::error;

pub struct Namespace {
    name: String,
    db: Arc<DB>,
}

#[derive(Debug, Serialize, Deserialize)]
struct Value {
    crc: u32,
    version: u32, // need to check to make sure the current version at least one above the current version, and if it is not, return a cas error
    value: Box<[u8]>,
}

impl Value {
    // Might want to consider passing in the buffer that is stack allocated to fill instead of allocating a vec on the heap for this
    fn metadata_as_bytes(&self) -> Vec<u8> {
        return vec!(self.crc.to_be_bytes().as_slice(), self.version.to_be_bytes().as_slice()).concat().to_vec()
    }
}

impl Namespace {
    fn new<I>(name: impl Into<String>, path: I) -> Result<Namespace, Error> where I: AsRef<Path>{
        let name =  name.into();
        let mut options = Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);

        let db = DB::open_cf(&options, &path, vec!(DEFAULT_COLUMN_FAMILY_NAME, "metadata"))?;

        let db= Arc::new(db);
        Ok(Namespace{name, db})
    }

    fn get(&self, key: &[u8]) -> Result<Value, ErrorKind> {
        let metadata_handle = self.db.cf_handle("metadata").unwrap();
        let default_handle = self.db.cf_handle(DEFAULT_COLUMN_FAMILY_NAME).unwrap();

        let get_parts = self.db.multi_get_cf(vec!((&default_handle, key), (&metadata_handle, key)));

        let value: Box<[u8]> = match get_parts.get(0) {
            Some(Ok(Some(value))) => value.clone().into_boxed_slice(),

            Some(Err(err)) => return {
                error!(err = err.to_string(), "failed to get value");
                return Err(err.kind())
            },

            _ => return Err(ErrorKind::Incomplete)
        };

        let (crc, version) = match get_parts.get(1) {
            Some(Ok(Some(value))) => {
                let (crc, version) = value.split_at(4);
                (u32::from_be_bytes(crc.try_into().unwrap()), u32::from_be_bytes(version.try_into().unwrap()))
            },
            Some(Err(err)) => return {
                error!(err = err.to_string(), "failed to get value");
                return Err(err.kind())
            },
            _ => return Err(ErrorKind::Incomplete)
        };

        Ok(Value{
            crc,
            version,
            value
        })
    }

    fn put(&self, key: &[u8], value: &Value) -> Result<(), rocksdb::ErrorKind> {
        let cf_handle = self.db.cf_handle("metadata").unwrap();
        let mut batch = WriteBatch::default();
        batch.put_cf(&cf_handle, key, value.metadata_as_bytes());
        batch.put(key, &value.value);

        self.db.write(batch).map_err(|err| {
            error!{err = err.to_string(), "failed to write value"};
            err.kind()
        })
    }

    fn exists(&self, key: &[u8]) -> Result<bool, Error> {
        self.db.get(key).map(|v| v.is_some())
    }

    fn delete(&self, key: &[u8]) -> Result<(), Error> {
        self.db.delete(key)
    }
}