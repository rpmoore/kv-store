use common::storage::KeyMetadata;
use common::storage::Metadata;
use rocksdb::{
    IteratorMode, Options, WriteBatch, DB, DEFAULT_COLUMN_FAMILY_NAME,
};
use serde::{Deserialize, Serialize};
use std::fmt::{Debug, Formatter};
use std::path::Path;
use std::sync::Arc;
use tracing::{error, info};
use tracing_attributes::instrument;
use uuid::Uuid;
use std::fmt::Display;
use crate::partition::Error::RocksDBError;
use std::error::Error as StdError;

#[derive(Debug, Clone)]
pub enum Error {
    RocksDBError(rocksdb::Error),
    General(String)
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            RocksDBError(err) => f.write_str(err.to_string().as_str()),
            Error::General(err) => f.write_str(err.as_str())
        }
    }
}

impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            RocksDBError(err) => Some(err),
            Error::General(_) => None
        }
    }
}

impl From<rocksdb::Error> for Error {
    fn from(value: rocksdb::Error) -> Self {
        RocksDBError(value)
    }
}

impl From<&rocksdb::Error> for Error {
    fn from(value: &rocksdb::Error) -> Self {
        RocksDBError(value.clone())
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct Key(Arc<[u8]>);

impl From<&[u8]> for Key {
    fn from(bytes: &[u8]) -> Self {
        Key(bytes.into())
    }
}

impl From<Key> for Vec<u8> {
    fn from(key: Key) -> Self {
        key.0.to_vec()
    }
}

impl From<&Vec<u8>> for Key {
    fn from(bytes: &Vec<u8>) -> Self {
        Key(bytes.as_slice().into())
    }
}

impl From<Arc<[u8]>> for Key {
    fn from(bytes: Arc<[u8]>) -> Self {
        Key(bytes.clone())
    }
}

impl AsRef<[u8]> for Key {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

#[derive(Clone)]
pub struct Partition {
    db: Arc<DB>,
    pub namespace_id: Uuid,
    pub tenant_id: Uuid,
    pub id: Uuid,
}

impl Debug for Partition {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Partition")
            .field("namespace_id", &self.namespace_id)
            .field("tenant_id", &self.tenant_id)
            .field("id", &self.id)
            .finish()
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PutValue<'a> {
    pub crc: u32,
    pub version: u32, // need to check to make sure the current version at least one above the current version, and if it is not, return a cas error
    pub value: &'a [u8],
}

impl PutValue<'_> {
    // Might want to consider passing in the buffer that is stack allocated to fill instead of allocating a vec on the heap for this
    fn metadata_as_bytes(&self) -> Vec<u8> {
        return vec![
            self.crc.to_be_bytes().as_slice(),
            self.version.to_be_bytes().as_slice(),
        ]
        .concat()
        .to_vec();
    }
}

pub struct ValueMetadata {
    pub crc: u32,
    pub version: u32,
}

pub struct GetValue {
    pub crc: u32,
    pub version: u32, // need to check to make sure the current version at least one above the current version, and if it is not, return a cas error
    pub value: Vec<u8>,
}

#[derive(Debug, Clone, Default)]
pub struct ListOptions<'a> {
    limit: Option<usize>,
    start_at: Option<&'a str>,
}

impl<'a> ListOptions<'a> {
    pub fn with_limit(&mut self, limit: usize) -> &mut Self {
        self.limit = Some(limit);
        self
    }

    pub fn with_start_at(&mut self, start_at: &'a str) -> &mut Self {
        self.start_at = Some(start_at);
        self
    }
}

impl Partition {
    pub fn new<I>(
        id: Uuid,
        namespace_id: Uuid,
        tenant_id: Uuid,
        path: I,
    ) -> Result<Partition, Error>
    where
        I: AsRef<Path>,
    {
        info!(partition_id = id.to_string(), namespace_id = namespace_id.to_string(), tenant_id = tenant_id.to_string(), "initializing partition");
        let mut options = Options::default();
        options.create_if_missing(true);
        options.set_use_direct_io_for_flush_and_compaction(true);
        options.set_use_direct_reads(true);
        options.create_missing_column_families(true);

        let path = path.as_ref().join(id.to_string());

        let db = DB::open_cf(
            &options,
            path.as_path(),
            vec![DEFAULT_COLUMN_FAMILY_NAME, "metadata"],
        )?;

        let db = Arc::new(db);
        Ok(Partition {
            id,
            namespace_id,
            tenant_id,
            db,
        })
    }

    #[instrument(skip(self, key) fields(namespace_id = %self.namespace_id, tenant_id = %self.tenant_id, partition_id = %self.id))]
    pub fn get(&self, key: &Key) -> Result<GetValue, Error> {
        let metadata_handle = self.db.cf_handle("metadata").unwrap();
        let default_handle = self.db.cf_handle(DEFAULT_COLUMN_FAMILY_NAME).unwrap();

        let mut get_parts = self
            .db
            .multi_get_cf(vec![(&default_handle, key), (&metadata_handle, key)]);

        let (crc, version) = match get_parts.remove(1) {
            Ok(Some(value)) => {
                let (crc, version) = value.split_at(4);
                (
                    u32::from_be_bytes(crc.try_into().unwrap()),
                    u32::from_be_bytes(version.try_into().unwrap()),
                )
            }
            Err(err) => {
                error!({info = err.to_string()}, "failed to get value: {}", err);
                return Err(err.into());
            }
            _ => return Err(Error::General("could not find value".to_string())),
         };


        let value: Vec<u8> = match get_parts.remove(0) {
            Ok(Some(value)) => value,

            Err(err) => {
                error!({info = err.to_string()}, "failed to get value: {}", err);
                return Err(err.into());
            }

            _ => return Err(Error::General("could not find value".to_string())),
        };

        Ok(GetValue {
            crc,
            version,
            value,
        })
    }

    pub fn put(&self, key: Key, value: &PutValue) -> Result<ValueMetadata, rocksdb::ErrorKind> {
        // todo get the metadata first to get the latest version and crc information, then update if no invariants are violated, like making sure the version we're going to put is larger than the current version
        let cf_handle = self.db.cf_handle("metadata").unwrap();
        let mut batch = WriteBatch::default();
        batch.put_cf(&cf_handle, &key, value.metadata_as_bytes());
        batch.put(&key, value.value);

        self.db.write(batch).map_err(|err| {
            error! {err = err.to_string(), "failed to write value"};
            err.kind()
        })?;

        Ok(ValueMetadata {
            crc: value.crc,
            version: value.version,
        })
    }

    pub fn exists(&self, key: Key) -> Result<bool, Error> {
        Ok(self.db.get(&key).map(|v| v.is_some())?)
    }

    pub fn delete(&self, key: Key) -> Result<(), Error> {
        let cf_handle = self.db.cf_handle("metadata").unwrap();
        let mut batch = WriteBatch::default();
        batch.delete_cf(&cf_handle, &key);
        batch.delete(&key);

        self.db.write(batch).map_err(|err| Error::RocksDBError(err))
    }

    #[instrument(skip(self, opts), fields(namespace_id = %self.namespace_id, tenant_id = %self.tenant_id, partition_id = %self.id))]
    pub fn list_keys(&self, opts: ListOptions) -> Result<Arc<[KeyMetadata]>, Error> {
        info!("listing keys");
        let cf_handle = self.db.cf_handle("metadata").unwrap();

        let iter = match opts.start_at {
            Some(start_at) => self.db.iterator_cf(
                &cf_handle,
                IteratorMode::From(start_at.as_bytes(), rocksdb::Direction::Forward),
            ),
            None => self.db.iterator_cf(&cf_handle, IteratorMode::Start),
        };

        let mut results = Vec::new();

        for item in iter.take(opts.limit.unwrap_or(50)) {
            let (key, metadata) = item?;
            results.push(KeyMetadata {
                key: key.to_vec(),
                metadata: Some(Metadata {
                    crc: u32::from_be_bytes(metadata[..4].try_into().unwrap()),
                    version: u32::from_be_bytes(metadata[4..].try_into().unwrap()),
                    creation_time: None,
                }),
            });
        }

        info!(result_size = results.len(), "finished listing keys");

        Ok(results.as_slice().into())
    }
}
