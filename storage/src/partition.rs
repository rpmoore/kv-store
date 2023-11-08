use common::storage::KeyMetadata;
use common::storage::Metadata;
use rocksdb::{
    Error, ErrorKind, IteratorMode, Options, WriteBatch, DB, DEFAULT_COLUMN_FAMILY_NAME,
};
use serde::{Deserialize, Serialize};
use std::fmt::{Debug, Formatter};
use std::path::Path;
use std::sync::Arc;
use tracing::error;
use uuid::Uuid;

pub struct Partition {
    db: Arc<DB>,
    namespace_id: Uuid,
    tenant_id: Uuid,
    id: Uuid,
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
    pub value: Box<[u8]>,
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
        let mut options = Options::default();
        options.create_if_missing(true);
        options.set_use_direct_io_for_flush_and_compaction(true);
        options.set_use_direct_reads(true);
        options.create_missing_column_families(true);

        let db = DB::open_cf(
            &options,
            &path,
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

    //todo need to return a different error type here
    pub fn get(&self, key: &[u8]) -> Result<GetValue, ErrorKind> {
        let metadata_handle = self.db.cf_handle("metadata").unwrap();
        let default_handle = self.db.cf_handle(DEFAULT_COLUMN_FAMILY_NAME).unwrap();

        let get_parts = self
            .db
            .multi_get_cf(vec![(&default_handle, key), (&metadata_handle, key)]);

        let value: Box<[u8]> = match get_parts.get(0) {
            Some(Ok(Some(value))) => value.clone().into_boxed_slice(),

            Some(Err(err)) => {
                return {
                    error!(err = err.to_string(), "failed to get value");
                    return Err(err.kind());
                }
            }

            _ => return Err(ErrorKind::Incomplete),
        };

        let (crc, version) = match get_parts.get(1) {
            Some(Ok(Some(value))) => {
                let (crc, version) = value.split_at(4);
                (
                    u32::from_be_bytes(crc.try_into().unwrap()),
                    u32::from_be_bytes(version.try_into().unwrap()),
                )
            }
            Some(Err(err)) => {
                return {
                    error!(err = err.to_string(), "failed to get value");
                    return Err(err.kind());
                }
            }
            _ => return Err(ErrorKind::Incomplete),
        };

        Ok(GetValue {
            crc,
            version,
            value,
        })
    }

    pub fn put(&self, key: &[u8], value: &PutValue) -> Result<ValueMetadata, rocksdb::ErrorKind> {
        // todo get the metadata first to get the latest version and crc information, then update if no invariants are violated, like making sure the version we're going to put is larger than the current version
        let cf_handle = self.db.cf_handle("metadata").unwrap();
        let mut batch = WriteBatch::default();
        batch.put_cf(&cf_handle, key, value.metadata_as_bytes());
        batch.put(key, value.value);

        self.db.write(batch).map_err(|err| {
            error! {err = err.to_string(), "failed to write value"};
            err.kind()
        })?;

        Ok(ValueMetadata {
            crc: value.crc,
            version: value.version,
        })
    }

    pub fn exists(&self, key: &[u8]) -> Result<bool, Error> {
        self.db.get(key).map(|v| v.is_some())
    }

    pub fn delete(&self, key: &[u8]) -> Result<(), Error> {
        let cf_handle = self.db.cf_handle("metadata").unwrap();
        let mut batch = WriteBatch::default();
        batch.delete_cf(&cf_handle, key);
        batch.delete(key);

        self.db.write(batch)
    }

    pub fn list_keys(&self, opts: ListOptions) -> Result<Box<[KeyMetadata]>, Error> {
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

        Ok(results.into_boxed_slice())
    }
}
