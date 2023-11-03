use rocksdb::{DB, Error};

pub struct Namespace {
    name: String,
    db: DB,
}

struct PutValue<'a> {
    value: &'a[u8],
    crc: u32,
    version: u32, // need to check to make sure the current version at least one above the current version, and if it is not, return a cas error
}

impl AsRef<[u8]> for PutValue {
    fn as_ref(&self) -> &[u8] {
        vec![self.crc.to_be_bytes(), self.version.to_be_bytes(), self.value].concat().as_ref()
    }
}

impl Namespace {
    fn new(name: impl Into<String>, path: impl Into<String>) -> Result<Namespace, Error>{
        let name =  name.into();
        let path = path.into();
        let db: DB = DB::open_default(path)?;
        Ok(Namespace{name, db})
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        self.db.get(key)
    }

    fn put(&self, key: &[u8], value: PutValue) -> Result<(), Error> {
        self.db.put(key, value)
    }

    fn exists(&self, key: &[u8]) -> Result<bool, Error> {
        self.db.get(key).map(|v| v.is_some())
    }

    fn delete(&self, key: &[u8]) -> Result<(), Error> {
        self.db.delete(key)
    }
}