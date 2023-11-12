use std::collections::HashMap;
use std::error::Error;
use std::fmt::Formatter;
use std::fs::File;
use std::path::{Path, PathBuf};
use crate::partition::{Key, Partition, Error as PError};
use dashmap::DashMap;
use jumphash::{CustomJumpHasher, JumpHasher};
use tracing::instrument;
use std::sync::Arc;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde::de::Visitor;
use tracing::info;
use uuid::Uuid;
use common::crc64hasher::Crc64Hasher;

const PARTITION_CONFIG: &str = "partitions.json";

#[derive(Debug, Clone)]
pub struct PartitionLookup {
    partitions: DashMap<(Uuid, Uuid), Arc<[Partition]>>,
    config_dir: String,
    hasher: CustomJumpHasher<Crc64Hasher>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct PersistedState {
    partitions: HashMap<PersistedID, Vec<PersistedPartition>>,
}

#[derive(Debug, Clone, Hash, Ord, PartialOrd, Eq, PartialEq)]
struct PersistedID {
    namespace_id: Uuid,
    tenant_id: Uuid,
}

impl From<&(Uuid, Uuid)> for PersistedID {
    fn from(value: &(Uuid, Uuid)) -> Self {
        PersistedID {
            namespace_id: value.0,
            tenant_id: value.1,
        }
    }
}

impl From<&PersistedID> for (Uuid, Uuid) {
    fn from(value: &PersistedID) -> Self {
        (value.namespace_id, value.tenant_id)
    }
}

impl Serialize for PersistedID {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error> where S: Serializer {
        serializer.serialize_str(&format!("{}::{}", self.namespace_id, self.tenant_id))
    }
}

struct PersistedIDVisitor;

impl Visitor<'_> for PersistedIDVisitor{
    type Value = PersistedID;

    fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
        formatter.write_str("a persisted namespace-tenant uuid pair")
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E> where E: serde::de::Error {
        let Some((namespace_id, tenant_id)) =  v.split_once("::") else {
            return Err(E::custom("invalid persisted id"))
        };

        info!(namespace_id = namespace_id, tenant_id = tenant_id, "deserializing persisted id");

        Ok(PersistedID {
            namespace_id: Uuid::parse_str(namespace_id).map_err(|err| E::custom(err.to_string()))?,
            tenant_id: Uuid::parse_str(tenant_id).map_err(|err| E::custom(err.to_string()))?,
        })

    }
}

impl<'de> Deserialize<'de> for PersistedID {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error> where D: Deserializer<'de> {
        deserializer.deserialize_str(PersistedIDVisitor)
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct PersistedPartition {
    namespace_id: Uuid,
    tenant_id: Uuid,
    id: Uuid,
}

impl PersistedState {
    fn to_partition_lookup(&self, config_dir: impl AsRef<Path>) -> Result<PartitionLookup, PError> {
        let config_dir = config_dir.as_ref();
        let mut partitions: DashMap<(Uuid, Uuid), Arc<[Partition]>> = DashMap::new();
        for (key, value) in self.partitions.iter() {
            let value: Vec<Partition> = value.iter().map(|partition| partition.to_partition(config_dir)).collect::<Result<Vec<Partition>, PError>>()?;

            partitions.insert(key.into(), value.into());
        }

        Ok(PartitionLookup {
            partitions,
            hasher: CustomJumpHasher::new(Crc64Hasher::new()),
            config_dir: config_dir.to_str().unwrap().to_string(),
        })
    }
}

impl PersistedPartition {
    fn to_partition(&self, base_path: impl AsRef<Path>) -> Result<Partition, PError> {
        Partition::new(
            self.id,
            self.namespace_id,
            self.tenant_id,
            &base_path,
        )
    }
}

impl From<&Partition> for PersistedPartition {
    fn from(value: &Partition) -> Self {
        PersistedPartition {
            namespace_id: value.namespace_id,
            tenant_id: value.tenant_id,
            id: value.id,
        }
    }
}


impl From<&PartitionLookup> for PersistedState {
    fn from(value: &PartitionLookup) -> Self {
        let mut partitions: HashMap<PersistedID, Vec<PersistedPartition>> = HashMap::new();
        for item in value.partitions.iter() {

            let value: Vec<PersistedPartition> = item.value().iter().map(|partition| partition.into()).collect();

            partitions.insert(item.key().into(), value);
        }

        PersistedState { partitions }
    }
}

impl PartitionLookup {
    pub fn load(config: impl AsRef<Path>) -> Result<PartitionLookup, Box<dyn Error>> {

        let config = config.as_ref();

        let binding = config.join(PARTITION_CONFIG);

        let config_file = binding.as_path();

        if !config_file.exists() {
            info!("creating empty partition lookup");
            return Ok(PartitionLookup{
                partitions: DashMap::new(),
                config_dir: config.to_str().unwrap().to_string(),
                hasher: CustomJumpHasher::new(Crc64Hasher::new()),
            })
        }

        info!("loading existing partition lookup");
        let config_file = File::options().read(true).write(false).open(config_file)?;
        let mut persisted_state: PersistedState = serde_json::from_reader(config_file)?;

        let mut lookup: PartitionLookup = persisted_state.to_partition_lookup(config)?;
        lookup.config_dir = config.to_str().unwrap().to_string();

        Ok(lookup)
    }

    fn save(&self) -> std::io::Result<()> {
        let config_path =  PathBuf::from(&self.config_dir).join(PARTITION_CONFIG);
        let config_file = File::options()
            .write(true)
            .create(true)
            .truncate(true)
            .open(config_path.as_path())?;

        let persisted_state: PersistedState = self.into();

        serde_json::to_writer_pretty(&config_file, &persisted_state)?;
        Ok(())
    }

    // Returns the partition that the key routes to using the consistent jump algorithm
    #[instrument(skip(self, key))]
    pub fn get_partition_for_key(
        &self,
        tenant_id: Uuid,
        namespace_id: Uuid,
        key: &Key,
    ) -> Option<Partition> {
        self.partitions(tenant_id, namespace_id).map(|partitions| {
            let partition_count = partitions.len();
            let partition_index = self.hasher.slot(key, partition_count as u32);
            info!(partitions = partition_count, partition_index = partition_index, "routing key to partition");
            partitions[partition_index as usize].clone()
        })
    }

    pub fn partitions(&self, tenant_id: Uuid, namespace_id: Uuid) -> Option<Arc<[Partition]>> {
        match self.partitions.get(&(tenant_id, namespace_id)) {
            Some(partitions) => Some(partitions.value().clone()),
            None => None,
        }
    }

    pub fn add_partition(&self, partition: Partition) -> std::io::Result<()> {
        self.add_partition_internal(partition);
        info!("adding new partition");
        self.save()
    }

    fn add_partition_internal(&self, partition: Partition) {
        let id = (partition.tenant_id, partition.namespace_id);
        let partitions: Vec<Partition> = match self.partitions.get(&id) {
            Some(partitions) => {
                let mut vec = partitions.to_vec();
                vec.push(partition);
                vec
            }
            None => vec![partition],
        };

        // insert should replace the existing value
        self.partitions.insert(id, partitions.into());
    }
}
