use crate::partition::{Key, Partition};
use dashmap::DashMap;
use jumphash::JumpHasher;
use std::sync::Arc;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct PartitionLookup {
    partitions: DashMap<(Uuid, Uuid), Arc<[Partition]>>,
}

impl PartitionLookup {
    pub fn new() -> PartitionLookup {
        PartitionLookup {
            partitions: DashMap::new(),
        }
    }

    // Returns the partition that the key routes to using the consistent jump algorithm
    pub fn get_partition_for_key(
        &self,
        tenant_id: Uuid,
        namespace_id: Uuid,
        key: &Key,
    ) -> Option<Partition> {
        self.partitions(tenant_id, namespace_id).map(|partitions| {
            let partition_count = partitions.len();
            let partition_index = JumpHasher::new().slot(key, partition_count as u32);
            partitions[partition_index as usize].clone()
        })
    }

    pub fn partitions(&self, tenant_id: Uuid, namespace_id: Uuid) -> Option<Arc<[Partition]>> {
        match self.partitions.get(&(tenant_id, namespace_id)) {
            Some(partitions) => Some(partitions.value().clone()),
            None => None,
        }
    }

    pub fn add_partition(&self, partition: Partition) {
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
