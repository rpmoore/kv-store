use crate::partition::Partition;
use uuid::Uuid;

#[derive(Debug, Clone, Default)]
pub struct PartitionLookup {}

impl PartitionLookup {
    // Returns the partition that the key routes to using the consistent jump algorithm
    pub fn get_partition_for_key(&self, tenant_id: Uuid, namespace_id: Uuid, key: &[u8]) -> Option<Partition> {
        None
    }
}
