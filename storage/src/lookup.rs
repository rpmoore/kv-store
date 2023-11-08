use crate::namespace::Namespace;
use uuid::Uuid;

#[derive(Debug, Clone, Default)]
struct NamespaceLookup {}

impl NamespaceLookup {
    fn get_namespace(&self, tenant_id: Uuid, namespace: &str) -> Option<Namespace> {
        None
    }
}
