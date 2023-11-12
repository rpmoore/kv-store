use derive_more::Display;
use serde::Serialize;
use sqlx::sqlite::SqliteRow;
use sqlx::{query, Pool, Result, Row, Sqlite};
use tracing::{error, info};
use tracing_attributes::instrument;
use uuid::Uuid;

#[derive(Serialize, Clone, Debug)]
pub struct Namespace {
    pub name: String,
    pub id: Uuid,
}

impl std::fmt::Display for Namespace {
fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{{ name: {}, id: {} }}", self.name, self.id)
    }
}

impl From<SqliteRow> for Namespace {
    fn from(row: SqliteRow) -> Self {
        Namespace {
            name: row.get(0),
            id: Uuid::parse_str(row.get(1)).unwrap(),
        }
    }
}

pub struct NamespaceRepo {
    db_pool: Pool<Sqlite>,
}

impl NamespaceRepo {
    pub fn new(db_pool: Pool<Sqlite>) -> NamespaceRepo {
        NamespaceRepo { db_pool }
    }
    pub async fn exists(&self, tenant: Uuid, namespace: &str) -> bool {
        match query("select exists(select * from namespaces left join tenants on namespaces.tenant_id = tenants.id where tenants.uuid = ? and namespaces.name = ?)")
            .bind(tenant.to_string())
            .bind(&namespace)
            .map(|sqlite_row: SqliteRow| sqlite_row.get(0))
            .fetch_one(&self.db_pool)
            .await {
            Ok(exists) => exists,
            Err(err) => {
                error!(err = err.to_string(), "failed to determine if namespace exists");
                false
            }
        }
    }

    #[instrument(skip(self))]
    pub async fn get(&self, tenant_id: Uuid, namespace: &str) -> Result<Namespace> {
        info!("getting namespace");
        query("select ns.name, ns.uuid from namespaces as ns join tenants on ns.tenant_id = tenants.id where tenants.uuid = ? and ns.name = ?")
            .bind(tenant_id.to_string())
            .bind(namespace)
            .map(|row: SqliteRow| row.into())
            .fetch_one(&self.db_pool).await
    }

    pub async fn list(&self, tenant_id: Uuid) -> Result<Vec<Namespace>> {
        query("select ns.name, ns.uuid from namespaces as ns inner join tenants on ns.tenant_id = tenants.id where tenants.uuid = ?")
            .bind(tenant_id.to_string())
            .map(|row: SqliteRow| row.into())
            .fetch_all(&self.db_pool).await
    }
}
