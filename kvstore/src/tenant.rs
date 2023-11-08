use sqlx::{Pool, query, Result, Row, Sqlite};
use sqlx::sqlite::SqliteRow;
use uuid::Uuid;

#[derive(Debug)]
pub struct Tenant {
    pub name: Box<str>,
    pub uuid: Uuid,
}

pub struct TenantRepo {
    db_pool: Pool<Sqlite>,
}

impl TenantRepo {

    pub fn new(db_pool: Pool<Sqlite>) -> TenantRepo {
        TenantRepo { db_pool }
    }
    pub async fn get(&self, name: impl Into<String>) -> Result<Tenant> {
        query("select name, uuid from tenants where name = ?")
            .bind(name.into())
            .map(|row: SqliteRow| Tenant {
                name: Box::from(row.get::<String, usize>(0)),
                uuid: Uuid::parse_str(row.get(1)).unwrap(),
            })
            .fetch_one(&self.db_pool).await
    }
}