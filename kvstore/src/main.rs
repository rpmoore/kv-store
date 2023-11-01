use std::io::{Error, ErrorKind};
use common::storage::{PutRequest, storage_client::StorageClient};
use actix_web::{App, body::BoxBody, error, get, http::header::ContentType, HttpRequest, HttpResponse, HttpResponseBuilder, HttpServer, post, put, Responder, web};
use actix_web::http::StatusCode;
use serde::{Deserialize, Serialize};
use derive_more::{Display, Error};
use tonic::transport::Channel;
use crate::connections::ConnectionManager;
use tracing_actix_web::TracingLogger;
use tracing::{error, info, Level};
use tracing_attributes::instrument;
use tracing_subscriber::fmt::FormatFields;
use futures::{try_join, TryStreamExt};
use tonic::Extensions;
use common::auth::{JwtIssuer, JwtValidator};
use uuid::Uuid;
use sqlx::sqlite::{Sqlite, SqlitePoolOptions, SqliteRow};
use sqlx::{migrate::MigrateDatabase, Pool, query, Row};

mod connections;
mod auth;

#[actix_web::main]
async fn main() -> Result<(), Error> {
    tracing_subscriber::fmt()
        .json()
        .with_max_level(Level::INFO)
        .with_target(true)
        .with_thread_names(true)
        .with_file(true)
        .init();

    let private_key = common::read_file_bytes("key.pem")?;
    let public_key = common::read_file_bytes("key.pub.pem")?;
    let jwts = auth::JwtIssuerVerifier::new(private_key.as_slice(), public_key.as_slice()).map_err(|err| {
        error!{err = err.to_string(), "failed to parse key"};
        ErrorKind::InvalidData
    })?;

    let pool = create_db_pool("sqlite://data.db").await?;

    info!("creating sqlite tables");
    create_tables(&pool).await.unwrap();
    info!("ran create tables");

    let channel = Channel::from_static("http://[::1]:50051").connect_lazy();

    let client = StorageClient::new(channel);

    let mut connection_manager = connections::ConnectionManager::default();
    connection_manager.new_conn(client);

    let app_data = web::Data::new(AppData{connection_manager, jwts, db_pool: pool});

    let healthcheck = common::healthcheck::healthcheck_endpoint(8081, || Ok("healthy".to_string()));

    let server =  HttpServer::new(move || App::new().app_data(app_data.clone()).wrap(TracingLogger::default())
        .service(put)
        .service(gen_token)
        .service(list_namespaces)
    )
        .bind(("0.0.0.0", 8080)).unwrap()
        .run();

    try_join!(healthcheck, server).map(|(_,_)| ())
}

async fn create_db_pool(path: &str) -> Result<Pool<Sqlite>, ErrorKind> {
    if !Sqlite::database_exists(path).await.unwrap_or(false) {
        println!("Creating database {}", path);
        match Sqlite::create_database(path).await {
            Ok(_) => println!("Create db success"),
            Err(err) => {
                error!(err = err.to_string(), "failed to create db");
                return Err(ErrorKind::NotFound);
            }
        }
    }

    let pool = SqlitePoolOptions::new().connect(path).await.map_err(|err| {
        error!{err = err.to_string(), "failed to connect to db"};
        ErrorKind::NotFound
    })?;
    Ok(pool)
}

async fn create_tables(pool: &Pool<Sqlite>) -> Result<(), sqlx::Error> {
    query("create table if not exists namespaces (id integer primary key autoincrement, uuid varchar(36), name varchar(255), tenant_id integer, unique(tenant_id, name), foreign key(tenant_id) references tenants(id))").execute(pool).await?;
    query("create table if not exists storage_targets (id integer primary key autoincrement, namespace_id integer, endpoint varchar(255))").execute(pool).await?;
    query("create table if not exists tenants(id integer primary key autoincrement, uuid varchar(36), name varchar(255), password_hash varchar(255), unique(name), unique(uuid))").execute(pool).await?;
    let user_id: u32 = match query("insert or ignore into tenants (name, uuid) values ('dev', ?) returning id").bind(Uuid::new_v4().to_string()).map(|row :SqliteRow| row.get(0)).fetch(pool).try_next().await? {
        Some(id) => id,
        None => return Ok(())
    };
    query("insert or ignore into namespaces (name, uuid, tenant_id) values('dev', ?, ?)").bind(Uuid::new_v4().to_string()).bind(user_id).execute(pool).await?;
    Ok(())
}

#[derive(Debug)]
struct AppData {
    connection_manager: ConnectionManager,
    jwts: auth::JwtIssuerVerifier,
    db_pool: Pool<Sqlite>
}

#[derive(Deserialize, Debug)]
struct PutValue {
    value: String,
    crc: Option<u32>,
}

#[derive(Serialize)]
struct PutResp {
    version: u32,
    crc: u32,
    creation_time: String,
}

impl Responder for PutResp {
    type Body = BoxBody;

    fn respond_to(self, _req: &HttpRequest) -> HttpResponse<Self::Body> {
        let body = serde_json::to_string(&self).unwrap();

        // Create response and set content type
        HttpResponse::Ok()
            .content_type(ContentType::json())
            .body(body)
    }
}

#[derive(Error, Display, Debug)]
enum KVErrors {
    #[display(fmt="downstream service unavailable")]
    ServiceUnavailable,

    #[display(fmt="internal server error")]
    InternalServerError
}

impl error::ResponseError for KVErrors {
    fn status_code(&self) -> StatusCode {
        match *self {
            KVErrors::InternalServerError => StatusCode::INTERNAL_SERVER_ERROR,
            KVErrors::ServiceUnavailable => StatusCode::SERVICE_UNAVAILABLE,
        }
    }

    fn error_response(&self) -> HttpResponse {
        HttpResponse::build(self.status_code())
            .insert_header(ContentType::plaintext())
            .body(self.to_string())
    }
}

#[derive(Serialize, Debug)]
struct GenTokenResponse {
    token: String
}

#[derive(Deserialize, Debug)]
struct GenTokenRequest {
    name: String
}

#[derive(Debug)]
struct Tenant {
    name: String,
    uuid: Uuid
}

#[instrument]
#[post("/tokens")]
async fn gen_token(app_data: web::Data<AppData>, data: web::Json<GenTokenRequest>) -> Result<impl Responder, Box<dyn std::error::Error>> {
    let issuer = app_data.jwts.clone();

    let tenant = match query("select name, uuid from tenants where name = ?")
        .bind(&data.name)
        .map(|row: SqliteRow| Tenant{ name: row.get(0), uuid: Uuid::parse_str(row.get(1)).unwrap()} )
        .fetch_one(&app_data.db_pool).await {
        Ok(tenant) => tenant,
        Err(err) => {
            error!(err = err.to_string(), "failed to get tenant information");
            return Ok(HttpResponseBuilder::new(StatusCode::BAD_REQUEST).finish())
        }
    };
    let token = issuer.new_identity(tenant.uuid)?;
    Ok(HttpResponseBuilder::new(StatusCode::OK).json(GenTokenResponse{token:token.token()}))
}

#[instrument]
#[put("/namespaces/{namespace}/keys/{id}")]
async fn put(path: web::Path<(String, String)>, data: web::Json<PutValue>, app_data : web::Data<AppData>, auth_data: web::Header<common::auth::AuthHeader>) -> Result<impl Responder, KVErrors> {
    let (namespace, id) = path.into_inner();

    // grab identity from headers
    let metadata = auth_data.into_inner().into();

    let mut client = {
        app_data.connection_manager.get_conn(0).unwrap().clone() // clone to avoid race conditions
    };

    let value = data.into_inner();
    info!(key = id, "putting new key");

    let request = tonic::Request::from_parts(metadata, Extensions::default(),PutRequest {
        namespace: namespace.to_owned(),
        key: id.clone().into_bytes(),
        value: value.value.into_bytes(),
        crc: value.crc,
    });

    let put_response = match client.put(request).await {
        Ok(response) => response.into_inner(),
        Err(err) => {
            error!(key = id, err = err.to_string(), "failed to put value");
            return Err(KVErrors::InternalServerError)
        }
    };

    Ok(PutResp{
        version: put_response.version,
        crc: put_response.crc,
        creation_time: put_response.creation_time.map_or(String::from(""), |timestamp| timestamp.to_string())
    })
}

#[derive(Deserialize, Clone, Debug)]
struct CreateNamespace {
    name: String
}

//#[instrument]
#[post("/namespaces")]
async fn create_namespace(path: web::Path<(String, String)>, data: web::Json<CreateNamespace>, app_data : web::Data<AppData>, auth_data: web::Header<common::auth::AuthHeader>) -> Result<impl Responder, KVErrors> {
    Ok(HttpResponseBuilder::new(StatusCode::NOT_IMPLEMENTED).finish())
}

#[derive(Serialize, Clone, Debug)]
struct NamespaceResponse {
    name: String,
    id: Uuid
}

#[derive(Serialize, Debug)]
struct NamespacesResponse {
    namespaces: Vec<NamespaceResponse>
}

#[instrument(skip(app_data))]
#[get("/namespaces")]
async fn list_namespaces(app_data : web::Data<AppData>, auth_data: web::Header<common::auth::AuthHeader>) -> Result<impl Responder, KVErrors> {
    let identity = match app_data.jwts.clone().parse(auth_data.as_ref()) {
        Ok(id) => id,
        Err(err) => {
            error!(err = err.to_string(), "failed to verify auth data");
            return Ok(HttpResponseBuilder::new(StatusCode::NOT_FOUND).finish())
        }
    };

    let tenant_id = identity.tenant_id();

    info!(tenant_id = tenant_id.to_string(), "fetching namespaces");

    let namespaces = match query("select namespaces.name, namespaces.uuid from namespaces inner join tenants on namespaces.tenant_id = tenants.id where tenants.uuid = ?")
        .bind(tenant_id.to_string())
        .map(|row: SqliteRow| NamespaceResponse{
            name: row.get(0),
            id: Uuid::parse_str(row.get(1)).unwrap()
        })
        .fetch_all(&app_data.db_pool).await {
        Ok(namespaces) => namespaces,
        Err(err) => {
            error!(err = err.to_string());
            return Ok(HttpResponseBuilder::new(StatusCode::INTERNAL_SERVER_ERROR).finish())
        }
    };

    Ok(HttpResponseBuilder::new(StatusCode::OK).json(namespaces))
}

