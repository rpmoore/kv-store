use std::io::{Error, ErrorKind};
use common::storage::{PutRequest, storage_client::StorageClient};
use tonic;
use std::fmt;
use actix_web;
use actix_web::{App, body::BoxBody, error, http::header::ContentType, HttpRequest, HttpResponse, HttpResponseBuilder, HttpServer, put, post, get, delete, Responder, web, http::header, HttpMessage};
use actix_web::http::StatusCode;
use actix_web::http::header::{TryIntoHeaderValue};
use serde::{Deserialize, Serialize};
use derive_more::{Display, Error};
use tonic::transport::Channel;
use crate::connections::ConnectionManager;
use tracing_subscriber;
use tracing_actix_web;
use tracing_actix_web::TracingLogger;
use tracing::{info, error, Level, Subscriber};
use tracing_attributes::instrument;
use tracing_subscriber::fmt::FormatFields;
use tracing_subscriber::Layer;
use futures::try_join;
use tonic::Extensions;
use common::auth::{JwtIssuer, RsaJwtIssuer};
use uuid::Uuid;

mod connections;

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
    let issuer = RsaJwtIssuer::new(private_key.as_slice()).map_err(|err| {
        error!{err = err.to_string(), "failed to parse key"};
        ErrorKind::InvalidData
    })?;

    let channel = Channel::from_static("http://[::1]:50051").connect_lazy();

    let client = StorageClient::new(channel);

    let mut connection_manager = connections::ConnectionManager::default();
    connection_manager.new_conn(client);

    let app_data = web::Data::new(AppData{connection_manager, jwt_issuer: issuer});

    let healthcheck = common::healthcheck::healthcheck_endpoint(8081, || Ok("healthy".to_string()));

    let server =  HttpServer::new(move || App::new().app_data(app_data.clone()).wrap(TracingLogger::default())
        .service(put)
        .service(gen_token)
    )
        .bind(("0.0.0.0", 8080)).unwrap()
        .run();

    try_join!(healthcheck, server).map(|(_,_)| ())
}

#[derive(Debug)]
struct AppData {
    connection_manager: ConnectionManager,
    jwt_issuer: RsaJwtIssuer,
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

#[instrument]
#[post("/tokens")]
async fn gen_token(app_data: web::Data<AppData>) -> Result<impl Responder, Box<dyn std::error::Error>> {
    let issuer = app_data.jwt_issuer.clone();
    let token = issuer.new_identity(Uuid::new_v4())?;
    Ok(HttpResponseBuilder::new(StatusCode::OK).json(GenTokenResponse{token:token.token()}))
}

#[instrument]
#[put("/namespace/{namespace}/keys/{id}")]
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
