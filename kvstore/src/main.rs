use common::storage::{PutRequest, storage_client::StorageClient};
use tonic;
use actix_web;
use actix_web::{App, body::BoxBody, error, http::header::ContentType, HttpRequest, HttpResponse, HttpServer, put, Responder, web};
use actix_web::http::StatusCode;
use serde::{Deserialize, Serialize};
use derive_more::{Display, Error};
use tonic::transport::Channel;
use crate::connections::ConnectionManager;
use crate::MainErrors::{IoError, TonicError};
use tracing_subscriber;
use tracing_actix_web;
use tracing_actix_web::TracingLogger;
use tracing::{info, error, Level, Subscriber};
use tracing_attributes::instrument;
use tracing_subscriber::fmt::FormatFields;
use tracing_subscriber::Layer;
use futures::try_join;

mod connections;

#[derive(Error, Debug, Display)]
enum MainErrors {
    #[display(fmt="io error")]
    IoError(std::io::Error),
    #[display(fmt="tonic error")]
    TonicError(tonic::transport::Error)
}

#[actix_web::main]
async fn main() -> Result<(), MainErrors> {

    tracing_subscriber::fmt()
        .json()
        .with_max_level(Level::INFO)
        .with_target(true)
        .with_thread_names(true)
        .with_file(true)
        .init();

    let channel = Channel::from_static("http://[::1]:50051").connect_lazy();

    let client = StorageClient::new(channel);

    let mut connection_manager = connections::ConnectionManager::default();
    connection_manager.new_conn(client);

    let app_data = web::Data::new(AppData{connection_manager});

    let healthcheck = common::healthcheck::healthcheck_endpoint(8081, || Ok("healthy".to_string()));

    let server =  HttpServer::new(move || App::new().app_data(app_data.clone()).wrap(TracingLogger::default()).service(put))
        .bind(("0.0.0.0", 8080)).unwrap()
        .run();

    try_join!(healthcheck, server).map(|(_,_)| ()).map_err(|err|IoError(err))
}

#[derive(Debug)]
struct AppData {
    connection_manager: ConnectionManager,
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

#[instrument]
#[put("/key/{id}")]
async fn put(path: web::Path<String>, data: web::Json<PutValue>, app_data : web::Data<AppData>) -> Result<impl Responder, KVErrors> {
    let id = path.into_inner();

    let mut client = {
        app_data.connection_manager.get_conn(0).unwrap().clone() // clone to avoid race conditions
    };

    let value = data.into_inner();
    info!(key = id, "putting new key");
    let request = tonic::Request::new(PutRequest {
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
