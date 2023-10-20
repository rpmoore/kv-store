use common::storage::{PutRequest, storage_client::StorageClient};
use tonic;
use actix_web;
use actix_web::{App, body::BoxBody, error, http::header::ContentType, HttpRequest, HttpResponse, HttpServer, put, Responder, web};
use actix_web::http::StatusCode;
use serde::{Deserialize, Serialize};
use derive_more::{Display, Error};
use crate::connections::ConnectionManager;
use crate::MainErrors::{IoError, TonicError};

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

    let result = StorageClient::connect("http://[::1]:50051").await;

    let client = match result {
        Ok(client) => client,
        Err(err) => return Err(TonicError(err)),
    };

    let mut connection_manager = connections::ConnectionManager::default();
    connection_manager.new_conn(client);

    let app_data = web::Data::new(AppData{connection_manager});

    HttpServer::new(move || App::new().app_data(app_data.clone()).service(put))
        .bind(("0.0.0.0", 8080)).unwrap()
        .run().await.map_err(|err|IoError(err))
}

struct AppData {
    connection_manager: ConnectionManager,
}

#[derive(Deserialize)]
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

#[put("/key/{id}")]
async fn put(path: web::Path<String>, data: web::Json<PutValue>, app_data : web::Data<AppData>) -> Result<impl Responder, KVErrors> {
    let id = path.into_inner();

    let mut client = {
        app_data.connection_manager.get_conn(0).unwrap().clone() // clone to avoid race conditions
    };

    let value = data.into_inner();

    let request = tonic::Request::new(PutRequest {
        key: id.into_bytes(),
        value: value.value.into_bytes(),
        crc: value.crc,
    });

    let put_response = match client.put(request).await {
        Ok(response) => response.into_inner(),
        Err(_) => return Err(KVErrors::InternalServerError)
    };

    Ok(PutResp{
        version: put_response.version,
        crc: put_response.crc,
        creation_time: put_response.creation_time.map_or(String::from(""), |timestamp| timestamp.to_string())
    })
}
