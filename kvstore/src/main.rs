use common::storage::{storage_client::StorageClient, PutRequest};
use tonic;
use actix_web;
use actix_web::{Responder, web, HttpServer, App, put, HttpResponse, body::BoxBody, HttpRequest, http::header::ContentType, error};
use actix_web::http::StatusCode;
use serde::{Deserialize, Serialize};
use derive_more::{Display, Error};

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    HttpServer::new(|| App::new().service(put))
        .bind(("0.0.0.0", 8080))?
        .run()
        .await
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
async fn put(path: web::Path<String>, data: web::Json<PutValue>) -> Result<impl Responder, KVErrors> {
    let id = path.into_inner();

    let result = StorageClient::connect("http://[::1]:50051").await;

    let mut client = match result {
        Ok(client) => client,
        Err(_) => return Err(KVErrors::ServiceUnavailable),
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