use actix_web::http::StatusCode;
use actix_web::{get, web::Data, App, HttpResponse, HttpResponseBuilder, HttpServer};
use serde::Serialize;
use std::io;
use tracing_actix_web::TracingLogger;

type HealthCheck = fn() -> Result<String, String>;

#[derive(Serialize, Debug)]
struct HealthCheckResponse {
    message: String,
}

#[get("/health")]
async fn check(health_check_fn: Data<HealthCheck>) -> HttpResponse {
    match health_check_fn.into_inner()() {
        Ok(success) => {
            HttpResponseBuilder::new(StatusCode::OK).json(HealthCheckResponse { message: success })
        }
        Err(err) => {
            HttpResponseBuilder::new(StatusCode::OK).json(HealthCheckResponse { message: err })
        }
    }
}

pub async fn healthcheck_endpoint(port: u16, healthcheck_fn: HealthCheck) -> io::Result<()> {
    HttpServer::new(move || {
        App::new()
            .app_data(Data::new(healthcheck_fn))
            .wrap(TracingLogger::default())
            .service(check)
    })
    .bind(("0.0.0.0", port))
    .unwrap()
    .run()
    .await
}
