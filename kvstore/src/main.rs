use common::storage::{storage_client::StorageClient, PutRequest};
use tonic;
use actix_web;

#[actix_web::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = StorageClient::connect("http://[::1]:50051").await?;

    let request = tonic::Request::new(PutRequest {
        key: "key".as_bytes().to_vec(),
        value: "value".as_bytes().to_vec(),
        crc: Some(12)
    });

    let response = client.put(request).await?;
    println!("RESPONSE={:?}", response);

    Ok(())
}
