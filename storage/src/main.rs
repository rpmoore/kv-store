use std::time::SystemTime;
use prost_types::Timestamp;
use common::storage::{storage_server::Storage, storage_server::StorageServer, PutRequest, PutResponse, GetRequest, GetResponse};
use tonic::{transport::Server, Request, Response, Status};
use tokio;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse()?;


    let server = NodeStorageServer::default();

    Server::builder()
        .add_service(StorageServer::new(server))
        .serve(addr)
        .await?;

    Ok(())
}

#[derive(Debug, Default)]
pub struct NodeStorageServer{}

#[tonic::async_trait]
impl Storage for NodeStorageServer {
    async fn put(&self, _: Request<PutRequest>) -> Result<Response<PutResponse>, Status> {
        println!("got request to put data");
        let response = PutResponse {
            version: 1,
            crc: 25,
            creation_time: Some(Timestamp::from(SystemTime::now())),
        };

        Ok(Response::new(response))
    }
    async fn get(&self, _: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        todo!()
    }
}