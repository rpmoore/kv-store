use std::time::SystemTime;
use prost_types::Timestamp;
use common::storage::{storage_server::Storage, storage_server::StorageServer, PutRequest, PutResponse, GetRequest, GetResponse, CreateNamespaceRequest, DeleteNamespaceRequest, DeleteRequest, MigrateToNewNodeRequest};
use common::auth::{Identity, RsaJwtValidator};
use common::read_file_bytes;
use tonic::{transport::Server, Request, Response, Status};
use tokio;
use tracing_attributes::instrument;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse()?;

    let private_key = read_file_bytes("key.pub.pem")?;

    let validator = RsaJwtValidator::new(private_key.as_slice())?;

    let server = NodeStorageServer::new(validator);

    Server::builder()
        .add_service(StorageServer::new(server))
        .serve(addr)
        .await?;
    Ok(())
}


#[derive(Debug)]
struct NodeStorageServer{
    jwt_validator: RsaJwtValidator
}

impl NodeStorageServer {
    fn new(jwt_validator: RsaJwtValidator) -> NodeStorageServer {
        NodeStorageServer{jwt_validator}
    }
}

#[tonic::async_trait]
impl Storage for NodeStorageServer {

    #[instrument]
    async fn create_namespace(&self, request: Request<CreateNamespaceRequest>) -> Result<Response<()>, Status> {
        todo!()
    }

    async fn delete_namespace(&self, request: Request<DeleteNamespaceRequest>) -> Result<Response<()>, Status> {
        todo!()
    }

    async fn put(&self, request: Request<PutRequest>) -> Result<Response<PutResponse>, Status> {
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

    async fn delete(&self, request: Request<DeleteRequest>) -> Result<Response<()>, Status> {
        todo!()
    }

    async fn migrate_to_new_node(&self, request: Request<MigrateToNewNodeRequest>) -> Result<Response<()>, Status> {
        todo!()
    }
}