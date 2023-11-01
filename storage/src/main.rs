use std::time::SystemTime;
use prost_types::Timestamp;
use common::storage::{CreateNamespaceRequest, DeleteKeyRequest, DeleteNamespaceRequest, GetRequest, GetResponse, ListKeysRequest, ListKeysResponse, MigrateToNewNodeRequest, PutRequest, PutResponse, storage_server::Storage, storage_server::StorageServer};
use common::auth::{JwtValidator, RsaJwtValidator};
use common::read_file_bytes;
use tonic::{Code, Request, Response, Status, transport::Server};
use tracing::{error, info, Level};
use tracing_attributes::instrument;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .json()
        .with_max_level(Level::INFO)
        .with_target(true)
        .with_thread_names(true)
        .with_file(true)
        .init();

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
        let auth_header = match common::auth::AuthHeader::try_from(request.metadata()) {
            Ok(header) => header,
            Err(err) => {
                error!(err = err.to_string(), "invalid auth header");
                return Err(Status::new(Code::Unauthenticated, "auth header missing"))
            }
        };

        let identity = match self.jwt_validator.clone().parse(auth_header) {
            Ok(id) => id,
            Err(err) => {
                error!(err = err.to_string(), "invalid auth header");
                return Err(Status::new(Code::NotFound, "not found"))
            }
        };

        info!(tenant_id = identity.tenant_id().to_string(), "authenticated as tenant");

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

    async fn delete(&self, request: Request<DeleteKeyRequest>) -> Result<Response<()>, Status> {
        todo!()
    }

    async fn migrate_to_new_node(&self, request: Request<MigrateToNewNodeRequest>) -> Result<Response<()>, Status> {
        todo!()
    }

    async fn list_keys(&self, request: Request<ListKeysRequest>) -> Result<Response<ListKeysResponse>, Status> {
        todo!()
    }
}