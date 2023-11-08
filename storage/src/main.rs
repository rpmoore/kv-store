mod namespace;

use std::time::SystemTime;
use prost_types::Timestamp;
use common::storage::{CreateNamespaceRequest, DeleteKeyRequest, DeleteNamespaceRequest, GetRequest, GetResponse, KeyMetadata, ListKeysRequest, ListKeysResponse, MigrateToNewNodeRequest, PutRequest, PutResponse, storage_server::Storage, storage_server::StorageServer};
use common::auth::{Identity, JwtValidator, RsaJwtValidator};
use common::read_file_bytes;
use tonic::{Code, Request, Response, Status, transport::Server};
use tonic::service::Interceptor;
use tracing::{error, info, Level, warn};
use tracing_attributes::instrument;
use uuid::Uuid;
use namespace::{Namespace, PutValue};
use crc32fast::Hasher;
use crate::namespace::ListOptions;

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

    let private_key = read_file_bytes("key.pub")?;

    let validator = RsaJwtValidator::new(private_key.as_slice())?;

    let interceptor = AuthInterceptor::new(validator);

    // replace with a real namespace in the future that belongs to a specific tenant
    let namespace = Namespace::new("test", 0, Uuid::new_v4(), "namespaces")?;

    let server = NodeStorageServer::new(namespace);

    Server::builder()
        .add_service(StorageServer::with_interceptor(server, interceptor))
        .serve(addr)
        .await?;
    Ok(())
}

#[derive(Debug, Clone)]
struct AuthInterceptor {
    jwt_validator: RsaJwtValidator,
}

impl AuthInterceptor {
    fn new(jwt_validator: RsaJwtValidator) -> AuthInterceptor {
        AuthInterceptor{jwt_validator}
    }
}

impl Interceptor for AuthInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        let Ok(auth_header) = common::auth::AuthHeader::try_from(request.metadata()) else {
            error!("invalid auth header");
            return Err(Status::new(Code::Unauthenticated, "auth header missing"))
        };

        let Ok(identity) = self.jwt_validator.parse(auth_header) else {
            error!("invalid auth header");
            return Err(Status::new(Code::NotFound, "not found"))
        };

        info!(tenant_id = identity.tenant_id().to_string(), "authenticated as tenant");
        request.extensions_mut().insert(identity);
        Ok(request)
    }
}

#[derive(Debug)]
struct NodeStorageServer{
    namespace: Namespace,
}

impl NodeStorageServer {
    fn new(namespace: Namespace) -> NodeStorageServer {
        NodeStorageServer{namespace}
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

    #[instrument(skip(request))]
    async fn put(&self, request: Request<PutRequest>) -> Result<Response<PutResponse>, Status> {
        let identity = request.extensions().get::<Identity>().unwrap();

        let request = request.get_ref();

        info!(uuid = identity.tenant_id().to_string(), "got request to put data");

        let mut crc_hasher = Hasher::new();
        crc_hasher.update(request.key.as_slice());
        crc_hasher.update(request.value.as_slice());
        let calculated_crc = crc_hasher.finalize();

        match request.crc {
            Some(crc) => {
                if crc != calculated_crc {
                    error!("crc mismatch");
                    return Err(Status::new(Code::InvalidArgument, "crc mismatch"))
                }
            },
            None => {
                warn!("crc not provided");
            },
        };

        match self.namespace.put(request.key.as_slice(), &PutValue {
            crc: calculated_crc,
            version: 1, // todo calculate the version given the current version
            value: request.value.as_slice(),
        }) {
            Err(err) => {
                error!("failed to put value");
                Err(Status::new(Code::Internal, "internal error"))
            },
            Ok(metadata) => Ok(Response::new(
                PutResponse{
                version: metadata.version,
                crc: metadata.crc,
                creation_time: Some(Timestamp::from(SystemTime::now())),
            })),
        }
    }

    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        let identity = request.extensions().get::<Identity>().unwrap();

        info!(uuid = identity.tenant_id().to_string(), "got request to get data");

        match self.namespace.get(request.get_ref().key.as_slice()) {
            Ok(value) => {
                Ok(Response::new(GetResponse{
                    key: request.get_ref().key.clone(),
                    value: value.value.to_vec(),
                    metadata: Some(common::storage::Metadata{
                        version: value.version,
                        crc: value.crc,
                        creation_time: Some(Timestamp::from(SystemTime::now())),
                    })
                }))
            },
            Err(err) => {
                error!("failed to get value");
                Err(Status::new(Code::NotFound, "not found"))
            }
        }
    }

    async fn list_keys(&self, request: Request<ListKeysRequest>) -> Result<Response<ListKeysResponse>, Status> {
        let identity = request.extensions().get::<Identity>().unwrap();

        let request = request.get_ref();

        info!(uuid = identity.tenant_id().to_string(), "listing keys in namespace");

        self.namespace.list_keys(ListOptions::default())
            .map(|keys| Response::new(ListKeysResponse{
                keys: keys.iter().map(|metadata| {

                    let key_metadata = metadata.metadata.as_ref().unwrap();

                    KeyMetadata {
                        key: metadata.key.clone(),
                        metadata: Some(common::storage::Metadata {
                            version: key_metadata.version,
                                crc: key_metadata.crc,
                            creation_time: Some(Timestamp::from(SystemTime::now())),
                        })
                    }

                } ).collect(),
            }))
            .map_err(|err| {
                error!("failed to list keys");
                Status::new(Code::Internal, "internal error")
            })
    }

    async fn delete(&self, request: Request<DeleteKeyRequest>) -> Result<Response<()>, Status> {
        todo!()
    }

    async fn migrate_to_new_node(&self, request: Request<MigrateToNewNodeRequest>) -> Result<Response<()>, Status> {
        todo!()
    }

    async fn get_metadata(&self, request: Request<GetRequest>) -> Result<Response<common::storage::Metadata>, Status> {
        todo!()
    }
}