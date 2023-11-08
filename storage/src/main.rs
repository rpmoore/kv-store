mod lookup;
mod partition;
mod auth;

use auth::AuthInterceptor;
use partition::ListOptions;
use lookup::PartitionLookup;
use common::auth::{Identity, JwtValidator, RsaJwtValidator};
use common::read_file_bytes;
use common::storage::{
    storage_server::Storage, storage_server::StorageServer, CreateNamespaceRequest,
    DeleteKeyRequest, DeleteNamespaceRequest, GetRequest, GetResponse, KeyMetadata,
    ListKeysRequest, ListKeysResponse, MigrateToNewNodeRequest, PutRequest, PutResponse,
};
use crc32fast::Hasher;
use partition::{Partition, PutValue};
use prost_types::Timestamp;
use std::time::SystemTime;
use tonic::service::Interceptor;
use tonic::{transport::Server, Code, Request, Response, Status};
use tracing::{error, info, warn, Level};
use tracing_attributes::instrument;
use uuid::Uuid;

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
    let namespace = Partition::new(Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4(), "namespaces")?;

    let server = NodeStorageServer::new(namespace);

    Server::builder()
        .add_service(StorageServer::with_interceptor(server, interceptor))
        .serve(addr)
        .await?;
    Ok(())
}

#[derive(Debug)]
struct NodeStorageServer {
    partition_lookup: PartitionLookup,
    namespace: Partition,
}

impl NodeStorageServer {
    fn new(namespace: Partition) -> NodeStorageServer {
        NodeStorageServer { namespace, partition_lookup: PartitionLookup::default()}

    }
}

#[tonic::async_trait]
impl Storage for NodeStorageServer {
    #[instrument]
    async fn create_namespace(
        &self,
        request: Request<CreateNamespaceRequest>,
    ) -> Result<Response<()>, Status> {
        todo!()
    }

    async fn delete_namespace(
        &self,
        request: Request<DeleteNamespaceRequest>,
    ) -> Result<Response<()>, Status> {
        todo!()
    }

    #[instrument(skip(request) fields(namespace_id = %request.get_ref().namespace_id))]
    async fn put(&self, request: Request<PutRequest>) -> Result<Response<PutResponse>, Status> {
        let identity = request.extensions().get::<Identity>().unwrap();

        let request = request.get_ref();

        info!(
            uuid = identity.tenant_id().to_string(),
            "got request to put data"
        );

        let namespace_id = match Uuid::parse_str(&request.namespace_id) {
            Ok(id) => id,
            Err(err) => {
                error!(err = err.to_string(), "failed to parse uuid");
                return Err(Status::new(Code::InvalidArgument, "invalid uuid"));
            }
        };

        let mut crc_hasher = Hasher::new();
        crc_hasher.update(request.key.as_slice());
        crc_hasher.update(request.value.as_slice());
        let calculated_crc = crc_hasher.finalize();

        match request.crc {
            Some(crc) => {
                if crc != calculated_crc {
                    error!("crc mismatch");
                    return Err(Status::new(Code::InvalidArgument, "crc mismatch"));
                }
            }
            None => {
                warn!("crc not provided");
            }
        };

        let partition = self.partition_lookup.get_partition_for_key(identity.tenant_id(), namespace_id, request.key.as_slice())
            .ok_or(Status::new(Code::NotFound, "partition not found"))?;

        match partition.put(
            request.key.as_slice(),
            &PutValue {
                crc: calculated_crc,
                version: 1, // todo calculate the version given the current version
                value: request.value.as_slice(),
            },
        ) {
            Err(err) => {
                error!("failed to put value");
                Err(Status::new(Code::Internal, "internal error"))
            }
            Ok(metadata) => Ok(Response::new(PutResponse {
                version: metadata.version,
                crc: metadata.crc,
                creation_time: Some(Timestamp::from(SystemTime::now())),
            })),
        }
    }

    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        let identity = request.extensions().get::<Identity>().unwrap();

        info!(
            uuid = identity.tenant_id().to_string(),
            "got request to get data"
        );



        match self.namespace.get(request.get_ref().key.as_slice()) {
            Ok(value) => Ok(Response::new(GetResponse {
                key: request.get_ref().key.clone(),
                value: value.value.to_vec(),
                metadata: Some(common::storage::Metadata {
                    version: value.version,
                    crc: value.crc,
                    creation_time: Some(Timestamp::from(SystemTime::now())),
                }),
            })),
            Err(err) => {
                error!("failed to get value");
                Err(Status::new(Code::NotFound, "not found"))
            }
        }
    }

    async fn get_metadata(
        &self,
        request: Request<GetRequest>,
    ) -> Result<Response<common::storage::Metadata>, Status> {
        todo!()
    }

    async fn list_keys(
        &self,
        request: Request<ListKeysRequest>,
    ) -> Result<Response<ListKeysResponse>, Status> {
        let identity = request.extensions().get::<Identity>().unwrap();

        let request = request.get_ref();

        info!(
            uuid = identity.tenant_id().to_string(),
            "listing keys in namespace"
        );

        self.namespace
            .list_keys(ListOptions::default())
            .map(|keys| {
                Response::new(ListKeysResponse {
                    keys: keys
                        .iter()
                        .map(|metadata| {
                            let key_metadata = metadata.metadata.as_ref().unwrap();

                            KeyMetadata {
                                key: metadata.key.clone(),
                                metadata: Some(common::storage::Metadata {
                                    version: key_metadata.version,
                                    crc: key_metadata.crc,
                                    creation_time: Some(Timestamp::from(SystemTime::now())),
                                }),
                            }
                        })
                        .collect(),
                })
            })
            .map_err(|err| {
                error!("failed to list keys");
                Status::new(Code::Internal, "internal error")
            })
    }

    async fn delete(&self, request: Request<DeleteKeyRequest>) -> Result<Response<()>, Status> {
        todo!()
    }

    async fn migrate_to_new_node(
        &self,
        request: Request<MigrateToNewNodeRequest>,
    ) -> Result<Response<()>, Status> {
        todo!()
    }
}
