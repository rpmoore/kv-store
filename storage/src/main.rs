mod auth;
mod lookup;
mod partition;

use std::error::Error;
use std::path::Path;
use auth::AuthInterceptor;
use common::auth::{Identity, JwtValidator, RsaJwtValidator};
use common::read_file_bytes;
use common::storage::{
    storage_server::Storage, storage_server::StorageServer, CreateNamespaceRequest,
    DeleteKeyRequest, DeleteNamespaceRequest, GetRequest, GetResponse, KeyMetadata,
    ListKeysRequest, ListKeysResponse, MigrateToNewNodeRequest, PutRequest, PutResponse,
};
use crc32fast::Hasher;
use lookup::PartitionLookup;
use partition::ListOptions;
use partition::{Key, PutValue, Error as PError};
use prost_types::Timestamp;
use rayon::prelude::*;
use std::time::SystemTime;
use tonic::service::Interceptor;
use tonic::{transport::Server, Code, Request, Response, Status};
use tracing::{error, info, warn, Level};
use tracing_attributes::instrument;
use uuid::Uuid;
use futures::future::join_all;
use futures::{FutureExt, TryFutureExt};
use tracing_subscriber::fmt::format::FmtSpan;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    if cfg!(debug_assertions) {
    tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .with_target(true)
        .with_span_events(FmtSpan::CLOSE)
        .with_thread_names(true)
        .with_file(true)
        .init();
        } else {
        tracing_subscriber::fmt()
            .json()
            .with_max_level(Level::INFO)
            .with_target(true)
            .with_span_events(FmtSpan::CLOSE)
            .with_thread_names(true)
            .with_file(true)
            .init();
    }

    let addr = "[::1]:50051".parse()?;

    let private_key = read_file_bytes("key.pub")?;

    let validator = RsaJwtValidator::new(private_key.as_slice())?;

    let interceptor = AuthInterceptor::new(validator);

    /*
    // replace with a real namespace in the future that belongs to a specific tenant
    let partition = Partition::new(
        Uuid::parse_str("17f8457b-bf2b-4788-9cbc-2043a5fbad14").unwrap(),
        Uuid::parse_str("9cafb784-ae2f-49a2-800e-e7fafeffabad").unwrap(),
        Uuid::parse_str("afd98cbf-040e-4a4c-b398-26bbc1d492d5").unwrap(),
        "namespaces",
    )?;

    let partition2 = Partition::new(
        Uuid::parse_str("3921ca4a-2bd5-479d-9dea-2fe1f6835a2c").unwrap(),
        Uuid::parse_str("9cafb784-ae2f-49a2-800e-e7fafeffabad").unwrap(),
        Uuid::parse_str("afd98cbf-040e-4a4c-b398-26bbc1d492d5").unwrap(),
        "namespaces",
    )?;
     */

    let server = NodeStorageServer::new(Path::new("namespaces"))?;
    //server.partition_lookup.add_partition(partition)?;
    //server.partition_lookup.add_partition(partition2)?;

    Server::builder()
        .add_service(StorageServer::with_interceptor(server, interceptor))
        .serve(addr)
        .await?;
    Ok(())
}

#[derive(Debug)]
struct NodeStorageServer {
    partition_lookup: PartitionLookup,
}

impl NodeStorageServer {
    fn new(config: impl AsRef<Path>) -> Result<NodeStorageServer, Box<dyn Error>> {
        let partition_lookup = PartitionLookup::load(config)?; // should move this out
        Ok(NodeStorageServer { partition_lookup })
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

        let key: Key = (&request.key).into();

        let partition = self
            .partition_lookup
            .get_partition_for_key(identity.tenant_id(), namespace_id, &key)
            .ok_or(Status::new(Code::NotFound, "partition not found"))?;

        match partition.put(
            key,
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

    #[instrument(skip(self, request) fields(namespace_id = %request.get_ref().namespace_id))]
    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        let identity = request.extensions().get::<Identity>().unwrap();

        let request = request.get_ref();

        info!(
            uuid = identity.tenant_id().to_string(),
            "got request to get data"
        );

        let namespace_id = match Uuid::parse_str(&request.namespace_id) {
            Ok(id) => id,
            Err(err) => {
                error!(err = err.to_string(), "failed to parse uuid");
                return Err(Status::new(Code::InvalidArgument, "invalid uuid"));
            }
        };

        let key: Key = (&request.key).into();

        let partition = self
            .partition_lookup
            .get_partition_for_key(identity.tenant_id(), namespace_id, &key)
            .ok_or(Status::new(Code::NotFound, "partition not found"))?;

        match partition.get(&key) {
            Ok(value) => Ok(Response::new(GetResponse {
                key: key.into(),
                value: value.value,
                metadata: Some(common::storage::Metadata {
                    version: value.version,
                    crc: value.crc,
                    creation_time: Some(Timestamp::from(SystemTime::now())),
                }),
            })),
            Err(err) => {
                error!(err = err.to_string(), "failed to get value");
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

    #[instrument(skip(self, request) fields(namespace_id = %request.get_ref().namespace_id))]
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

        let Some(partitions) = self.partition_lookup.partitions(
            identity.tenant_id(),
            Uuid::parse_str(&request.namespace_id).unwrap(),
        ) else {
            return Ok(Response::new(ListKeysResponse::default())); // if there are no partitions return an empty list
        };
        // todo see if we can use rayon here, I ran into some issues with not being able to map the data in inner iterator and then return that back

        let futures = partitions.iter().map(|partition| async move {
            let result_set = partition.list_keys(ListOptions::default())?;
            let mut keys = Vec::new();
            for metadata in result_set.as_ref() {
                let key_metadata = metadata.metadata.as_ref().unwrap();
                keys.push(KeyMetadata {
                    key: metadata.key.clone(),
                    metadata: Some(common::storage::Metadata {
                        version: key_metadata.version,
                        crc: key_metadata.crc,
                        creation_time: Some(Timestamp::from(SystemTime::now())),
                    }),
                });
            }

            Ok::<Vec<KeyMetadata>, PError>(keys)
        });

        let mut keys = Vec::new();

        for result_set in join_all(futures).await.iter() {
            match result_set {
                Ok(result_set) => keys.extend_from_slice(result_set.as_slice()),
                Err(err) => {
                    error!(err = format!("err: {}", err), "failed to list keys");
                    return Err(Status::new(Code::Internal, "internal error"));
                }
            }
        }

        Ok(Response::new(ListKeysResponse { keys }))
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
