use common::storage::storage_client::StorageClient;
use tonic::transport::Channel;

#[derive(Debug, Default)]
pub struct ConnectionManager {
    connections: Vec<StorageClient<Channel>>,
}

impl ConnectionManager {
    pub fn get_conn(&self, index: usize) -> Option<&StorageClient<Channel>> {
        self.connections.get(index)
    }

    pub fn new_conn(&mut self, client: StorageClient<Channel>) {
        self.connections.push(client)
    }
}
