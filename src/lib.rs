use tonic::{transport::Server, Request, Response, Status};
use tonic_reflection::server::Builder as ReflectionBuilder;
pub mod raft;         // ✅ Add this at top-level
pub mod storage;
use storage::engine::KVEngine;
use std::sync::Arc;


pub mod kvstore {
    tonic::include_proto!("kvstore");
    pub const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("kvstore_descriptor");
}

use kvstore::kv_store_server::{KvStore, KvStoreServer};
use kvstore::{
    PingRequest, PingResponse,
    PutRequest, PutResponse,
    GetRequest, GetResponse,
    DeleteRequest, DeleteResponse,
    ScanRequest, ScanResponse, KeyValue,
};

#[derive(Clone)]
pub struct KvStoreService {
    engine: Arc<KVEngine>,
}

impl KvStoreService {
    pub fn new(log_path: impl AsRef<std::path::Path>) -> Self {
        let engine = Arc::new(KVEngine::new(log_path, "sstables"));
        Self { engine }
    }
}


#[tonic::async_trait]
impl KvStore for KvStoreService {
    async fn ping(&self, request: Request<PingRequest>) -> Result<Response<PingResponse>, Status> {
        let msg = request.into_inner().message;
        Ok(Response::new(PingResponse { reply: format!("Pong: {}", msg) }))
    }

    async fn put(&self, request: Request<PutRequest>) -> Result<Response<PutResponse>, Status> {
        let PutRequest { key, value } = request.into_inner();
        self.engine
            .put(key, value)
            .map_err(|e| Status::internal(format!("Put error: {}", e)))?;
        Ok(Response::new(PutResponse { success: true }))
    }

    async fn delete(&self, request: Request<DeleteRequest>) -> Result<Response<DeleteResponse>, Status> {
        let key = request.into_inner().key;
        self.engine
            .delete(key)
            .map_err(|e| Status::internal(format!("Delete error: {}", e)))?;
        Ok(Response::new(DeleteResponse { success: true }))
    }

    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        let key = request.into_inner().key;
        match self.engine.get(&key) {
            Some(v) => Ok(Response::new(GetResponse { value: v, found: true })),
            None    => Ok(Response::new(GetResponse { value: vec![], found: false })),
        }
    }

    async fn scan(&self, request: Request<ScanRequest>) -> Result<Response<ScanResponse>, Status> {
        let prefix = request.into_inner().prefix;
        let results = self.engine.scan(&prefix);
        let pairs = results
            .into_iter()
            .map(|(k, v)| KeyValue { key: k, value: v })
            .collect();
        Ok(Response::new(ScanResponse { pairs }))
    }
}

pub async fn serve() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "0.0.0.0:50051".parse()?;
    let service = KvStoreService::new("wal.log");

    println!("KVStoreServer listening on {}", addr);
    let reflection = ReflectionBuilder::configure()
        .register_encoded_file_descriptor_set(kvstore::FILE_DESCRIPTOR_SET)
        .build()?;

    Server::builder()
        .add_service(KvStoreServer::new(service))
        .add_service(reflection)
        .serve(addr)
        .await?;

    Ok(())
}
