// /src/lib.rs
use tonic::{transport::Server, Request, Response, Status};
use tonic_reflection::server::Builder as ReflectionBuilder;

// These constants pull in the compiled descriptor sets from OUT_DIR:
const RAFT_DESCRIPTOR: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/raft_descriptor.bin"));
const KVSTORE_DESCRIPTOR: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/kvstore_descriptor.bin"));

pub mod kvstore {
    tonic::include_proto!("kvstore");
}

pub mod raft_proto {
    tonic::include_proto!("raft");
}
pub mod raft;   // your Raft logic and gRPC server stub
pub mod storage;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use storage::engine::KVEngine;
use raft::RaftNode;
use raft::server::RaftGrpcServer;


// kvstore protobuf


use kvstore::kv_store_server::{KvStore, KvStoreServer};
use kvstore::{
    PingRequest, PingResponse,
    PutRequest, PutResponse,
    GetRequest, GetResponse,
    DeleteRequest, DeleteResponse,
    ScanRequest, ScanResponse, KeyValue,
};

// Bring in the generated Raft gRPC types from the .proto


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

        // Spin up your KV engine and Raft node:
    let engine = Arc::new(KVEngine::new("wal.log", "sstables"));
    let raft_node = Arc::new(Mutex::new(RaftNode::new_with_timeout(1, Duration::from_millis(300))));

    let kv_service = KvStoreService { engine: Arc::clone(&engine) };

    // now this matches our updated constructor
    let raft_service = RaftGrpcServer::new(
        Arc::clone(&raft_node),
        Arc::clone(&engine),
    );


    // Build the reflection service using the two descriptor blobs:
    let reflection = ReflectionBuilder::configure()
        .register_encoded_file_descriptor_set(RAFT_DESCRIPTOR)
        .register_encoded_file_descriptor_set(KVSTORE_DESCRIPTOR)
        .build()?;

    println!("Server listening on {}", addr);
    Server::builder()
    .add_service(KvStoreServer::new(kv_service))
    .add_service(raft_proto::raft_server::RaftServer::new(raft_service))
    .add_service(reflection)
    .serve(addr)
    .await?;
    Ok(())
}
