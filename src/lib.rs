use tonic::{transport::Server, Request, Response, Status};
use tonic_reflection::server::Builder as ReflectionBuilder;
mod storage;
use storage::MemTable;
use kvstore::{PingRequest, PingResponse, PutRequest, PutResponse, GetRequest, GetResponse};


pub mod kvstore {
    tonic::include_proto!("kvstore");

    pub const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("kvstore_descriptor");
}


use kvstore::kv_store_server::{KvStore, KvStoreServer};
use kvstore::{PingRequest, PingResponse};

#[derive(Clone)]
pub struct KvStoreService {
    memtable: MemTable,
}

impl KvStoreService {
    pub fn new() -> Self {
        Self {
            memtable: MemTable::default(),
        }
    }
}

#[tonic::async_trait]
impl KvStore for KvStoreService {
    async fn ping(&self, request: Request<PingRequest>) -> Result<Response<PingResponse>, Status> {
        let msg = request.into_inner().message;
        Ok(Response::new(PingResponse {
            reply: format!("Pong: {}", msg),
        }))
    }

    async fn put(&self, request: Request<PutRequest>) -> Result<Response<PutResponse>, Status> {
        let req = request.into_inner();
        self.memtable.put(req.key, req.value);
        Ok(Response::new(PutResponse { success: true }))
    }

    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        let key = request.into_inner().key;
        match self.memtable.get(&key) {
            Some(value) => Ok(Response::new(GetResponse {
                value,
                found: true,
            })),
            None => Ok(Response::new(GetResponse {
                value: vec![],
                found: false,
            })),
        }
    }
}


pub async fn serve() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "0.0.0.0:50051".parse()?; // IPv4
    let service = KvStoreService::new();

    println!("KVStoreServer listening on {}", addr);
    let reflection = ReflectionBuilder::configure()
        .register_encoded_file_descriptor_set(kvstore::FILE_DESCRIPTOR_SET)
        .build()
        .unwrap();

    Server::builder()
        .add_service(KvStoreServer::new(service))
        .add_service(reflection) // 👈 Add this line
        .serve(addr)
        .await?;


    Ok(())
}
