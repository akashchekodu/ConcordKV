use tonic::{transport::Server, Request, Response, Status};
mod storage;
use tonic_reflection::server::Builder as ReflectionBuilder;

pub mod kvstore {
    tonic::include_proto!("kvstore");
    // Pull in the file the build script just generated:
    pub const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("kvstore_descriptor");
}


use kvstore::kv_store_server::{KvStore, KvStoreServer};
use kvstore::{PingRequest, PingResponse, PutRequest, PutResponse, GetRequest, GetResponse};

use storage::{MemTable, WriteAheadLog};

#[derive(Clone)]
pub struct KvStoreService {
    memtable: MemTable,
    wal: WriteAheadLog,
}

impl KvStoreService {
    pub fn new(log_path: impl AsRef<std::path::Path>) -> Self {
        // 1) Open or create the WAL
        let wal = WriteAheadLog::new(log_path.as_ref()).expect("failed to open WAL");
        // 2) Rebuild in-memory state from existing log
        let memtable = {
            let mut m = MemTable::default();
            if let Ok(entries) = WriteAheadLog::replay(log_path.as_ref()) {
                for (k, v) in entries {
                    m.put(k, v);
                }
            }
            m
        };
        Self { memtable, wal }
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
        let PutRequest { key, value, .. } = request.into_inner();

        // 1) Persist to WAL
        self.wal
            .append(&key, &value)
            .map_err(|e| Status::internal(format!("WAL error: {}", e)))?;
        // 2) Then update in-memory state
        self.memtable.put(key.clone(), value.clone());

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
    let addr = "0.0.0.0:50051".parse()?; // gRPC server address

    let service = KvStoreService::new("wal.log"); // ⬅️ pass log path, not WriteAheadLog directly

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
