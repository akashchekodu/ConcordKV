use tonic::{transport::Server, Request, Response, Status};

pub mod kvstore {
    tonic::include_proto!("kvstore"); // Matches proto `package kvstore`
}

use kvstore::kv_store_server::{KvStore, KvStoreServer};
use kvstore::{PingRequest, PingResponse};

#[derive(Default)]
pub struct KvStoreService {}

#[tonic::async_trait]
impl KvStore for KvStoreService {
    async fn ping(&self, request: Request<PingRequest>) -> Result<Response<PingResponse>, Status> {
        let msg = request.into_inner().message;
        Ok(Response::new(PingResponse {
            reply: format!("Pong: {}", msg),
        }))
    }
}

pub async fn serve() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse()?;
    let service = KvStoreService::default();

    println!("KVStoreServer listening on {}", addr);
    Server::builder()
        .add_service(KvStoreServer::new(service))
        .serve(addr)
        .await?;

    Ok(())
}
