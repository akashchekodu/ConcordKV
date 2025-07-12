// src/raft/server.rs

use tonic::{Request, Response, Status};
use crate::raft_proto::{
    raft_server::Raft,
    RequestVoteRequest, RequestVoteResponse,
    AppendEntriesRequest, AppendEntriesResponse,
};



use crate::raft::RaftNode;
use crate::storage::KVEngine;
use std::sync::{Arc, Mutex};

/// Your gRPC server implementation
pub struct RaftGrpcServer {
    node: Arc<Mutex<RaftNode>>,
    engine: Arc<KVEngine>,
}

impl RaftGrpcServer {
    pub fn new(node: Arc<Mutex<RaftNode>>, engine: Arc<KVEngine>) -> Self {
        Self { node, engine }
    }
}

#[tonic::async_trait]
impl Raft for RaftGrpcServer {
    async fn request_vote(
        &self,
        request: Request<RequestVoteRequest>,
    ) -> Result<Response<RequestVoteResponse>, Status> {
        let req = request.into_inner();
        let mut n = self.node.lock().unwrap();
        let grant = n.handle_vote_request(req.term, req.candidate_id);
        Ok(Response::new(RequestVoteResponse {
            term: n.current_term,
            vote_granted: grant,
        }))
    }

    async fn append_entries(
        &self,
        request: Request<AppendEntriesRequest>,
    ) -> Result<Response<AppendEntriesResponse>, Status> {
        let req = request.into_inner();
        let mut n = self.node.lock().unwrap();
        // … your log‐append + apply logic here …
        println!(
            "[RPC] AppendEntries(term={}, entries={})",
            req.term,
            req.entries.len()
        );
        Ok(Response::new(AppendEntriesResponse {
            term: req.term,
            success: true,
        }))
    }
}
