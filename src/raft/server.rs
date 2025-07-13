// src/raft/server.rs

use tonic::{Request, Response, Status};
use crate::raft_proto::{
    raft_server::Raft,
    RequestVoteRequest, RequestVoteResponse,
    AppendEntriesRequest, AppendEntriesResponse,
    LogEntry,
};
use crate::raft::{RaftNode, RaftRole};
use crate::storage::KVEngine;
use crate::raft_proto::Command;
use std::sync::{Arc, Mutex};
use prost::Message; // for decoding Command
use crate::raft::command::Command as InternalCommand;
use crate::raft::LogEntryWithCommand;
use tokio::time::{interval, Duration};

pub struct RaftGrpcServer {
    node: Arc<Mutex<RaftNode>>,
    engine: Arc<KVEngine>,
}

impl RaftGrpcServer {
    pub fn new(node: Arc<Mutex<RaftNode>>, engine: Arc<KVEngine>) -> Self {
        let cloned_node = node.clone();
        let cloned_engine = engine.clone();

        // 🫀 Spawn heartbeat loop (only leader sends)
        tokio::spawn(async move {
            let mut ticker = interval(Duration::from_millis(100)); // heartbeat every 100ms
            loop {
                ticker.tick().await;

                let mut n = cloned_node.lock().unwrap();
                if n.role == RaftRole::Leader {
                    n.send_append_entries_to_peers();
                }
            }
        });

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
        let vote = n.handle_vote_request(req.term, req.candidate_id);
        Ok(Response::new(RequestVoteResponse {
            term: n.current_term,
            vote_granted: vote,
        }))
    }

    async fn append_entries(
        &self,
        request: Request<AppendEntriesRequest>,
    ) -> Result<Response<AppendEntriesResponse>, Status> {
        let req = request.into_inner();
        let mut node = self.node.lock().unwrap();

        // 🧠 Step 1: Reject if term is stale
        if req.term < node.current_term {
            return Ok(Response::new(AppendEntriesResponse {
                term: node.current_term,
                success: false,
            }));
        }

        // 🧠 Step 2: Step down if term is newer
        if req.term > node.current_term {
            node.become_follower(req.term);
        }

        // 🧠 Step 3: Check log consistency with prev_log_index & prev_log_term
        let prev_index = req.prev_log_index as usize;
        let prev_ok = if prev_index == 0 {
            true
        } else if let Some(entry) = node.log.get(prev_index) {
            entry.term == req.prev_log_term
        } else {
            false
        };

        if !prev_ok {
            return Ok(Response::new(AppendEntriesResponse {
                term: node.current_term,
                success: false,
            }));
        }

        // 🧠 Step 4: Append new entries (resolving conflicts)
        let mut index = prev_index + 1;

        for entry in req.entries {
            if index < node.log.len() {
                // Conflict? Overwrite if term mismatches
                if node.log[index].term != entry.term {
                    node.log.truncate(index);
                }
            }

            if index >= node.log.len() {
                // Decode command
                let proto = Command::decode(&*entry.command)
                    .map_err(|e| Status::internal(format!("Failed to decode command: {}", e)))?;

                let cmd = InternalCommand::from_proto(proto);

                node.log.push(LogEntryWithCommand {
                    term: entry.term,
                    cmd,
                });
            }

            index += 1;
        }

        // 🧠 Step 5: Advance commit index and apply to state machine
        let leader_commit = req.leader_commit as usize;
        let new_commit = std::cmp::min(leader_commit, node.log.len().saturating_sub(1));

        if new_commit > node.commit_index {
            for i in (node.commit_index + 1)..=new_commit {
                let entry = &node.log[i];
                node.apply(entry.cmd.clone());
            }
            node.commit_index = new_commit;
        }

        Ok(Response::new(AppendEntriesResponse {
            term: node.current_term,
            success: true,
        }))
    }
}
