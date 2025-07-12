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
        let mut n = self.node.lock().unwrap();

        // Step 1: Reject if leader term is stale
        if req.term < n.current_term {
            return Ok(Response::new(AppendEntriesResponse {
                term: n.current_term,
                success: false,
            }));
        }

        // Step 2: Reject if no match at prev_log_index
        if req.prev_log_index as usize > n.log.len() {
            return Ok(Response::new(AppendEntriesResponse {
                term: n.current_term,
                success: false,
            }));
        }

        if let Some(prev_entry) = n.log.get(req.prev_log_index as usize) {
            if prev_entry.term != req.prev_log_term {
                return Ok(Response::new(AppendEntriesResponse {
                    term: n.current_term,
                    success: false,
                }));
            }
        } else if req.prev_log_index > 0 {
            return Ok(Response::new(AppendEntriesResponse {
                term: n.current_term,
                success: false,
            }));
        }

        // Step 3: Conflict detection
let start_index = req.prev_log_index as usize + 1;
let mut conflict_index = None;

for (i, entry) in req.entries.iter().enumerate() {
    let index = start_index + i;

    if let Some(existing) = n.log.get(index) {
        if existing.term != entry.term {
            conflict_index = Some(index);
            break;
        }
    } else {
        conflict_index = Some(index);
        break;
    }
}

let append_from = conflict_index.unwrap_or(n.log.len());
n.log.truncate(append_from);

// Step 3 (continued): Append new entries from conflict point
for (i, entry) in req.entries.iter().enumerate() {
    let index = start_index + i;

    if conflict_index.map_or(false, |conflict_at| index >= conflict_at) {
        let proto_cmd = Command::decode(&*entry.command)
            .map_err(|e| Status::internal(format!("command decode error: {}", e)))?;

        let cmd = InternalCommand::from_proto(proto_cmd);
        n.log.push(LogEntryWithCommand {
            term: entry.term,
            cmd,
        });
    }
}

// ✅ Step 4: Advance commit index and apply unapplied entries
let leader_commit = req.leader_commit as usize;
let new_commit = std::cmp::min(leader_commit, n.log.len());

while n.last_applied < new_commit {
    let cmd = n.log[n.last_applied].cmd.clone();
    self.engine.apply_command(cmd);
    n.last_applied += 1;
}

// ✅ Step 5: Update term if leader is newer
if req.term > n.current_term {
    n.become_follower(req.term);
}

Ok(Response::new(AppendEntriesResponse {
    term: n.current_term,
    success: true,
}))

    }
}
