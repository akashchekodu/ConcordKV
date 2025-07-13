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

    async fn append_entries(
        &self,
        request: Request<AppendEntriesRequest>,
    ) -> Result<Response<AppendEntriesResponse>, Status> {
        let req = request.into_inner();
        let mut node = self.node.lock().unwrap();

        // Step 1: stale-term check
        if req.term < node.current_term {
            return Ok(Response::new(AppendEntriesResponse {
                term: node.current_term,
                success: false,
            }));
        }

        // Step 2: step down on higher term
        if req.term > node.current_term {
            node.become_follower(req.term);
        }

        // Step 3: consistency check
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

        // Step 4: conflict detection & overwrite
        let start = prev_index + 1;
        let mut conflict_at = start;
        for (i, ent) in req.entries.iter().enumerate() {
            let idx = start + i;
            if let Some(existing) = node.log.get(idx) {
                if existing.term != ent.term {
                    break;
                }
                conflict_at += 1;
            } else {
                break;
            }
        }
        node.log.truncate(conflict_at);

        // Step 5: append new entries beyond conflict point
        for ent in &req.entries[(conflict_at - start)..] {
            let proto = Command::decode(&*ent.command)
                .map_err(|e| Status::internal(format!("decode error: {}", e)))?;
            let cmd = InternalCommand::from_proto(proto);
            node.log.push(LogEntryWithCommand {
                term: ent.term,
                cmd,
            });
        }

        // Step 6: advance commit index & apply
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

    async fn request_vote(
        &self,
        request: Request<RequestVoteRequest>,
    ) -> Result<Response<RequestVoteResponse>, Status> {
        let req = request.into_inner();
        let mut node = self.node.lock().unwrap();

        if req.term < node.current_term {
            return Ok(Response::new(RequestVoteResponse {
                term: node.current_term,
                vote_granted: false,
            }));
        }

        if req.term > node.current_term {
            node.become_follower(req.term);
        }

        let vote_granted = node.voted_for.is_none() || node.voted_for == Some(req.candidate_id);
        if vote_granted {
            node.voted_for = Some(req.candidate_id);
        }

        Ok(Response::new(RequestVoteResponse {
            term: node.current_term,
            vote_granted,
        }))
    }
}
