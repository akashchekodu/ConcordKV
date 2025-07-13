use rand::Rng;
use std::collections::HashMap;
use tonic::Request;
use futures::future::join_all;
use std::sync::{Arc, Mutex};
// Prefer standard Duration by default (used in most structs and timers)
use std::time::{Duration as StdDuration, Instant};

// Then, for tokio-specific interval timers:
use tokio::time::{interval, Duration as TokioDuration};

use crate::storage::KVEngine;
use crate::raft::command::Command;
use crate::raft::log::LogEntryWithCommand;
use crate::raft_proto::raft_client::RaftClient;
use tonic::transport::Channel;
use crate::raft_proto::{RequestVoteRequest, RequestVoteResponse};

use crate::raft_proto::{AppendEntriesRequest, AppendEntriesResponse, LogEntry};
use prost::Message; // needed for encoding Command

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RaftRole {
    Follower,
    Candidate,
    Leader,
}

pub struct RaftNode {
    pub id: u64,
    pub current_term: u64,
    pub voted_for: Option<u64>,
    pub role: RaftRole,
    pub last_heartbeat: Instant,
    pub election_timeout: StdDuration,
    pub log: Vec<LogEntryWithCommand>,
    pub commit_index: usize,
    pub last_applied: usize,
    pub next_index: HashMap<u64, usize>,
    pub match_index: HashMap<u64, usize>,
    pub peers: HashMap<u64, RaftClient<Channel>>, // 👈 NEW
    pub engine: Arc<KVEngine>, // ✅ Add this

}

impl RaftNode {
    pub async fn with_peers(
        id: u64,
        peer_addresses: &HashMap<u64, String>,
        timeout: StdDuration,
        engine: Arc<KVEngine>
    ) -> Self {
        let mut node = Self {
            id,
            current_term: 0,
            voted_for: None,
            role: RaftRole::Follower,
            last_heartbeat: Instant::now(),
            election_timeout: timeout,
            log: Vec::new(),
            commit_index: 0,
            last_applied: 0,
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            peers: HashMap::new(), // 👈 You need to add this to your struct
           engine,
        };

        for (&peer_id, addr) in peer_addresses.iter() {
            if peer_id == id {
                continue; // skip self
            }

            match RaftClient::connect(format!("http://{}", addr)).await {
                Ok(client) => {
                    node.peers.insert(peer_id, client);
                }
                Err(e) => {
                    eprintln!("Failed to connect to peer {} at {}: {}", peer_id, addr, e);
                }
            }
        }

        node
    }

    pub fn new(id: u64, engine: Arc<KVEngine>) -> Self {
        let timeout = StdDuration::from_millis(rand::thread_rng().gen_range(150..300));
        Self::new_with_timeout(id, timeout, engine)
    }


    pub fn new_with_timeout(id: u64, timeout: StdDuration,engine: Arc<KVEngine>) -> Self {
        Self {
            id,
            current_term: 0,
            voted_for: None,
            role: RaftRole::Follower,
            last_heartbeat: Instant::now(),
            election_timeout: timeout,
            log: Vec::new(),
            commit_index: 0,
            last_applied: 0,
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            peers: HashMap::new(), 
            engine,
        }
    }

    pub fn simulate_append_entries(&mut self, peers: &mut [RaftNode])

 {
        // Only leader should call this
        if self.role != RaftRole::Leader {
            return;
        }

        for peer in peers.iter_mut().filter(|p| p.id != self.id) {
            let peer_id = peer.id;

            let next_index = self.next_index.get(&peer_id).copied().unwrap_or(1);
            let prev_log_index = if next_index > 0 { next_index - 1 } else { 0 };
            let prev_log_term = if prev_log_index < self.log.len() {
                self.log[prev_log_index].term
            } else {
                0
            };

            let entries: Vec<LogEntry> = self.log
                .iter()
                .skip(next_index)
                .map(|entry| {
                    LogEntry {
                        term: entry.term,
                        command: entry.cmd.to_proto().encode_to_vec(),
                    }
                })
                .collect();

            let request = AppendEntriesRequest {
                term: self.current_term,
                leader_id: self.id,
                prev_log_index: prev_log_index as u64,
                prev_log_term,
                entries,
                leader_commit: self.commit_index as u64,
            };

            // Simulate sending RPC (for now, call peer.handle_append_entries directly)
            let response = peer.handle_append_entries(&request);

            if response.success {
                // update match_index and next_index
                let new_match_index = prev_log_index + request.entries.len();
                self.match_index.insert(peer_id, new_match_index);
                self.next_index.insert(peer_id, new_match_index + 1);
            } else if response.term > self.current_term {
                self.become_follower(response.term);
                return;
            } else {
                // Decrement next_index for retry
                let next = self.next_index.get_mut(&peer_id).unwrap();
                if *next > 1 {
                    *next -= 1;
                }
            }
        }
        self.update_commit_index();
    }

    pub fn update_commit_index(&mut self) {
        if self.role != RaftRole::Leader {
            return;
        }
    }
    
    pub fn send_append_entries_to_peers(&mut self) {
        let term = self.current_term;
        let leader_id = self.id;
        let commit_index = self.commit_index as u64;

        for (&peer_id, client) in self.peers.iter_mut() {
            let log = self.log.clone(); // avoid borrow issues
            let next_index = *self.next_index.get(&peer_id).unwrap_or(&0);
            let prev_log_index = if next_index > 0 { next_index - 1 } else { 0 };
            let prev_log_term = log.get(prev_log_index).map(|e| e.term).unwrap_or(0);

            let entries: Vec<LogEntry> = log
                .iter()
                .skip(next_index)
                .map(|entry| LogEntry {
                    term: entry.term,
                    command: entry.cmd.to_proto().encode_to_vec(),
                })
                .collect();

            let request = AppendEntriesRequest {
                term,
                leader_id,
                prev_log_index: prev_log_index as u64,
                prev_log_term,
                entries,
                leader_commit: commit_index,
            };

            // Clone values for move into async
            let mut client_clone = client.clone();
            let request_clone = request.clone();
            let peer_id_copy = peer_id;

            tokio::spawn(async move {
                match client_clone.append_entries(Request::new(request_clone)).await {
                    Ok(resp) => {
                        let resp = resp.into_inner();
                        println!("AppendEntries to peer {} success: {}", peer_id_copy, resp.success);

                        // NOTE: Updating match_index & next_index would need Arc<Mutex<RaftNode>>
                    }
                    Err(e) => {
                        eprintln!("AppendEntries to peer {} failed: {:?}", peer_id_copy, e);
                    }
                }
            });
        }

        // 👇 Handle commit advancement (after all async sends)
        let mut match_indexes: Vec<usize> = self.match_index.values().copied().collect();
        match_indexes.push(self.log.len().saturating_sub(1)); // self's own log index

        match_indexes.sort_unstable_by(|a, b| b.cmp(a));
        let majority_index = match_indexes[match_indexes.len() / 2];

        if majority_index < self.log.len()
            && self.log[majority_index].term == self.current_term
            && majority_index > self.commit_index
        {
            println!(
                "[Node {}] Advancing commit_index to {} (term = {})",
                self.id, majority_index, self.current_term
            );
            self.commit_index = majority_index;

            while self.last_applied < self.commit_index {
                self.last_applied += 1;
                let cmd = self.log[self.last_applied].cmd.clone();
                self.apply(cmd);
            }
        }
    }

    fn all_peer_ids(&self) -> Vec<u64> {
        vec![1, 2, 3].into_iter().filter(|&id| id != self.id).collect()
    }
    
    pub fn is_election_timeout(&self) -> bool {
        self.last_heartbeat.elapsed() >= self.election_timeout
    }

    pub fn apply(&self, cmd: Command) {
        let engine = &*self.engine; 
        match cmd {
            Command::Put { key, value } => {
                let _ = self.engine.put(key, value);
            }
            Command::Delete { key } => {
                let _ = self.engine.delete(key);
            }
        }
    }

    pub fn propose(&mut self, cmd: Command) {
        if self.role != RaftRole::Leader {
            println!("[Node {}] Rejecting propose — not leader", self.id);
            return;
        }

        println!("[Node {}] Proposing command: {:?}", self.id, cmd);
        self.log.push(LogEntryWithCommand {
            term: self.current_term,
            cmd: cmd.clone(),
        });
        self.apply( cmd);
    }

    pub fn become_candidate(&mut self) {
        self.role = RaftRole::Candidate;
        self.current_term += 1;
        self.voted_for = Some(self.id);
        self.last_heartbeat = Instant::now();
        println!("[Node {}] Became CANDIDATE in term {}", self.id, self.current_term);
    }

    pub fn become_leader(&mut self) {
        self.role = RaftRole::Leader;

        // Initialize replication tracking
        let last_log_index = self.log.len();
        self.next_index = Default::default();
        self.match_index = Default::default();

        for peer_id in self.all_peer_ids() {
            self.next_index.insert(peer_id, last_log_index);
            self.match_index.insert(peer_id, 0); // Nothing replicated yet
        }

        println!("[Node {}] Became LEADER in term {}", self.id, self.current_term);
    }



    pub fn become_follower(&mut self, term: u64) {
        self.role = RaftRole::Follower;
        self.current_term = term;
        self.voted_for = None;
        self.last_heartbeat = Instant::now();
        println!("[Node {}] Became FOLLOWER in term {}", self.id, self.current_term);
    }

    pub async fn send_vote_requests(&mut self) {
        let mut votes = 1; // vote for self
        let term = self.current_term;
        let candidate_id = self.id;

        println!("[Node {}] Sending RequestVote RPCs to peers...", self.id);

        let futures = self.peers.iter_mut().map(|(&peer_id, client)| {
            let mut client = client.clone();
            let last_log_index = self.log.len().saturating_sub(1) as u64;
            let last_log_term = if self.log.is_empty() {
                0
            } else {
                self.log[last_log_index as usize].term
            };
            let request = crate::raft_proto::RequestVoteRequest {
                term,
                candidate_id,
                last_log_index,
                last_log_term,
            };

            async move {
                match client.request_vote(Request::new(request)).await {
                    Ok(response) => {
                        let vote_granted = response.into_inner().vote_granted;
                        println!("[Node {}] Peer {} responded with vote_granted = {}", candidate_id, peer_id, vote_granted);
                        vote_granted
                    }
                    Err(e) => {
                        eprintln!("[Node {}] Failed to send RequestVote to peer {}: {}", candidate_id, peer_id, e);
                        false
                    }
                }
            }
        });

        // Wait for all votes
        let results = futures::future::join_all(futures).await;
        for granted in results {
            if granted {
                votes += 1;
            }
        }

        let majority = (self.peers.len() + 1) / 2 + 1;
        if votes >= majority {
            println!("[Node {}] Won election with {}/{} votes", self.id, votes, self.peers.len() + 1);
            self.become_leader();
        } else {
            println!("[Node {}] Lost election with {}/{} votes", self.id, votes, self.peers.len() + 1);
            self.role = RaftRole::Follower;
        }
    }



    pub fn send_heartbeats(&mut self, peers: &mut [RaftNode]) {
        println!("[Node {}] Sending heartbeat (AppendEntries RPCs)...", self.id);
        self.send_append_entries_to_peers(); // 👈 simulate real RAFT interaction
    }



    pub fn handle_vote_request(&mut self, term: u64, candidate_id: u64) -> bool {
        if term < self.current_term {
            return false;
        }

        if term > self.current_term {
            self.become_follower(term);
        }

        if self.voted_for.is_none() || self.voted_for == Some(candidate_id) {
            self.voted_for = Some(candidate_id);
            true
        } else {
            false
        }
    }

    pub fn handle_heartbeat(&mut self, term: u64, leader_id: u64) {
        if term < self.current_term {
            return;
        }

        if term > self.current_term || self.role != RaftRole::Follower {
            self.become_follower(term);
        }

        self.last_heartbeat = Instant::now();
        println!("[Node {}] Received heartbeat from leader {}", self.id, leader_id);
    }

    pub fn handle_append_entries(&mut self, req: &AppendEntriesRequest) -> AppendEntriesResponse {
        // Update heartbeat timestamp to prevent election timeout
        self.last_heartbeat = std::time::Instant::now();

        // Step 1: If term < currentTerm, reply false
        if req.term < self.current_term {
            return AppendEntriesResponse {
                term: self.current_term,
                success: false,
            };
        }

        // Step 2: If term > currentTerm, update self to follower
        if req.term > self.current_term {
            self.become_follower(req.term);
        }

        // Step 3: Check log consistency
        let prev_index = req.prev_log_index as usize;
        if prev_index >= self.log.len() {
            return AppendEntriesResponse {
                term: self.current_term,
                success: false,
            };
        }

        if self.log[prev_index].term != req.prev_log_term {
            return AppendEntriesResponse {
                term: self.current_term,
                success: false,
            };
        }

        // Step 4: Remove conflicting entries
        let mut index = prev_index + 1;
        for (i, entry) in req.entries.iter().enumerate() {
            if index < self.log.len() {
                if self.log[index].term != entry.term {
                    self.log.truncate(index);
                    break;
                }
            }
            index += 1;
        }

        // Step 5: Append any new entries not already in the log
        for (i, entry) in req.entries.iter().enumerate() {
            let insert_index = prev_index + 1 + i;
            if insert_index >= self.log.len() {
                if let Ok(cmd) = crate::raft::command::Command::from_proto_bytes(entry.command.as_slice()) {
                    self.log.push(LogEntryWithCommand {
                        term: entry.term,
                        cmd,
                    });
                }
            }
        }

        // Step 6: Update commit index
        let leader_commit = req.leader_commit as usize;
        if leader_commit > self.commit_index {
            self.commit_index = std::cmp::min(leader_commit, self.log.len().saturating_sub(1));
        }

        AppendEntriesResponse {
            term: self.current_term,
            success: true,
        }
    }

    pub fn check_election_timeout_and_maybe_become_candidate(&mut self) {
        if self.is_election_timeout() {
            println!("[Node {}] Election timeout. Becoming candidate...", self.id);
            self.start_election();
        }
    }

    pub fn check_election_timeout_and_maybe_restart_election(&mut self) {
        if self.is_election_timeout() {
            println!("[Node {}] Election timeout (as candidate). Restarting election...");
            self.start_election();
        }
    }

    pub async fn start_election(&mut self) {
        self.become_candidate();

        let mut votes = 1; // vote for self
        let term = self.current_term;
        let candidate_id = self.id;

        println!("[Node {}] Starting election for term {}", self.id, term);

        let vote_futures = self.peers.iter().map(|(&peer_id, client)| {
            let mut client = client.clone();
            let last_log_index = self.log.len().saturating_sub(1) as u64;
            let last_log_term = if self.log.is_empty() {
                0
            } else {
                self.log[last_log_index as usize].term
            };
            let request = crate::raft_proto::RequestVoteRequest {
                term,
                candidate_id,
                last_log_index,
                last_log_term,
            };


            async move {
                match client.request_vote(tonic::Request::new(request)).await {
                    Ok(response) => {
                        let resp = response.into_inner();
                        println!("[Node {}] Vote response from {}: {}", candidate_id, peer_id, resp.vote_granted);
                        resp.vote_granted
                    }
                    Err(err) => {
                        eprintln!("[Node {}] Failed to contact peer {} for vote: {}", candidate_id, peer_id, err);
                        false
                    }
                }
            }
        });

        // Wait for all responses
        let results = futures::future::join_all(vote_futures).await;
        for granted in results {
            if granted {
                votes += 1;
            }
        }

        let majority = (self.peers.len() + 1) / 2 + 1;
        if votes >= majority {
            println!("[Node {}] Won election with {}/{} votes", self.id, votes, self.peers.len() + 1);
            self.become_leader();
        } else {
            println!("[Node {}] Election failed with {}/{}", self.id, votes, self.peers.len() + 1);
            self.role = RaftRole::Follower;
        }
    }


    pub fn tick(&mut self, peers: &mut [RaftNode]) {
        if !self.is_election_timeout() {
            return;
        }

        match self.role {
            RaftRole::Follower => self.become_candidate(),
            RaftRole::Candidate => {
                self.become_candidate();
                self.send_vote_requests();
            }
            RaftRole::Leader => self.send_heartbeats(peers),
        }
    }

    /// Simulates sending heartbeats by directly calling handle_append_entries on test peers.
    pub fn simulate_send_heartbeats(&mut self, peers: &mut [RaftNode]) {
        for peer in peers.iter_mut() {
            let prev_log_index = if self.log.len() > 0 { self.log.len() - 1 } else { 0 };
            let prev_log_term = if self.log.len() > 0 {
                self.log[prev_log_index].term
            } else {
                0
            };

            let req = AppendEntriesRequest {
                term: self.current_term,
                leader_id: self.id,
                prev_log_index: prev_log_index as u64,
                prev_log_term,
                entries: vec![], // Heartbeat has no log entries
                leader_commit: self.commit_index as u64,
            };

            peer.handle_append_entries(&req);
        }
    }

}


pub async fn start_raft_main_loop(node: Arc<Mutex<RaftNode>>) {
    let mut ticker = interval(TokioDuration::from_millis(100));

    loop {
        ticker.tick().await;

        let mut node = node.lock().unwrap();
        node.tick(&mut peers);

        match node.role {
            RaftRole::Leader => {
                node.send_append_entries_to_peers();
            }
            RaftRole::Candidate => {
                node.check_election_timeout_and_maybe_restart_election();
            }
            RaftRole::Follower => {
                node.check_election_timeout_and_maybe_become_candidate();
            }
        }
    }
}



fn test_engine() -> Arc<KVEngine> {
    Arc::new(KVEngine::new("test_wal.log", "test_sstables"))
}


#[cfg(test)]
mod tests {
    use super::*;
    use std::thread::sleep;

    fn test_engine() -> Arc<KVEngine> {
        Arc::new(KVEngine::new("test_wal.log", "test_sstables"))
    }

    #[test]
    fn test_election_timeout_triggers_candidate() {
        let mut node = RaftNode::new_with_timeout(1, StdDuration::from_millis(100), test_engine());
        sleep(StdDuration::from_millis(200));
        assert!(node.is_election_timeout());

        node.become_candidate();
        assert_eq!(node.role, RaftRole::Candidate);
    }

    #[test]
    fn test_tick_promotes_to_candidate_after_timeout() {
        let mut nodes = vec![RaftNode::new_with_timeout(1, StdDuration::from_millis(100), test_engine())];
        std::thread::sleep(StdDuration::from_millis(200));

        let (first, rest) = nodes.split_at_mut(1);
        first[0].tick(rest);
        assert_eq!(first[0].role, RaftRole::Candidate);
    }

    #[test]
    fn test_candidate_wins_election_with_majority_votes() {
        let mut nodes: Vec<RaftNode> = (1..=5)
            .map(|id| RaftNode::new_with_timeout(id, StdDuration::from_millis(300), test_engine()))
            .collect();

        let (self_node, peers) = nodes.split_first_mut().unwrap();
        self_node.start_election(peers);
        assert_eq!(self_node.role, RaftRole::Leader);
    }

    #[test]
fn test_leader_sends_heartbeats_to_followers() {
    let mut nodes: Vec<RaftNode> = (1..=3)
        .map(|id| RaftNode::new_with_timeout(id, StdDuration::from_millis(300), test_engine()))
        .collect();

    let (leader, peers) = nodes.split_first_mut().unwrap();
    leader.become_leader();

    for peer in peers.iter_mut() {
        peer.last_heartbeat = Instant::now() - StdDuration::from_millis(500);
    }

    leader.simulate_send_heartbeats(peers); // ✅ simulation version

    for peer in peers {
        assert!(!peer.is_election_timeout(), "Peer {} should not timeout", peer.id);
        assert_eq!(peer.role, RaftRole::Follower);
    }
}


    #[test]
    fn test_candidate_retries_election_on_timeout() {
        let mut nodes: Vec<RaftNode> = (1..=3)
            .map(|id| RaftNode::new_with_timeout(id, StdDuration::from_millis(200), test_engine()))
            .collect();

        let (node, peers) = nodes.split_first_mut().unwrap();

        node.become_candidate();
        node.voted_for = Some(99);

        for peer in peers.iter_mut() {
            peer.current_term = node.current_term;
            peer.voted_for = Some(999);
        }

        std::thread::sleep(Duration::from_millis(250));
        node.tick(peers);

        assert_eq!(node.role, RaftRole::Leader);
        assert_eq!(node.voted_for, Some(node.id));
        assert!(node.current_term > 1, "Should increase term");
    }

    #[test]
    fn test_handle_vote_request_refuses_second_different_candidate() {
        let mut node = RaftNode::new_with_timeout(1, StdDuration::from_millis(100), test_engine());
        assert!(node.handle_vote_request(1, 42));
        assert!(!node.handle_vote_request(1, 17));
        assert_eq!(node.voted_for, Some(42));
    }

    #[test]
    fn test_handle_vote_request_honours_higher_term() {
        let mut node = RaftNode::new_with_timeout(1, StdDuration::from_millis(100), test_engine());
        node.become_candidate();
        assert!(node.handle_vote_request(2, 99));
        assert_eq!(node.role, RaftRole::Follower);
        assert_eq!(node.current_term, 2);
        assert_eq!(node.voted_for, Some(99));
    }

    #[test]
    fn test_handle_heartbeat_ignores_older_term() {
        let mut node = RaftNode::new_with_timeout(1, StdDuration::from_millis(100), test_engine());
        node.become_candidate();
        let old_term = node.current_term;
        node.handle_heartbeat(0, 5);
        assert_eq!(node.role, RaftRole::Candidate);
        assert_eq!(node.current_term, old_term);
    }

    #[test]
    fn test_handle_heartbeat_promotes_to_follower_on_term_advance() {
        let mut node = RaftNode::new_with_timeout(1, StdDuration::from_millis(100), test_engine());
        node.become_candidate();
        node.handle_heartbeat(2, 5);
        assert_eq!(node.role, RaftRole::Follower);
        assert_eq!(node.current_term, 2);
    }
}
