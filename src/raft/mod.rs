use std::time::{Duration, Instant};
use rand::Rng;
pub mod command;
use crate::raft::command::Command;
use crate::storage::KVEngine;


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
    pub election_timeout: Duration,
    pub log: Vec<Command>,  // <--- ADD THIS
}


impl RaftNode {
    pub fn new(id: u64) -> Self {
        let timeout = Duration::from_millis(rand::thread_rng().gen_range(150..300));
        Self::new_with_timeout(id, timeout)
    }

    pub fn new_with_timeout(id: u64, timeout: Duration) -> Self {
        Self {
            id,
            current_term: 0,
            voted_for: None,
            role: RaftRole::Follower,
            last_heartbeat: Instant::now(),
            election_timeout: timeout,
            log: vec![],  // <-- Initialize empty log
        }
    }

    pub fn is_election_timeout(&self) -> bool {
        self.last_heartbeat.elapsed() >= self.election_timeout
    }

    pub fn apply(&self, engine: &KVEngine, cmd: Command) {
        match cmd {
            Command::Put { key, value } => {
                let _ = engine.put(key, value);
            }
            Command::Delete { key } => {
                let _ = engine.delete(key);
            }
        }
    }

    pub fn propose(&mut self, cmd: Command, engine: &KVEngine) {
        if self.role != RaftRole::Leader {
            println!("[Node {}] Rejecting propose — not leader", self.id);
            return;
        }

        println!("[Node {}] Proposing command: {:?}", self.id, cmd);

        self.log.push(cmd.clone());      // Simulate append to replicated log
        self.apply(engine, cmd);         // Immediately apply (later we'll wait for quorum)
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
        println!("[Node {}] Became LEADER in term {}", self.id, self.current_term);
    }

    pub fn send_vote_requests(&mut self, peers: &mut [RaftNode]) {
        let mut votes = 1; // Vote for self

        for peer in peers.iter_mut().filter(|p| p.id != self.id) {
            if peer.handle_vote_request(self.current_term, self.id) {
                votes += 1;
            }
        }

        if votes > (peers.len() / 2) {
            self.become_leader();
        } else {
            println!("[Node {}] Election failed with {}/{} votes", self.id, votes, peers.len());
        }
    }

    pub fn send_heartbeats(&mut self, peers: &mut [RaftNode]) {
        for peer in peers.iter_mut().filter(|p| p.id != self.id) {
            peer.last_heartbeat = Instant::now();
        }
        println!("[Node {}] Sent heartbeats", self.id);
    }


    pub fn become_follower(&mut self, term: u64) {
        self.role = RaftRole::Follower;
        self.current_term = term;
        self.voted_for = None;
        self.last_heartbeat = Instant::now();
        println!("[Node {}] Became FOLLOWER in term {}", self.id, self.current_term);
    }

    pub fn tick(&mut self, peers: &mut [RaftNode]) {
        if !self.is_election_timeout() {
            return;
        }

        match self.role {
            RaftRole::Follower => {
                // First timeout: become candidate, but don't dispatch RPCs yet
                self.become_candidate();
            }

            RaftRole::Candidate => {
                // Subsequent timeout as candidate: start a fresh election
                self.become_candidate();           // bump term + vote for self
                self.send_vote_requests(peers);    // collect votes
            }

            RaftRole::Leader => {
                // Leader heartbeat timeout: proactively ping followers
                self.send_heartbeats(peers);
            }
        }
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

    /// Starts election and collects votes
    pub fn start_election(&mut self, peers: &mut [RaftNode]) {
        self.become_candidate();

        let mut votes = 1; // vote for self
        for peer in peers.iter_mut() {
            if peer.id == self.id {
                continue;
            }
            if peer.handle_vote_request(self.current_term, self.id) {
                votes += 1;
            }
        }

        let majority = peers.len() / 2 + 1;
        if votes >= majority {
            self.become_leader();
        } else {
            println!("[Node {}] Election failed with {} votes", self.id, votes);
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

}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread::sleep;

    #[test]
    fn test_election_timeout_triggers_candidate() {
        let mut node = RaftNode::new_with_timeout(1, Duration::from_millis(100));

        sleep(Duration::from_millis(200));
        assert!(node.is_election_timeout());

        node.become_candidate();
        assert_eq!(node.role, RaftRole::Candidate);
    }
#[test]
fn test_tick_promotes_to_candidate_after_timeout() {
    // Create a single-node “cluster”
    let mut nodes = vec![RaftNode::new_with_timeout(1, Duration::from_millis(100))];

    // Wait past the election timeout
    std::thread::sleep(Duration::from_millis(200));

    // Split off the first node and the (empty) rest-of-cluster
    let (first, rest) = nodes.split_at_mut(1);
    // `first` is a slice of length 1; call tick on its element, passing the rest
    first[0].tick(rest);

    // Verify it became a Candidate
    assert_eq!(first[0].role, RaftRole::Candidate);
}

    #[test]
    fn test_candidate_wins_election_with_majority_votes() {
        let mut nodes: Vec<RaftNode> = (1..=5).map(|id| RaftNode::new_with_timeout(id, Duration::from_millis(300))).collect();

        let (self_node, peers) = nodes.split_first_mut().unwrap();

        self_node.start_election(peers);

        assert_eq!(self_node.role, RaftRole::Leader);
    }

    #[test]
fn test_leader_sends_heartbeats_to_followers() {
    let mut nodes: Vec<RaftNode> = (1..=3).map(|id| RaftNode::new_with_timeout(id, Duration::from_millis(300))).collect();

    let (leader, peers) = nodes.split_first_mut().unwrap();
    leader.become_leader();

    // simulate followers last heartbeat in the past
    for peer in peers.iter_mut() {
        peer.last_heartbeat = Instant::now() - Duration::from_millis(500);
    }

    leader.send_heartbeats(peers);

    for peer in peers {
        assert!(!peer.is_election_timeout(), "Peer {} should not timeout", peer.id);
        assert_eq!(peer.role, RaftRole::Follower);
    }
}
#[test]
fn test_candidate_retries_election_on_timeout() {
    let mut nodes: Vec<RaftNode> = (1..=3)
        .map(|id| RaftNode::new_with_timeout(id, Duration::from_millis(200)))
        .collect();

    let (node, peers) = nodes.split_first_mut().unwrap();

    // Make node a candidate but not enough votes
    node.become_candidate();
    node.voted_for = Some(99); // prevent vote for self in retry

    // simulate peers rejecting vote
    for peer in peers.iter_mut() {
        peer.current_term = node.current_term;
        peer.voted_for = Some(999); // already voted for someone else
    }

    // Force retry
    std::thread::sleep(Duration::from_millis(250));
    node.tick(peers);

    assert_eq!(node.role, RaftRole::Leader);
    assert_eq!(node.voted_for, Some(node.id));
    assert!(node.current_term > 1, "Should increase term");
}
#[test]
fn test_handle_vote_request_refuses_second_different_candidate() {
    let mut node = RaftNode::new_with_timeout(1, Duration::from_millis(100));
    // first time: vote for candidate 42
    assert!(node.handle_vote_request(1, 42));
    // second time, same term but different candidate -> reject
    assert!(!node.handle_vote_request(1, 17));
    // your vote_for remains 42
    assert_eq!(node.voted_for, Some(42));
}

#[test]
fn test_handle_vote_request_honours_higher_term() {
    let mut node = RaftNode::new_with_timeout(1, Duration::from_millis(100));
    node.become_candidate();        // term=1
    // now a request for term=2 should demote you
    assert!(node.handle_vote_request(2, 99));
    assert_eq!(node.role, RaftRole::Follower);
    assert_eq!(node.current_term, 2);
    assert_eq!(node.voted_for, Some(99));
}

#[test]
fn test_handle_heartbeat_ignores_older_term() {
    let mut node = RaftNode::new_with_timeout(1, Duration::from_millis(100));
    node.become_candidate();
    let old_term = node.current_term;
    // incoming heartbeat from term 0 — ignore
    node.handle_heartbeat(0, 5);
    assert_eq!(node.role, RaftRole::Candidate);
    assert_eq!(node.current_term, old_term);
}

#[test]
fn test_handle_heartbeat_promotes_to_follower_on_term_advance() {
    let mut node = RaftNode::new_with_timeout(1, Duration::from_millis(100));
    node.become_candidate();
    // heartbeat from new leader in term=2
    node.handle_heartbeat(2, 5);
    assert_eq!(node.role, RaftRole::Follower);
    assert_eq!(node.current_term, 2);
}


}
