use std::thread;
use std::time::Duration;
use raft::RaftNode;
pub mod raft;         // ✅ Add this at top-level
pub mod storage;

fn main() {
    let mut node = RaftNode::new(1);

    println!("[Startup] Node role: {:?}", node.role);

    // Simulate wait
    thread::sleep(Duration::from_millis(250));

    if node.is_election_timeout() {
        node.become_candidate();
    }

    println!("[After Timeout] Node role: {:?}", node.role);
}
