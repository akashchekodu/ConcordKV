use kvstore_rs::raft::{RaftNode, RaftRole};
use kvstore_rs::raft::command::Command;
use kvstore_rs::storage::KVEngine;
use std::sync::Arc;
use std::time::Duration;

#[test]
fn test_replicated_put_applies_to_all_nodes() {
    // 🏗 Set up 3 Raft nodes with Arc-wrapped KVEngines
    let engines: Vec<Arc<KVEngine>> = (1..=3)
        .map(|i| {
            Arc::new(KVEngine::new(
                format!("testdata/replicated-node{}-wal", i),
                format!("testdata/replicated-node{}-sst", i),
            ))
        })
        .collect();

    let mut nodes: Vec<RaftNode> = (1..=3)
        .map(|id| RaftNode::new_with_timeout(
            id,
            Duration::from_millis(300),
            Arc::clone(&engines[id as usize - 1]),
        ))
        .collect();

    // Elect node 1 as the leader
    nodes[0].role = RaftRole::Leader;

    // 💾 Command to replicate
    let cmd = Command::Put {
        key: b"foo".to_vec(),
        value: b"bar".to_vec(),
    };

    // Leader proposes the command (it internally uses its own engine)
    nodes[0].propose(cmd.clone());

    // Simulate replication: manually apply the same command to followers
    for i in 1..3 {
        nodes[i].apply(cmd.clone());
    }

    // ✅ Check that all nodes have the key/value
    for (i, engine) in engines.iter().enumerate() {
        assert_eq!(
            engine.get(b"foo"),
            Some(b"bar".to_vec()),
            "Node {} should have replicated value",
            i + 1
        );
    }

    // 🧹 Clean up test files
    for i in 1..=3 {
        let _ = std::fs::remove_file(format!("testdata/replicated-node{}-wal", i));
        let _ = std::fs::remove_dir_all(format!("testdata/replicated-node{}-sst", i));
    }
}
