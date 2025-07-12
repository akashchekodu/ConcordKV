use kvstore_rs::raft::RaftNode;
use kvstore_rs::raft::command::Command;
use kvstore_rs::storage::KVEngine;
use std::time::Duration;

#[test]
fn test_leader_propose_applies_to_kvengine() {
    // Ensure testdata directory exists
    std::fs::create_dir_all("testdata").unwrap();
    let _ = std::fs::remove_file("testdata/test-wal");
    let _ = std::fs::remove_dir_all("testdata/test-sst");

    let engine = KVEngine::new("testdata/test-wal", "testdata/test-sst");
    let mut node = RaftNode::new_with_timeout(1, std::time::Duration::from_millis(300));
    node.role = kvstore_rs::raft::RaftRole::Leader;

    let cmd = Command::Put {
        key: b"hello".to_vec(),
        value: b"world".to_vec(),
    };

    node.propose(cmd.clone(), &engine);
    assert_eq!(engine.get(b"hello"), Some(b"world".to_vec()));

    let cmd2 = Command::Delete {
        key: b"hello".to_vec(),
    };

    node.propose(cmd2.clone(), &engine);
    assert_eq!(engine.get(b"hello"), None);

    // Clean up test data
    std::fs::remove_file("testdata/test-wal").ok();
    std::fs::remove_dir_all("testdata/test-sst").ok();
}
