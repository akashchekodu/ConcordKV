use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::raft::{RaftNode, LogEntryWithCommand};
use crate::raft::command::Command;
use crate::raft::server::RaftGrpcServer;
use crate::raft_proto::{AppendEntriesRequest, LogEntry};
use crate::storage::KVEngine;
use tonic::Request;
use crate::raft_proto::raft_server::Raft;

/// Helper to create a log entry with serialized command
fn make_entry(term: u64, cmd: Command) -> LogEntry {
    LogEntry {
        term,
        command: cmd.encode_to_vec(),
    }
}

#[tokio::test]
async fn test_append_entries_appends_new_entries() {
    let engine = Arc::new(KVEngine::new("test-wal", "test-sst"));
    let node = Arc::new(Mutex::new(RaftNode::new_with_timeout(
        1,
        Duration::from_millis(300),
        Arc::clone(&engine),
    )));

    {
        let mut n = node.lock().unwrap();
        n.log.push(LogEntryWithCommand {
            term: 0,
            cmd: Command::Put { key: b"init".to_vec(), value: b"0".to_vec() },
        });
    }

    let entries = vec![
        make_entry(1, Command::Put { key: b"a".to_vec(), value: b"1".to_vec() }),
        make_entry(1, Command::Put { key: b"b".to_vec(), value: b"2".to_vec() }),
    ];

    let req = Request::new(AppendEntriesRequest {
        term: 1,
        leader_id: 99,
        prev_log_index: 0,
        prev_log_term: 0,
        entries,
        leader_commit: 0,
    });

    let server = RaftGrpcServer::new(Arc::clone(&node), Arc::clone(&engine));
    let resp = server.append_entries(req).await.unwrap().into_inner();

    assert!(resp.success);

    let log = &node.lock().unwrap().log;
    assert_eq!(log.len(), 3);
    assert_eq!(log[1].term, 1);
    assert_eq!(log[2].term, 1);

    let _ = std::fs::remove_file("test-wal");
    let _ = std::fs::remove_dir_all("test-sst");
}

#[tokio::test]
async fn test_append_entries_conflict_replaces_entries() {
    let engine = Arc::new(KVEngine::new("test-wal", "test-sst"));
    let node = Arc::new(Mutex::new(RaftNode::new_with_timeout(
        1,
        Duration::from_millis(300),
        Arc::clone(&engine),
    )));

    {
        let mut n = node.lock().unwrap();
        n.log.push(LogEntryWithCommand {
            term: 1,
            cmd: Command::Put { key: b"x".to_vec(), value: b"old".to_vec() },
        });
        n.log.push(LogEntryWithCommand {
            term: 1,
            cmd: Command::Put { key: b"y".to_vec(), value: b"old".to_vec() },
        });
    }

    let new_entries = vec![
        make_entry(2, Command::Put { key: b"y".to_vec(), value: b"new".to_vec() }),
        make_entry(2, Command::Put { key: b"z".to_vec(), value: b"new".to_vec() }),
    ];

    let req = Request::new(AppendEntriesRequest {
        term: 2,
        leader_id: 99,
        prev_log_index: 1,
        prev_log_term: 1,
        entries: new_entries,
        leader_commit: 0,
    });

    let server = RaftGrpcServer::new(Arc::clone(&node), Arc::clone(&engine));
    let resp = server.append_entries(req).await.unwrap().into_inner();

    assert!(resp.success);

    let n = node.lock().unwrap();
    assert_eq!(n.log.len(), 3);
    assert_eq!(n.log[1].term, 2);
    assert_eq!(n.log[2].term, 2);

    if let Command::Put { key, value } = &n.log[1].cmd {
        assert_eq!(key, b"y");
        assert_eq!(value, b"new");
    } else {
        panic!("Expected Put command at index 1");
    }

    let _ = std::fs::remove_file("test-wal");
    let _ = std::fs::remove_dir_all("test-sst");
}
