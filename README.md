# 🗃️ ConcordKV: Distributed Key-Value Store

**ConcordKV** is a high-performance, distributed key-value store built in Rust, designed for strong consistency, high availability, and efficient storage. It leverages the RAFT consensus algorithm to ensure consistent replication and fault tolerance across nodes.

---

## 🚀 Features

- ✅ Distributed architecture with RAFT-based leader election & replication
- ✅ gRPC API for high-performance key-value operations (`put`, `get`, `delete`)
- ✅ Persistent storage using Write-Ahead Logging (WAL) and SSTables (LSM Tree)
- ✅ Log compaction and storage optimization
- ✅ Real-time leader heartbeats and automatic election handling
- ✅ Modular architecture with `KVEngine`, `RaftNode`, `RaftGrpcServer`

---

## 📦 Architecture Overview

```
Client ──▶ gRPC API ──▶ RaftGrpcServer
                          │
                          ▼
                     [RaftNode]
                          │
       ┌─────────────┬────┴───────┬─────────────┐
       ▼             ▼            ▼             ▼
   Peers          WAL         MemTable        SSTable
```

---

## ⚙️ Components

| Component     | Description |
|---------------|-------------|
| `RaftNode`    | Handles RAFT consensus, elections, replication |
| `KVEngine`    | Manages WAL, SSTables, MemTable |
| `GrpcServer`  | Exposes gRPC API for client interaction |
| `command.rs`  | Command encoding/decoding between log entries and state machine |
| `append_entries.rs` | Implements AppendEntries RPC logic |
| `storage/`    | Durable, log-structured key-value storage backend |

---

## 📡 gRPC API (kvstore.proto)

- `Put`: Insert or update a key
- `Get`: Retrieve a value by key
- `Delete`: Remove a key
- `AppendEntries`: RAFT log replication
- `RequestVote`: RAFT leader election

---

## 🔧 How to Run

### Run 3 Node Cluster

```bash
# Terminal 1
cargo run -- --id 1 --port 50051

# Terminal 2
cargo run -- --id 2 --port 50052

# Terminal 3
cargo run -- --id 3 --port 50053
```

Each instance connects to peers using hardcoded addresses.

---

## 🧪 Tests

```bash
cargo test --all
```

Covers:
- RAFT log replication
- Leader election & timeout handling
- Conflict resolution in AppendEntries
- WAL + SSTable durability tests

---

## 📁 Folder Structure

```
kvstore-rs/
│
├── src/
│   ├── main.rs             # Entrypoint
│   ├── lib.rs              # gRPC server launch
│   ├── raft/               # RAFT logic (node, server, log, commands)
│   └── storage/            # KVEngine, WAL, SSTable, MemTable
│
├── protos/                 # Protobuf definitions
└── tests/                  # Integration tests
```

---

## 🧠 Inspirations

- [Raft Paper](https://raft.github.io/)
- etcd, TiKV, CockroachDB

---

## 🛠️ Future Improvements

- Dynamic membership changes
- Snapshot installation & log truncation
- RocksDB-based backend storage
- Leader re-election under network partitions

---

## 📝 License

MIT License © 2025 ConcordKV Team
