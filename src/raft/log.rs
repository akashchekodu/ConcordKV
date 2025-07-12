use crate::raft::command::Command;

/// A log‐entry in Raft: keeps the term plus the actual KV command
#[derive(Clone, Debug)]
pub struct LogEntryWithCommand {
    pub term: u64,
    pub cmd: Command,
}
