use crate::raft_proto::{Command as ProtoCommand, command::Cmd, Put, Delete};
use prost::Message;

#[derive(Clone, Debug)]
pub enum Command {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

impl Command {
    pub fn to_proto(&self) -> ProtoCommand {
        let cmd = match self {
            Command::Put { key, value } => {
                Cmd::Put(Put {
                    key: key.clone(),
                    value: value.clone(),
                })
            }
            Command::Delete { key } => {
                Cmd::Delete(Delete {
                    key: key.clone(),
                })
            }
        };

        ProtoCommand { cmd: Some(cmd) }
    }

    pub fn from_proto(proto: ProtoCommand) -> Self {
        match proto.cmd.expect("Missing command variant") {
            Cmd::Put(p) => Command::Put { key: p.key, value: p.value },
            Cmd::Delete(d) => Command::Delete { key: d.key },
        }
    }

    pub fn from_proto_bytes(bytes: &[u8]) -> Result<Self, prost::DecodeError> {
        let proto = ProtoCommand::decode(bytes)?;
        Ok(Self::from_proto(proto))
    }

    pub fn encode_to_vec(&self) -> Vec<u8> {
        self.to_proto().encode_to_vec()
    }
}
