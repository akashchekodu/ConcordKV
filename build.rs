// build.rs at the project root

use std::{env, path::PathBuf};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = PathBuf::from(env::var("OUT_DIR")?);

    // Compile both raft.proto and kvstore.proto in one shot:
    tonic_build::configure()
        .file_descriptor_set_path(out_dir.join("raft_descriptor.bin"))
        .compile(&["protos/raft.proto"], &["protos"])?;

    tonic_build::configure()
        .file_descriptor_set_path(out_dir.join("kvstore_descriptor.bin"))
        .compile(&["protos/kvstore.proto"], &["protos"])?;

    Ok(())
}
