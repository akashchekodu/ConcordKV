use std::env;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .file_descriptor_set_path("protos/kvstore_descriptor.bin") // needed for reflection
        .compile(&["protos/kvstore.proto"], &["protos"])?;
    Ok(())
}
