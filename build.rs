use std::{env, path::PathBuf};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Cargo gives us OUT_DIR where build artifacts belong
    let out_dir = PathBuf::from(env::var("OUT_DIR")?);

    tonic_build::configure()
        // write the descriptor into OUT_DIR/kvstore_descriptor.bin
        .file_descriptor_set_path(out_dir.join("kvstore_descriptor.bin"))
        .compile(&["protos/kvstore.proto"], &["protos"])?;
    Ok(())
}
