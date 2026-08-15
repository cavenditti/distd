fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut config = prost_build::Config::new();
    config.bytes([".distd.ChunkData.data", ".distd.BulkData.data"]);
    config.compile_protos(&["proto/distd.proto"], &["proto"])?;
    Ok(())
}
