fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    // Vendored protoc is a path this process owns for the build script only.
    unsafe {
        std::env::set_var("PROTOC", protoc);
    }
    tonic_build::configure().build_server(false).compile_protos(
        &["../../crates/diavasi/proto/data.proto"],
        &["../../crates/diavasi/proto"],
    )?;
    Ok(())
}
