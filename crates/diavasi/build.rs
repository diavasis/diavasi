fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Safe here: build scripts are single-threaded and PROTOC is only read by prost-build.
    unsafe {
        std::env::set_var(
            "PROTOC",
            protoc_bin_vendored::protoc_bin_path().expect("protoc"),
        );
    }
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        // Record payloads are `Bytes`, so the server sends the buffered
        // payload without copying it. The bench protocol keeps `Vec<u8>`.
        .bytes([".diavasi.data.v1.Record.payload"])
        .compile_protos(&["proto/bench.proto", "proto/data.proto"], &["proto"])?;
    Ok(())
}
