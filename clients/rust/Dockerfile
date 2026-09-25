FROM rust:1.88-bookworm
WORKDIR /src
COPY clients/rust /src/clients/rust
COPY crates/diavasi/proto /src/crates/diavasi/proto
WORKDIR /src/clients/rust
RUN cargo build --release --bin diavasi-consume
COPY clients/docker/wait-ca.sh /wait-ca.sh
RUN chmod +x /wait-ca.sh
CMD ["/bin/sh", "-c", "/wait-ca.sh && /src/clients/rust/target/release/diavasi-consume --addr \"$DIAVASI_DATA_ADDR\" --ca \"$DIAVASI_CA\" --token \"$DIAVASI_API_TOKEN\" --group demo --consumer rust --total 8"]
